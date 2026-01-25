# backend/executor.py
import asyncio
import traceback
import inspect
import logging
import functools
from registry import NODE_CLASS_MAPPINGS

logger = logging.getLogger("BrainFlow.Executor")


# ==========================================
#  0. 环路检测算法 (DFS) - 完整保留
# ==========================================
def validate_graph_acyclic(graph: dict):
    """
    使用深度优先搜索 (DFS) 检测图中是否存在环路。
    如果发现环路，抛出 ValueError。
    """
    visited = set()  # 已经完全检查过的节点 (Black)
    recursion_stack = set()  # 当前递归路径上的节点 (Gray)

    def dfs(node_id):
        visited.add(node_id)
        recursion_stack.add(node_id)

        # 获取依赖节点
        node_data = graph.get(node_id)
        if not node_data:
            return

        inputs = node_data.get("inputs", {})
        for val in inputs.values():
            # 只有 list 格式且长度为2的才是连接引用 [node_id, slot_idx]
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                if dep_id not in graph:
                    continue

                if dep_id in recursion_stack:
                    raise ValueError(f"❌ Cycle Detected: '{node_id}' -> '{dep_id}'")

                if dep_id not in visited:
                    dfs(dep_id)

        recursion_stack.remove(node_id)

    # 遍历图中所有节点
    for node_id in graph:
        if node_id not in visited:
            dfs(node_id)


# ==========================================
# 1. 参数校验与准备 - 完整保留
# ==========================================
def validate_and_prepare_inputs(node_cls, raw_inputs, node_id="Unknown"):
    """
    校验输入参数，补充默认值，并对必填项进行强制检查。
    """
    final_inputs = {}
    # 兼容性处理：有些节点可能没有定义 INPUT_TYPES
    input_defs = node_cls.INPUT_TYPES() if hasattr(node_cls, "INPUT_TYPES") else {"required": {}, "optional": {}}

    # --- 1. 必填项 (Required) ---
    for name, config in input_defs.get("required", {}).items():
        val = raw_inputs.get(name)
        input_type = config[0]
        meta = config[1] if len(config) > 1 and isinstance(config[1], dict) else {}

        # 默认值回填逻辑
        if val is None or (isinstance(val, str) and val == ""):
            if "default" in meta:
                val = meta["default"]
            elif isinstance(input_type, list) and len(input_type) > 0:
                val = input_type[0]

        # 必填校验 (String类型不能为空)
        if (val is None or (isinstance(val, str) and val == "")) and input_type == "STRING":
            raise ValueError(f"Required input '{name}' is missing for Node {node_id}.")

        final_inputs[name] = val

    # --- 2. 选填项 (Optional) ---
    for name, config in input_defs.get("optional", {}).items():
        val = raw_inputs.get(name)
        meta = config[1] if len(config) > 1 and isinstance(config[1], dict) else {}
        if val is None and "default" in meta:
            val = meta["default"]
        final_inputs[name] = val

    # --- 3. 类型自动转换 ---
    for name, val in final_inputs.items():
        if val is not None and isinstance(val, (str, int, float)):
            # 尝试查找类型定义
            def_info = input_defs.get("required", {}).get(name) or input_defs.get("optional", {}).get(name)
            if def_info:
                if def_info[0] == "INT":
                    try:
                        final_inputs[name] = int(val)
                    except:
                        pass
                elif def_info[0] == "FLOAT":
                    try:
                        final_inputs[name] = float(val)
                    except:
                        pass

    return final_inputs


# ==========================================
# 2. 核心执行器 (已修复)
# ==========================================
async def execute_graph(graph: dict, websocket):
    """
    执行图任务，包含环路预检、并行调度和取消处理。
    """
    try:
        # 0. 静态检查
        validate_graph_acyclic(graph)
        await websocket.send_json({"type": "log", "message": "Engine Started..."})

        results = {}
        tasks = {}

        # [修改] 不再启动 monitor_task，彻底根除后端产生的 RAM 废话日志

        # 获取当前事件循环，供后面跨线程回调使用
        loop = asyncio.get_running_loop()

        # 进度汇报辅助函数
        async def progress_callback(node_id, current, total, msg=""):
            # 如果 current 是 -1，保留 -1 (代表不确定进度/无限加载)
            p = -1 if current == -1 else (int((current / total) * 100) if total > 0 else 0)

            try:
                await websocket.send_json({"type": "progress", "taskId": node_id, "progress": p, "message": msg})
            except Exception:
                pass

        # -------------------------------------------------------------------
        # _compute_node 内部执行逻辑
        # -------------------------------------------------------------------
        async def _compute_node(node_id):
            try:
                # 1. 自动汇报开始
                await progress_callback(node_id, 0, 100, "Running...")

                node_data = graph.get(node_id)
                class_name = node_data["type"]

                # A. 解析依赖
                pending_inputs = {}
                final_inputs = {}
                # 遍历配置的 inputs
                for k, v in node_data.get("inputs", {}).items():
                    if isinstance(v, list) and len(v) == 2:
                        pending_inputs[k] = (v[0], v[1])
                    else:
                        final_inputs[k] = v

                # B. 并行等待上游
                dep_ids = list(set([x[0] for x in pending_inputs.values()]))
                if dep_ids:
                    await asyncio.gather(*(schedule_node(dep_id) for dep_id in dep_ids))

                # C. 组装参数
                for arg_name, (dep_id, slot_idx) in pending_inputs.items():
                    src_out = results[dep_id]
                    if isinstance(src_out, tuple) and slot_idx < len(src_out):
                        val = src_out[slot_idx]
                    elif isinstance(src_out, tuple):
                        val = src_out[0]
                    else:
                        val = src_out
                    final_inputs[arg_name] = val

                # D. 实例化与执行
                NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                if not NodeCls:
                    raise ValueError(f"Unknown node type: {class_name}")

                # 参数校验
                func_args = validate_and_prepare_inputs(NodeCls, final_inputs, node_id)

                instance = NodeCls()
                method_name = getattr(NodeCls, "FUNCTION", "execute")
                method = getattr(instance, method_name)

                # [关键功能] 注入线程安全的回调函数
                # 这样你在节点代码里写的 callback(50, 100, "Processing...") 就能传到前端
                if 'callback' in inspect.signature(method).parameters:
                    def thread_safe_cb(c, t, m=""):
                        # 使用 run_coroutine_threadsafe 将异步任务提交回主循环
                        asyncio.run_coroutine_threadsafe(progress_callback(node_id, c, t, m), loop)

                    func_args['callback'] = thread_safe_cb

                # 运行 (支持同步和异步方法)
                if asyncio.iscoroutinefunction(method):
                    output = await method(**func_args)
                else:
                    # 将同步阻塞代码放入线程池运行 (防止卡死 WebSocket 心跳)
                    output = await asyncio.get_running_loop().run_in_executor(None,
                                                                              functools.partial(method, **func_args))

                # 2. 自动汇报结束
                await progress_callback(node_id, 100, 100, "Done")

                results[node_id] = output
                return output

            except Exception as e:
                err_msg = f"Error: {str(e)}"
                logger.error(f"Node {node_id} Failed: {err_msg}")
                traceback.print_exc()

                try:
                    await websocket.send_json({
                        "type": "progress",
                        "taskId": node_id,
                        "progress": 0,
                        "message": err_msg
                    })
                except Exception:
                    pass
                raise e

        # --- 内部调度器 ---
        async def schedule_node(node_id):
            if node_id in results:
                return results[node_id]
            if node_id in tasks:
                return await tasks[node_id]

            task = asyncio.create_task(_compute_node(node_id))
            tasks[node_id] = task
            return await task

        # --- 主流程 ---
        output_nodes = [nid for nid, d in graph.items() if
                        getattr(NODE_CLASS_MAPPINGS.get(d["type"]), "OUTPUT_NODE", False)]

        if not output_nodes and graph:
            output_nodes = [list(graph.keys())[-1]]

        if output_nodes:
            await asyncio.gather(*(schedule_node(nid) for nid in output_nodes))

            # [关键修复] 强制等待 0.2 秒
            # 让前端有时间处理完最后一个节点的 "100%" 进度包，防止 "Done" 信号先于进度包到达
            # 这是解决“任务跑完线还在转”最简单有效的物理手段
            await asyncio.sleep(0.2)

            await websocket.send_json({"type": "done", "message": "Workflow Finished"})

    except asyncio.CancelledError:
        logger.warning("Execution Cancelled by User.")
        try:
            await websocket.send_json({"type": "error", "message": "Execution Cancelled"})
        except Exception:
            pass
    except Exception as e:
        traceback.print_exc()
        try:
            await websocket.send_json({"type": "error", "message": f"Global Error: {str(e)}"})
        except Exception:
            pass
    finally:
        # 清理任务
        for t in tasks.values():
            if not t.done():
                t.cancel()
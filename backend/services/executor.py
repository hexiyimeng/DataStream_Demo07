import asyncio
import traceback
import inspect
import logging
import dask

from dask_manager import bind_layers_to_node
from backend.core.registry import NODE_CLASS_MAPPINGS
# [新增] 引入状态管理器
from backend.core.state_manager import state_manager

logger = logging.getLogger("BrainFlow.Executor")


# =============================================================================
#  0. 环路检测算法 (保持不变)
# =============================================================================
def validate_graph_acyclic(graph: dict):
    visited = set()
    recursion_stack = set()

    def dfs(node_id):
        visited.add(node_id)
        recursion_stack.add(node_id)
        node_data = graph.get(node_id)
        if not node_data: return
        inputs = node_data.get("inputs", {})
        for val in inputs.values():
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                if dep_id not in graph: continue
                if dep_id in recursion_stack:
                    raise ValueError(f"❌ Cycle Detected: '{node_id}' -> '{dep_id}'")
                if dep_id not in visited:
                    dfs(dep_id)
        recursion_stack.remove(node_id)

    for node_id in graph:
        if node_id not in visited:
            dfs(node_id)


# =============================================================================
# 1. 参数校验与准备 (保持不变)
# =============================================================================
def validate_and_prepare_inputs(node_cls, raw_inputs, node_id="Unknown"):
    final_inputs = {}
    if hasattr(node_cls, "INPUT_TYPES"):
        try:
            input_defs = node_cls.INPUT_TYPES()
        except Exception as e:
            logger.error(f"Failed to get INPUT_TYPES for {node_id}: {e}")
            input_defs = {"required": {}, "optional": {}}
    else:
        input_defs = {"required": {}, "optional": {}}

    for name, config in input_defs.get("required", {}).items():
        val = raw_inputs.get(name)
        input_type = config[0]
        meta = config[1] if len(config) > 1 and isinstance(config[1], dict) else {}
        if val is None or (isinstance(val, str) and val == ""):
            if "default" in meta:
                val = meta["default"]
            elif isinstance(input_type, list) and len(input_type) > 0:
                val = input_type[0]
        if (val is None or (isinstance(val, str) and val == "")) and input_type == "STRING":
            raise ValueError(f"Required input '{name}' is missing for Node {node_id}.")
        final_inputs[name] = val

    for name, config in input_defs.get("optional", {}).items():
        val = raw_inputs.get(name)
        meta = config[1] if len(config) > 1 and isinstance(config[1], dict) else {}
        if val is None and "default" in meta: val = meta["default"]
        final_inputs[name] = val

    for name, val in final_inputs.items():
        if val is not None and isinstance(val, (str, int, float)):
            def_info = input_defs.get("required", {}).get(name) or input_defs.get("optional", {}).get(name)
            if def_info:
                target_type = def_info[0]
                try:
                    if target_type == "INT":
                        final_inputs[name] = int(float(val))
                    elif target_type == "FLOAT":
                        final_inputs[name] = float(val)
                    elif target_type == "BOOLEAN":
                        if isinstance(val, str):
                            final_inputs[name] = val.lower() == "true"
                        else:
                            final_inputs[name] = bool(val)
                except:
                    pass
    return final_inputs


# =============================================================================
# 2. 核心执行器 (已修复：删除 websocket 参数)
# =============================================================================
async def execute_graph(graph: dict):  # <--- 注意这里，没有 websocket 参数了
    """
    执行图任务。不再需要传入 websocket 对象，全部通过 state_manager 广播。
    """
    try:
        # [状态管理] 1. 初始化运行状态
        state_manager.is_running = True
        state_manager.clear_state()  # 清除上一轮的进度条

        # [状态管理] 2. 广播开始日志
        validate_graph_acyclic(graph)
        await state_manager.broadcast(state_manager.add_log("🚀 Engine Started...", "info"))

        results = {}
        tasks = {}
        loop = asyncio.get_running_loop()

        # ==========================================
        # [关键修改] 统一回调函数：更新状态 + 广播
        # ==========================================
        async def progress_callback(node_id, current, total, msg=""):
            # 计算百分比
            p = -1 if current == -1 else (int((current / total) * 100) if total > 0 else 0)

            # A. 存入全局内存 (供后续重连使用)
            state_manager.update_progress(node_id, p, msg)

            # B. 实时广播给当前在线的客户端
            await state_manager.broadcast({
                "type": "progress",
                "taskId": node_id,
                "progress": p,
                "message": msg
            })

        # ==========================================
        # 内部执行逻辑 (保持 Dask/Cellpose 逻辑不变)
        # ==========================================
        async def _compute_node(node_id):
            try:
                await progress_callback(node_id, 0, 100, "Building Graph...")

                node_data = graph.get(node_id)
                class_name = node_data["type"]

                pending_inputs = {}
                final_inputs = {}
                for k, v in node_data.get("inputs", {}).items():
                    if isinstance(v, list) and len(v) == 2:
                        pending_inputs[k] = (v[0], v[1])
                    else:
                        final_inputs[k] = v

                dep_ids = list(set([x[0] for x in pending_inputs.values()]))
                if dep_ids:
                    await asyncio.gather(*(schedule_node(dep_id) for dep_id in dep_ids))

                for arg_name, (dep_id, slot_idx) in pending_inputs.items():
                    src_out = results[dep_id]
                    if isinstance(src_out, tuple) and slot_idx < len(src_out):
                        val = src_out[slot_idx]
                    elif isinstance(src_out, tuple):
                        val = src_out[0]
                    else:
                        val = src_out
                    final_inputs[arg_name] = val

                NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                if not NodeCls: raise ValueError(f"Unknown node type: {class_name}")

                func_args = validate_and_prepare_inputs(NodeCls, final_inputs, node_id)
                instance = NodeCls()
                method_name = getattr(NodeCls, "FUNCTION", "execute")
                method = getattr(instance, method_name)

                # 注入 Callbacks (改为调用我们的新 progress_callback)
                if 'callback' in inspect.signature(method).parameters:
                    def thread_safe_cb(c, t, m=""):
                        asyncio.run_coroutine_threadsafe(progress_callback(node_id, c, t, m), loop)

                    func_args['callback'] = thread_safe_cb

                if 'global_progress_callback' in inspect.signature(method).parameters:
                    def thread_safe_global_cb(target_nid, c, t, m=""):
                        asyncio.run_coroutine_threadsafe(progress_callback(target_nid, c, t, m), loop)

                    func_args['global_progress_callback'] = thread_safe_global_cb

                # 执行 (Dask Annotate)
                with dask.annotate(brainflow_node_id=node_id):
                    if asyncio.iscoroutinefunction(method):
                        output = await method(**func_args)
                    else:
                        output = await loop.run_in_executor(None, lambda: method(**func_args))

                # Dask 绑定逻辑 (保持不变)
                layers_to_bind = []
                iterable_output = output if isinstance(output, tuple) else (output,)
                is_lazy = False
                for item in iterable_output:
                    if hasattr(item, 'dask'):
                        is_lazy = True
                        if hasattr(item, 'name'): layers_to_bind.append(item.name)

                if layers_to_bind:
                    bind_layers_to_node(layers_to_bind, node_id)

                if is_lazy:
                    await progress_callback(node_id, 0, 100, "Graph Built (Waiting...)")
                else:
                    await progress_callback(node_id, 100, 100, "Done")

                results[node_id] = output
                return output

            except Exception as e:
                err_msg = f"Error: {str(e)}"
                logger.error(f"Node {node_id} Failed: {err_msg}")
                traceback.print_exc()
                # 记录错误状态
                await progress_callback(node_id, 0, 100, "Error")
                await state_manager.broadcast(state_manager.add_log(f"[{node_id}] {err_msg}", "error"))
                raise e

        async def schedule_node(node_id):
            if node_id in results: return results[node_id]
            if node_id in tasks: return await tasks[node_id]
            task = asyncio.create_task(_compute_node(node_id))
            tasks[node_id] = task
            return await task

        output_nodes = [nid for nid, d in graph.items() if
                        getattr(NODE_CLASS_MAPPINGS.get(d["type"]), "OUTPUT_NODE", False)]
        if not output_nodes and graph: output_nodes = [list(graph.keys())[-1]]

        if output_nodes:
            logger.info(f"Targets identified: {output_nodes}")
            await asyncio.gather(*(schedule_node(nid) for nid in output_nodes))
            await asyncio.sleep(0.2)

            # [状态管理] 任务完成
            await state_manager.broadcast({"type": "done", "message": "Workflow Finished"})
            await state_manager.broadcast(state_manager.add_log("Workflow Finished Successfully", "success"))
        else:
            await state_manager.broadcast(state_manager.add_log("Warning: No output nodes found.", "warning"))

    except asyncio.CancelledError:
        logger.warning("Execution Cancelled.")
        await state_manager.broadcast(state_manager.add_log("Execution Cancelled by User", "warning"))
        await state_manager.broadcast({"type": "error", "message": "Cancelled"})

    except Exception as e:
        traceback.print_exc()
        await state_manager.broadcast(state_manager.add_log(f"Global Error: {str(e)}", "error"))
        await state_manager.broadcast({"type": "error", "message": str(e)})

    finally:
        state_manager.is_running = False
        for t in tasks.values():
            if not t.done(): t.cancel()
import asyncio
import traceback
import inspect
import logging
import time
import dask
import copy
# [关键] 彻底弃用 Pub/Sub，改用稳如泰山的 Queue
from distributed import Queue

from services.dask_service import dask_service
from core.registry import NODE_CLASS_MAPPINGS, ProgressType
from core.state_manager import state_manager

logger = logging.getLogger("BrainFlow.Executor")
logging.getLogger("distributed.core").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)


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
        if node_id not in visited: dfs(node_id)


def validate_and_prepare_inputs(node_cls, raw_inputs, node_id="Unknown"):
    final_inputs = {}
    if hasattr(node_cls, "INPUT_TYPES"):
        try:
            input_defs = node_cls.INPUT_TYPES()
        except Exception as e:
            logger.warning(f"Failed to get INPUT_TYPES from {node_cls}: {e}")
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
                except Exception as e:
                    logger.warning(f"Failed to convert input {name} to boolean: {e}")
    return final_inputs


# =============================================================================
# 2. 后台进度监听器 (混合模式 - Queue + 轮询)
# =============================================================================
async def monitor_dask_progress(node_id, dask_obj, progress_callback_func):
    """
    监听 Dask 延迟计算的进度。

    只接受真实的 chunk 进度消息，不再使用伪进度轮询。

    Args:
        node_id: 节点ID
        dask_obj: Dask 延迟对象
        progress_callback_func: 进度回调函数
    """
    client = dask_service.get_client()
    if not client:
        await progress_callback_func(node_id, -1, 100, "Ready (No Dask client)")
        return

    # 获取总 chunk 数
    try:
        total_chunks = dask_obj.npartitions
    except Exception:
        total_chunks = 1

    if total_chunks <= 0:
        await progress_callback_func(node_id, -1, 100, "Ready (No chunks)")
        return

    # 发送初始状态
    await progress_callback_func(node_id, 0, 100, f"Computing (0/{total_chunks})")

    # 只尝试 Queue 监听（真实 chunk 进度）
    queue_success = await monitor_queue_progress(node_id, total_chunks, progress_callback_func)

    # 如果 Queue 监听失败，不再使用伪进度轮询
    # 改为发送状态型进度，说明任务正在运行但无法提供具体百分比
    if not queue_success:
        logger.warning(f"[QueueMonitor] Queue monitoring for {node_id} failed, switching to state-only mode")
        await progress_callback_func(node_id, -1, 100, "Computing...")
        # 注意：这里不会轮询进度，只是设置状态
        # 实际的完成会通过 compute() 调用的返回来检测


async def monitor_queue_progress(node_id, total_chunks, progress_callback_func):
    """
    使用 Dask Queue 监听进度（快速模式）。

    返回 True 如果成功接收所有进度，False 如果失败
    """
    try:
        q_name = f"queue_{node_id}"
        finished_chunks = 0
        last_percent = 0
        last_message_time = time.monotonic()
        MAX_WAIT_TIME = 1800  # 30 分钟最大等待时间

        queue = Queue(q_name, client=dask_service.get_client())
        loop = asyncio.get_running_loop()

        def blocking_listen():
            nonlocal finished_chunks, last_percent, last_message_time

            while finished_chunks < total_chunks:
                try:
                    msg = queue.get(timeout=2)
                    finished_chunks += 1
                    last_message_time = time.monotonic()

                    percent = int((finished_chunks / total_chunks) * 100)
                    if percent > 100: percent = 100

                    if percent > last_percent or finished_chunks == total_chunks:
                        try:
                            future = asyncio.run_coroutine_threadsafe(
                                progress_callback_func(node_id, percent, 100, f"Running: {finished_chunks}/{total_chunks}"),
                                loop
                            )
                            future.add_done_callback(lambda f: f.exception() and logger.warning(f"Progress callback failed: {f.exception()}"))
                        except Exception as e:
                            logger.warning(f"Failed to schedule progress callback: {e}")
                        last_percent = percent

                except Exception:
                    current_time = time.monotonic()
                    if current_time - last_message_time > MAX_WAIT_TIME:
                        logger.warning(f"[QueueMonitor] Node {node_id} timeout after {MAX_WAIT_TIME}s, {finished_chunks}/{total_chunks} chunks received")
                        return False  # 超时，返回 False
                    continue

            return True  # 成功接收所有 chunks

        result = await loop.run_in_executor(None, blocking_listen)

        # 标记完成
        await progress_callback_func(node_id, 100, 100, "Done")
        return result

    except asyncio.CancelledError:
        logger.info(f"[QueueMonitor] Node {node_id} monitoring cancelled")
        return False
    except Exception as e:
        logger.warning(f"Queue monitor warning for {node_id}: {e}")
        return False


# =============================================================================
# 3. 核心执行器
# =============================================================================
async def execute_graph(graph: dict):
    try:
        state_manager.is_running = True
        state_manager.clear_state()
        validate_graph_acyclic(graph)
        await state_manager.broadcast(state_manager.add_log("🚀 Engine Started...", "info"))

        results = {}
        tasks = {}
        monitor_tasks = []
        loop = asyncio.get_running_loop()

        async def progress_callback(node_id, current, total, msg=""):
            """
            统一进度回调 - 支持两种进度模式：
            1. 状态型进度 (STATE_ONLY): current < 0，只发送状态消息，progress 为 null
            2. 百分比型进度 (CHUNK_COUNT): current >= 0，计算并发送百分比

            自动从节点类获取进度类型，确保消息中包含正确的 progressType。
            """
            # 从 graph 中获取节点类型，再获取进度类型
            node_progress_type = ProgressType.STATE_ONLY  # 默认值
            try:
                node_data = graph.get(node_id)
                if node_data:
                    class_name = node_data.get("type")
                    if class_name:
                        NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                        if NodeCls:
                            node_progress_type = getattr(NodeCls, 'PROGRESS_TYPE', ProgressType.STATE_ONLY)
            except Exception as e:
                logger.warning(f"Failed to get progress type for {node_id}: {e}, using default STATE_ONLY")

            # 发送进度消息，区分两种类型
            if current < 0:
                # 状态型进度：只有消息，没有百分比
                progress_value = None
                logger.info(f"[Progress Callback] Node: {node_id}, Type: {node_progress_type.value}, Message: {msg}")
            else:
                # 百分比型进度：计算并发送百分比
                if total > 0:
                    p = int((current / total) * 100)
                else:
                    p = 0
                p = min(100, max(0, p))  # 确保在 0-100 范围内
                progress_value = p
                logger.info(f"[Progress Callback] Node: {node_id}, Type: {node_progress_type.value}, Progress: {p}%, Message: {msg}")

            # 更新本地状态管理器
            state_manager.update_progress(node_id, progress_value if progress_value is not None else -1, msg)

            # 发送 WebSocket 消息，显式区分进度类型
            await state_manager.broadcast({
                "type": "progress",
                "taskId": node_id,
                "progressType": node_progress_type.value,
                "progress": progress_value,
                "message": msg
            })

        async def _compute_node(node_id):
            NodeCls = None  # 初始化，避免错误处理时未定义
            class_name = None
            func_args = None

            try:
                await progress_callback(node_id, 0, 100, "Initializing...")
                node_data = graph.get(node_id)
                class_name = node_data["type"]

                # 1. 准备输入
                pending_inputs = {}
                final_inputs = {}
                upstream_metadatas = []

                for k, v in node_data.get("inputs", {}).items():
                    if isinstance(v, list) and len(v) == 2:
                        pending_inputs[k] = (v[0], v[1])
                    else:
                        final_inputs[k] = v

                dep_ids = list(set([x[0] for x in pending_inputs.values()]))
                if dep_ids:
                    await asyncio.gather(*(schedule_node(dep_id) for dep_id in dep_ids))

                for arg_name, (dep_id, slot_idx) in pending_inputs.items():
                    src_result = results[dep_id]
                    val = None
                    if isinstance(src_result, tuple):
                        if slot_idx < len(src_result): val = src_result[slot_idx]
                        for item in src_result:
                            if isinstance(item, dict) and ("axes" in item or "source_path" in item):
                                upstream_metadatas.append(item)
                    else:
                        val = src_result
                    final_inputs[arg_name] = val

                # 2. 实例化
                NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                if NodeCls is None: raise ValueError(f"❌ Node '{class_name}' not found!")

                # 获取节点的进度报告类型
                progress_type = getattr(NodeCls, 'PROGRESS_TYPE', ProgressType.STATE_ONLY)

                func_args = validate_and_prepare_inputs(NodeCls, final_inputs, node_id)
                func_args['_node_id'] = node_id

                instance = NodeCls()
                method = getattr(instance, getattr(NodeCls, "FUNCTION", "execute"))

                if 'callback' in inspect.signature(method).parameters:
                    func_args['callback'] = lambda c, t, m="": asyncio.run_coroutine_threadsafe(
                        progress_callback(node_id, c, t, m), loop)
                if 'global_progress_callback' in inspect.signature(method).parameters:
                    func_args['global_progress_callback'] = lambda tid, c, t, m="": asyncio.run_coroutine_threadsafe(
                        progress_callback(tid, c, t, m), loop)

                # 执行
                with dask.annotate(brainflow_node_id=node_id):
                    if asyncio.iscoroutinefunction(method):
                        output = await method(**func_args)
                    else:
                        output = await loop.run_in_executor(None, lambda: method(**func_args))

                # 3. 后处理
                output_list = list(output if isinstance(output, tuple) else (output,))
                is_lazy = False
                dask_obj = None

                for item in output_list:
                    if hasattr(item, 'dask'):
                        is_lazy = True
                        dask_obj = item
                        break

                has_meta = any(isinstance(x, dict) for x in output_list)
                if not has_meta and upstream_metadatas:
                    output_list.append(copy.copy(upstream_metadatas[0]))
                    output = tuple(output_list)

                # [核心逻辑] 进度监控策略 - 基于节点的进度类型
                monitor_task = None
                if is_lazy and dask_obj is not None:
                    if progress_type == ProgressType.CHUNK_COUNT:
                        # 有真实 chunk 数量的节点：启动 chunk 级进度监听
                        total_chunks = dask_obj.npartitions
                        if total_chunks > 0:
                            # 显示"计算中"状态，但不是 "Done"
                            await progress_callback(node_id, 0, 100, "Computing (0/0)")

                            # 启动进度监控任务（异步，不阻塞）
                            monitor_task = asyncio.create_task(
                                monitor_dask_progress(node_id, dask_obj, progress_callback)
                            )
                            monitor_tasks.append(monitor_task)

                            # 注意：不立即标记为 "Done"
                            # monitor_task 会在计算完成时调用 progress_callback(100, 100, "Done")
                            logger.info(f"[{node_id}] Started progress monitoring for {total_chunks} chunks")
                        else:
                            await progress_callback(node_id, 100, 100, "Done")
                    elif progress_type == ProgressType.STATE_ONLY:
                        # STATE_ONLY：只显示状态消息，不显示百分比
                        total_chunks = dask_obj.npartitions
                        status_msg = f"Ready ({total_chunks} chunks)" if total_chunks > 0 else "Ready"
                        await progress_callback(node_id, -1, 100, status_msg)
                    else:  # STAGE_BASED
                        # STAGE_BASED：使用 callback 报告阶段性进度
                        total_chunks = dask_obj.npartitions
                        status_msg = f"Ready ({total_chunks} chunks)" if total_chunks > 0 else "Ready"
                        await progress_callback(node_id, -1, 100, status_msg)
                else:
                    # 瞬时完成节点：直接 Done
                    await progress_callback(node_id, 100, 100, "Done")

                results[node_id] = output
                return output

            except Exception as e:
                # 增强错误上下文
                error_context = {
                    "node_id": node_id,
                    "node_type": class_name,
                    "node_category": getattr(NodeCls, "CATEGORY", "Unknown"),
                    "display_name": getattr(NodeCls, "DISPLAY_NAME", class_name),
                    "error_type": type(e).__name__,
                    "error_message": str(e)[:500],  # 限制长度
                }

                # 记录输入摘要（不包含敏感数据）
                if func_args:
                    inputs_summary = {k: str(v)[:100] if isinstance(v, (str, int, float)) else f"<{type(v).__name__}>"
                                      for k, v in func_args.items() if k != '_node_id'}
                    error_context["inputs"] = inputs_summary

                logger.error(
                    f"Node {node_id} ({error_context['node_type']}) Failed: {error_context['error_type']}: {error_context['error_message']}",
                    extra=error_context
                )
                traceback.print_exc()

                await progress_callback(node_id, 0, 100, f"Error: {error_context['error_type']}")
                raise e

        # 调度
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
            logger.info(f"Targets: {output_nodes}")
            await asyncio.gather(*(schedule_node(nid) for nid in output_nodes))
            await asyncio.sleep(0.5)
            await state_manager.broadcast({"type": "done", "message": "Workflow Finished"})
            await state_manager.broadcast(state_manager.add_log("Workflow Finished Successfully", "success"))
        else:
            await state_manager.broadcast(state_manager.add_log("Warning: No output nodes found.", "warning"))

    except asyncio.CancelledError:
        logger.warning("Execution Cancelled.")
        await state_manager.broadcast({"type": "error", "message": "Cancelled"})
    except Exception as e:
        traceback.print_exc()
        await state_manager.broadcast(state_manager.add_log(f"Global Error: {str(e)}", "error"))
    finally:
        state_manager.is_running = False
        import torch
        if torch.cuda.is_available(): torch.cuda.empty_cache()
        for t in tasks.values():
            if not t.done(): t.cancel()
        for t in monitor_tasks:
            if not t.done(): t.cancel()
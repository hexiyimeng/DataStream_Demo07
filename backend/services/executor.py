import asyncio
import traceback
import inspect
import logging
import time
import uuid
import threading
import dask
import copy
from distributed import Queue, wait as dist_wait

from services.dask_service import dask_service
from core.registry import NODE_CLASS_MAPPINGS, ProgressType
from core.state_manager import state_manager, ExecutionStatus
from utils.progress_helper import get_progress_queue_name, get_stage_progress_queue_name
from utils.memory_monitor import get_memory_monitor

logger = logging.getLogger("BrainFlow.Executor")
logging.getLogger("distributed.core").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)


# =============================================================================
# 1. 拓扑校验
# =============================================================================
def validate_graph_acyclic(graph: dict):
    visited = set()
    recursion_stack = set()

    def dfs(node_id):
        visited.add(node_id)
        recursion_stack.add(node_id)
        node_data = graph.get(node_id)
        if not node_data:
            return
        for val in node_data.get("inputs", {}).values():
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                if dep_id not in graph:
                    continue
                if dep_id in recursion_stack:
                    raise ValueError(f" Cycle Detected: '{node_id}' -> '{dep_id}'")
                if dep_id not in visited:
                    dfs(dep_id)
        recursion_stack.remove(node_id)

    for node_id in graph:
        if node_id not in visited:
            dfs(node_id)


# =============================================================================
# 2. 辅助：查找上游 CHUNK_COUNT 节点（用于 waiting_for 推断）
# =============================================================================
def _find_upstream_chunk_count_nodes(graph: dict, sink_id: str) -> list:
    """
    从指定 sink 节点向后遍历 DAG，收集所有上游 CHUNK_COUNT 节点。

    用于为 sink 节点的 waiting_for 字段提供用户可理解的"我在等谁"信息。

    Returns:
        list of {"node_id": ..., "display_name": ...} for upstream heavy nodes.
        只返回 PROGRESS_TYPE == CHUNK_COUNT 的节点（排除纯 STATE_ONLY 的 Reader/ROI）。
        如果没有上游 CHUNK_COUNT 节点，返回空列表。
    """
    if sink_id not in graph:
        return []

    visited = set()
    result = []

    def dfs(node_id):
        if node_id in visited:
            return
        visited.add(node_id)

        node_data = graph.get(node_id)
        if not node_data:
            return

        class_name = node_data.get("type")
        NodeCls = NODE_CLASS_MAPPINGS.get(class_name) if class_name else None
        if NodeCls:
            pt = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY)
            if pt == ProgressType.CHUNK_COUNT and node_id != sink_id:
                display_name = getattr(NodeCls, "DISPLAY_NAME", class_name or node_id)
                result.append({
                    "node_id": node_id,
                    "display_name": display_name,
                })

        # 继续向后遍历上游
        for val in node_data.get("inputs", {}).values():
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                dfs(dep_id)

    dfs(sink_id)
    return result


# =============================================================================
# 3. 输入准备
# =============================================================================
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
        if val is None and "default" in meta:
            val = meta["default"]
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
                    logger.warning(f"Failed to convert input {name}: {e}")
    return final_inputs


# =============================================================================
# 3. 进度监控
#
# 状态机说明：
#   GraphBuilding: 节点 execute() 执行，构建 lazy graph — 不启动 queue monitor
#   Submitted:      图已提交到 scheduler，等待调度 — 可启动 queue/stage monitor
#   Running:        有 chunk 在执行 — queue monitor 接收进度消息
#   Finished:       所有 sink 计算完成 — monitor 已 drain
#
# 前端文案约束：
#   - GraphBuilding 阶段: "Ready (N chunks)" 或 "Queued" — 不得出现 "Computing"
#   - Submitted/Running 阶段: "Computing (N/M)" — 开始真正 compute 后才显示
# =============================================================================
async def monitor_queue_progress(node_id, total_chunks, progress_callback_func, execution_id=None, stop_event=None):
    """
    使用 Dask Queue 监听进度。
    支持区分 completed/skipped/failed 三种 chunk 类型。

    返回 True 表示成功接收所有进度，False 表示失败。
    """
    try:
        q_name = get_progress_queue_name(node_id, execution_id)

        # 分类计数器
        completed_chunks = 0  # 真实推理完成
        skipped_chunks = 0   # 空块跳过
        failed_chunks = 0    # 异常失败
        processed_chunks = 0 # 总处理数

        last_percent = 0
        last_message_time = time.monotonic()
        MAX_WAIT_TIME = 1800  # 30 分钟最大等待时间

        queue = Queue(q_name, client=dask_service.get_client())
        loop = asyncio.get_running_loop()

        def blocking_listen():
            nonlocal completed_chunks, skipped_chunks, failed_chunks, processed_chunks
            nonlocal last_percent, last_message_time

            while processed_chunks < total_chunks:
                if stop_event and stop_event.is_set():
                    return False
                try:
                    msg = queue.get(timeout=2)
                    last_message_time = time.monotonic()

                    # 解析 chunk 类型（向后兼容：旧消息可能是数字或其他类型）
                    chunk_type = "completed"  # 默认值
                    if isinstance(msg, dict):
                        chunk_type = msg.get("type", "completed")

                    # 分类计数
                    if chunk_type == "completed":
                        completed_chunks += 1
                    elif chunk_type == "skipped":
                        skipped_chunks += 1
                    elif chunk_type == "failed":
                        failed_chunks += 1
                    else:
                        # 未知类型，按 completed 处理
                        completed_chunks += 1

                    processed_chunks = completed_chunks + skipped_chunks + failed_chunks

                    # 计算进度百分比（使用 processed_chunks，避免空块多时感觉"卡死"）
                    percent = int((processed_chunks / total_chunks) * 100)
                    if percent > 100:
                        percent = 100

                    # 构建详细消息
                    msg_text = (
                        f"Segmenting: inference {completed_chunks}, "
                        f"skipped {skipped_chunks}, failed {failed_chunks} / total {total_chunks}"
                    )

                    if percent > last_percent or processed_chunks == total_chunks or processed_chunks == 1:
                        try:
                            future = asyncio.run_coroutine_threadsafe(
                                progress_callback_func(
                                    node_id, percent, 100, msg_text,
                                    run_state="running",
                                    # 额外字段供前端使用
                                    extra={
                                        "totalChunks": total_chunks,
                                        "completedInferenceChunks": completed_chunks,
                                        "skippedChunks": skipped_chunks,
                                        "failedChunks": failed_chunks,
                                        "processedChunks": processed_chunks,
                                    }
                                ),
                                loop
                            )
                            future.add_done_callback(
                                lambda f: f.exception() and logger.warning(
                                    f"Progress callback failed: {f.exception()}"
                                )
                            )
                        except Exception as e:
                            logger.warning(f"Failed to schedule progress callback: {e}")
                        last_percent = percent

                except Exception:
                    if time.monotonic() - last_message_time > MAX_WAIT_TIME:
                        logger.warning(
                            f"[QueueMonitor] Node {node_id} timeout after {MAX_WAIT_TIME}s, "
                            f"processed {processed_chunks}/{total_chunks} chunks "
                            f"(completed={completed_chunks}, skipped={skipped_chunks}, failed={failed_chunks})"
                        )
                        return False
                    continue

            return True

        result = await loop.run_in_executor(None, blocking_listen)

        # 最终 Done 消息（显式传 run_state=done，不再依赖推断）
        final_msg = (
            f"Done: inference {completed_chunks}, "
            f"skipped {skipped_chunks}, failed {failed_chunks} / total {total_chunks}"
        )
        await progress_callback_func(node_id, 100, 100, final_msg, run_state="done")
        return result

    except asyncio.CancelledError:
        logger.info(f"[QueueMonitor] Node {node_id} monitoring cancelled")
        if stop_event:
            stop_event.set()
        return False
    except Exception as e:
        logger.warning(f"Queue monitor warning for {node_id}: {e}")
        return False


async def monitor_dask_progress(node_id, dask_obj, progress_callback_func, execution_id=None, stop_event=None):
    """
    监听 Dask lazy array 的 chunk 级进度。
    """
    client = dask_service.get_client()
    if not client:
        await progress_callback_func(node_id, -1, 100, "Ready (No Dask client)")
        return

    try:
        total_chunks = dask_obj.npartitions
    except Exception:
        total_chunks = 1

    if total_chunks <= 0:
        await progress_callback_func(node_id, -1, 100, "Ready (No chunks)")
        return

    await progress_callback_func(node_id, 0, 100, f"Computing (0/{total_chunks})")

    queue_success = await monitor_queue_progress(node_id, total_chunks, progress_callback_func, execution_id=execution_id, stop_event=stop_event)

    if not queue_success:
        logger.warning(f"[QueueMonitor] Queue monitoring for {node_id} failed, switching to state-only mode")
        await progress_callback_func(node_id, -1, 100, "Computing...")


async def monitor_stage_progress(node_id, progress_callback_func, execution_id=None, done_messages=None, stop_event=None):
    done_messages = set(done_messages or {"Done", "No cells detected"})
    queue_name = get_stage_progress_queue_name(node_id, execution_id)
    if not queue_name:
        return False

    try:
        queue = Queue(queue_name, client=dask_service.get_client())
        loop = asyncio.get_running_loop()
        last_message_time = time.monotonic()
        MAX_WAIT_TIME = 1800

        def blocking_listen():
            nonlocal last_message_time
            while True:
                if stop_event and stop_event.is_set():
                    return False
                try:
                    payload = queue.get(timeout=2)
                    last_message_time = time.monotonic()
                    current = payload.get("current", -1)
                    total = payload.get("total", 100)
                    message = payload.get("message", "")
                    # 显式推断 run_state，不再依赖隐式推断
                    if message in done_messages or (total > 0 and current >= total):
                        run_state = "done"
                    else:
                        run_state = "running"
                    future = asyncio.run_coroutine_threadsafe(
                        progress_callback_func(node_id, current, total, message, run_state=run_state),
                        loop
                    )
                    future.add_done_callback(
                        lambda f: f.exception() and logger.warning(
                            f"Stage progress callback failed: {f.exception()}"
                        )
                    )
                    if message in done_messages or (total > 0 and current >= total):
                        return True
                except Exception:
                    if time.monotonic() - last_message_time > MAX_WAIT_TIME:
                        logger.warning(f"[StageMonitor] Node {node_id} timeout after {MAX_WAIT_TIME}s")
                        return False
                    continue

        return await loop.run_in_executor(None, blocking_listen)
    except asyncio.CancelledError:
        logger.info(f"[StageMonitor] Node {node_id} monitoring cancelled")
        if stop_event:
            stop_event.set()
        return False
    except Exception as e:
        logger.warning(f"Stage monitor warning for {node_id}: {e}")
        return False


# =============================================================================
# 4. 核心执行器
# =============================================================================
async def execute_graph(graph: dict, execution_id: str = None):
    """
    执行 DAG 图。

    Args:
        graph: 节点图定义
        execution_id: 执行 ID（可选，不传则自动生成）

    Returns:
        execution_id: 本次执行的 ID
    """
    tasks = {}
    monitor_tasks = []
    used_progress_queues = set()
    monitor_stop_events = []
    sink_futures = []
    client = None
    should_cancel_dask_objects = False

    # 生成或使用传入的 execution_id
    if not execution_id:
        execution_id = uuid.uuid4().hex

    # === 内存监控初始化 ===
    mem_monitor = get_memory_monitor()
    mem_monitor.snapshots.clear()  # 清除旧快照

    # 创建 execution session
    session = state_manager.create_execution(execution_id)

    try:
        client = dask_service.get_client()
        validate_graph_acyclic(graph)

        # === 内存快照：execution 开始 ===
        mem_monitor.log_snapshot("execution_start", client=client)

        # 广播启动消息（带 execution_id）
        await state_manager.broadcast(execution_id, {
            "type": "log",
            "message": "Engine Started...",
            "executionId": execution_id
        })
        state_manager.add_log("Engine Started...", "info", execution_id=execution_id)

        # --- 找出所有 output 节点 ---
        output_nodes = [
            nid for nid, d in graph.items()
            if getattr(NODE_CLASS_MAPPINGS.get(d["type"]), "OUTPUT_NODE", False)
        ]
        if not output_nodes:
            # 没有 Output/Writer 节点，直接失败
            logger.error("[Executor] No output node found. Please connect a Writer/Output node to execute the workflow.")
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "failed",
                "message": "No output node found. Please connect a Writer/Output node to execute the workflow."
            })
            state_manager.add_log("No output node found. Cannot execute.", "error", execution_id=execution_id)
            return

        # --- 共享状态 ---
        results = {}          # node_id -> output tuple (保持 lazy)
        tasks = {}            # node_id -> asyncio.Task（防止重复调度）
        monitor_tasks = []    # 进度监控任务列表
        sink_progress_meta = {}  # node_id -> sink monitoring metadata
        node_dask_objs = {}   # node_id -> dask array（用于中间节点 queue monitor）
        loop = asyncio.get_running_loop()

        # ==========================================================================
        # 进度回调
        # run_state 语义:
        #   ready      - GraphBuilding 阶段，已构建好 lazy graph，待 submit
        #   submitted  - 已 submit 到 scheduler，等待调度（中间节点首次进入 running 前短暂状态）
        #   running    - 正在执行
        #   done       - 已完成
        #   failed     - 失败
        #   cancelled  - 取消
        # ==========================================================================
        async def progress_callback(node_id, current, total, msg="", extra=None, run_state=None, waiting_for=None):
            node_progress_type = ProgressType.STATE_ONLY
            node_device = None
            node_is_sink = False
            try:
                node_data = graph.get(node_id)
                if node_data:
                    class_name = node_data.get("type")
                    if class_name:
                        NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                        if NodeCls:
                            node_progress_type = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY)
                            node_device = getattr(NodeCls, "DEVICE_HINT", None)
                            node_is_sink = getattr(NodeCls, "OUTPUT_NODE", False)
            except Exception as e:
                logger.warning(f"Failed to get progress type for {node_id}: {e}")

            if current < 0:
                progress_value = None
            else:
                p = int((current / total) * 100) if total > 0 else 0
                p = min(100, max(0, p))
                progress_value = p

            # 推断 run_state（仅当未显式传入时）
            if run_state is None:
                msg_lower = msg.lower()
                if "error" in msg_lower or "fail" in msg_lower:
                    run_state = "failed"
                elif msg in ("Done",) and progress_value == 100:
                    run_state = "done"
                elif progress_value is not None and progress_value >= 0:
                    run_state = "running"
                else:
                    run_state = "ready"

            # 推断 progress_role（节点在 DAG 中的角色）
            if node_progress_type == ProgressType.CHUNK_COUNT:
                progress_role = "chunk_sink" if node_is_sink else "chunk_intermediate"
            elif node_progress_type == ProgressType.STAGE_BASED:
                progress_role = "stage_sink"
            else:
                progress_role = "state_only"

            if progress_value is None:
                logger.info(f"[Progress] {node_id} | progressRole={progress_role} | runState={run_state} | {msg}")
            else:
                logger.info(f"[Progress] {node_id} | progressRole={progress_role} | runState={run_state} | {progress_value}% | {msg}")

            state_manager.update_progress(
                node_id,
                progress_value if progress_value is not None else -1,
                msg,
                execution_id=execution_id,
                progress_type=node_progress_type.value,
                run_state=run_state,
                device=node_device,
                waiting_for=waiting_for,
                progress_role=progress_role,
                extra=extra if extra and isinstance(extra, dict) else None,
            )

            # 构建广播消息
            broadcast_msg = {
                "type": "progress",
                "taskId": node_id,
                "executionId": execution_id,
                "progressType": node_progress_type.value,
                "progress": progress_value,
                "message": msg,
                "runState": run_state,
                "progressRole": progress_role,
            }
            if node_device:
                broadcast_msg["device"] = node_device
            if waiting_for:
                broadcast_msg["waitingFor"] = waiting_for
            # 合并额外字段（如 chunk 分类统计）
            if extra and isinstance(extra, dict):
                broadcast_msg.update(extra)

            await state_manager.broadcast(execution_id, broadcast_msg)

        # ==========================================================================
        # 单节点计算（纯 lazy graph 构建）
        # ==========================================================================
        async def _compute_node(node_id):
            NodeCls = None
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
                    # 从 results 获取上游输出（保持 lazy）
                    src_result = results[dep_id]
                    val = None
                    if isinstance(src_result, tuple):
                        if slot_idx < len(src_result):
                            val = src_result[slot_idx]
                        for item in src_result:
                            if isinstance(item, dict) and ("axes" in item or "source_path" in item):
                                upstream_metadatas.append(item)
                    else:
                        val = src_result
                    final_inputs[arg_name] = val

                # 2. 实例化节点
                NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                if NodeCls is None:
                    raise ValueError(f" Node '{class_name}' not found!")

                progress_type = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY)

                func_args = validate_and_prepare_inputs(NodeCls, final_inputs, node_id)
                func_args["_node_id"] = node_id
                func_args["_execution_id"] = execution_id

                instance = NodeCls()
                method = getattr(instance, getattr(NodeCls, "FUNCTION", "execute"))

                if "callback" in inspect.signature(method).parameters:
                    func_args.pop("callback", None)
                if "global_progress_callback" in inspect.signature(method).parameters:
                    func_args.pop("global_progress_callback", None)

                # 3. 执行（保持 lazy，不提前 materialization）
                with dask.annotate(brainflow_node_id=node_id):
                    if asyncio.iscoroutinefunction(method):
                        output = await method(**func_args)
                    else:
                        output = await loop.run_in_executor(None, lambda: method(**func_args))

                # 4. 后处理
                output_list = list(output if isinstance(output, tuple) else (output,))
                is_lazy = False
                is_dask_array = False
                is_delayed_obj = False
                dask_obj = None

                try:
                    import dask.array as da
                    from dask.delayed import Delayed
                except ImportError:
                    da = None
                    Delayed = None

                for item in output_list:
                    if isinstance(item, dict) and "sink_progress" in item:
                        sink_progress_meta[node_id] = item["sink_progress"]
                    if da is not None and isinstance(item, da.Array):
                        is_lazy = True
                        is_dask_array = True
                        dask_obj = item
                        break
                    if Delayed is not None and isinstance(item, Delayed):
                        is_lazy = True
                        is_delayed_obj = True
                        dask_obj = item
                        break
                    if hasattr(item, "dask"):
                        is_lazy = True
                        dask_obj = item
                        break

                has_meta = any(isinstance(x, dict) for x in output_list)
                if not has_meta and upstream_metadatas:
                    output_list.append(copy.copy(upstream_metadatas[0]))
                    output = tuple(output_list)

                # 保存 dask_obj 引用，供 Phase 2 启动中间节点 queue monitor 使用
                if is_dask_array and dask_obj is not None:
                    node_dask_objs[node_id] = dask_obj

                # =================================================================
                # 5. 进度上报（不触发 eager computation）
                # 注意：此时仍在 Phase 1（GraphBuilding），不要启动 queue/stage monitor！
                # Phase 2（Submit to scheduler）才会真正启动 monitor。
                # 前端文案约束：GraphBuilding 阶段不得出现 "Computing"
                # =================================================================
                if is_lazy and is_dask_array and progress_type == ProgressType.CHUNK_COUNT and dask_obj is not None:
                    total_chunks = dask_obj.npartitions
                    if total_chunks > 0:
                        # 不在这里启动 queue monitor；Phase 2 会在 submit 后启动
                        await progress_callback(node_id, -1, 100, f"Ready ({total_chunks} chunks)")
                    else:
                        await progress_callback(node_id, 100, 100, "Done")
                elif is_lazy:
                    if is_dask_array:
                        total_chunks = getattr(dask_obj, "npartitions", 0)
                        status_msg = f"Ready ({total_chunks} chunks)" if total_chunks > 0 else "Ready"
                        await progress_callback(node_id, -1, 100, status_msg)
                    elif is_delayed_obj:
                        await progress_callback(node_id, -1, 100, "Queued" if progress_type == ProgressType.CHUNK_COUNT else "Ready")
                    else:
                        await progress_callback(node_id, -1, 100, "Ready")
                elif not is_lazy:
                    await progress_callback(node_id, 100, 100, "Done")

                results[node_id] = output if isinstance(output, tuple) else (output,)
                return results[node_id]

            except Exception as e:
                error_context = {
                    "node_id": node_id,
                    "node_type": class_name,
                    "node_category": getattr(NodeCls, "CATEGORY", "Unknown") if NodeCls else "Unknown",
                    "display_name": getattr(NodeCls, "DISPLAY_NAME", class_name) if NodeCls else class_name,
                    "error_type": type(e).__name__,
                    "error_message": str(e)[:500],
                }
                if func_args:
                    inputs_summary = {
                        k: str(v)[:100] if isinstance(v, (str, int, float)) else f"<{type(v).__name__}>"
                        for k, v in func_args.items() if k != "_node_id"
                    }
                    error_context["inputs"] = inputs_summary

                logger.error(
                    f"Node {node_id} ({error_context['node_type']}) Failed: "
                    f"{error_context['error_type']}: {error_context['error_message']}",
                    extra=error_context
                )
                traceback.print_exc()
                await progress_callback(node_id, 0, 100, f"Error: {error_context['error_type']}")
                raise e

        # ==========================================================================
        # 调度器：去重 + asyncio.Task 复用
        # ==========================================================================
        async def schedule_node(node_id):
            if node_id in results:
                return results[node_id]
            if node_id in tasks:
                return await tasks[node_id]
            task = asyncio.create_task(_compute_node(node_id))
            tasks[node_id] = task
            return await task

        # ==========================================================================
        # 辅助：判断是否是 dask.delayed 对象
        # ==========================================================================
        def _is_delayed(obj):
            try:
                from dask.delayed import Delayed
                return isinstance(obj, Delayed)
            except ImportError:
                return hasattr(obj, '__dask_graph__') and not hasattr(obj, 'dask')

        # ==========================================================================
        # 执行 output 节点
        #
        # 统一 lazy graph 构建 + unified sink submission：
        #   Phase 1: 所有节点 execute()，构建 lazy graph
        #   Phase 2: 收集所有 DELAYED 类型的 sink 结果，
        #            启动各自进度监控，client.compute([...]) 统一提交
        # ==========================================================================
        if output_nodes:
            logger.info(f"Targets: {output_nodes}")

            # ── Phase 1: 并发构建所有 output_nodes 的 lazy graph ─────────────────
            msg = "GraphBuilding..."
            await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
            state_manager.add_log(msg, "info", execution_id=execution_id)
            await asyncio.gather(*(schedule_node(nid) for nid in output_nodes))

            # ── Phase 2: 统一提交 DELAYED sink ────────────────────────────────────
            # 收集所有 output 节点返回的 DELAYED 对象
            delayed_sinks = []   # [(node_id, delayed_obj), ...]
            for nid in output_nodes:
                node_result = results.get(nid)
                if node_result is None:
                    continue
                # output tuple 里找 delayed
                items = node_result if isinstance(node_result, tuple) else (node_result,)
                for item in items:
                    if _is_delayed(item):
                        delayed_sinks.append((nid, item))
                        break

            if delayed_sinks:
                client = dask_service.get_client()
                if client:
                    msg = f"Submitted ({len(delayed_sinks)} sink(s)) — Computing..."
                    await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
                    state_manager.add_log(msg, "info", execution_id=execution_id)

                    # 为每个 DELAYED sink 启动进度监控
                    sink_node_ids = [nid for nid, _ in delayed_sinks]
                    sink_delayed_objs = [d for _, d in delayed_sinks]

                    # 1. 先 submit 到 scheduler（确保"Computing"文案在任务已在调度后才出现）
                    futures = await loop.run_in_executor(
                        None, lambda: client.compute(sink_delayed_objs)
                    )
                    if not isinstance(futures, list):
                        futures = [futures]
                    sink_futures.extend(futures)

                    # 2. 启动所有 CHUNK_COUNT 节点的 queue monitor（包括 sink 和中间节点）
                    #    中间节点：发 submitted 广播 → 启动 monitor → monitor 会更新为 running/done
                    #    sink 节点：发 submitted 广播 → 启动 monitor → monitor 会更新为 running/done
                    sink_node_set = {n for n, _ in delayed_sinks}
                    all_monitor_tasks = []

                    for nid, node_data in graph.items():
                        class_name = node_data.get("type")
                        NodeCls = NODE_CLASS_MAPPINGS.get(class_name) if class_name else None
                        if NodeCls is None:
                            continue
                        pt = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY)
                        if pt != ProgressType.CHUNK_COUNT:
                            continue

                        # 获取 total_chunks（sink 从 sink_progress_meta，中间从 node_dask_objs）
                        progress_meta = sink_progress_meta.get(nid)
                        total_chunks = 0
                        if progress_meta and progress_meta.get("kind") == "queue":
                            total_chunks = int(progress_meta.get("total_chunks", 0) or 0)
                        elif nid in node_dask_objs:
                            total_chunks = getattr(node_dask_objs[nid], "npartitions", 0)

                        # waiting_for：对于 sink 节点，显示它在上游等哪个 CHUNK_COUNT 节点
                        waiting_for = None
                        if nid in sink_node_set:
                            upstream_heavy = _find_upstream_chunk_count_nodes(graph, nid)
                            if upstream_heavy:
                                # 格式化：对用户友好 — 显示 display_name 列表
                                waiting_for = [u["display_name"] for u in upstream_heavy]

                        # 向所有 CHUNK_COUNT 节点广播 submitted（中间+sink 统一）
                        dev = getattr(NodeCls, "DEVICE_HINT", None)
                        broadcast_msg = {
                            "type": "progress",
                            "taskId": nid,
                            "executionId": execution_id,
                            "progressType": pt.value,
                            "progress": 0,
                            "message": "Submitted",
                            "runState": "submitted",
                        }
                        if dev:
                            broadcast_msg["device"] = dev
                        if waiting_for:
                            broadcast_msg["waitingFor"] = waiting_for
                        await state_manager.broadcast(execution_id, broadcast_msg)

                        if total_chunks > 0:
                            q_name = get_progress_queue_name(nid, execution_id)
                            used_progress_queues.add(q_name)
                            stop_event = threading.Event()
                            monitor_stop_events.append(stop_event)
                            mt = asyncio.create_task(
                                monitor_queue_progress(nid, total_chunks, progress_callback, execution_id=execution_id, stop_event=stop_event)
                            )
                            all_monitor_tasks.append(mt)
                            monitor_tasks.append(mt)
                        else:
                            # 无 chunk 的 CHUNK_COUNT 节点直接标记 running → done
                            await progress_callback(nid, 0, 100, "Running", run_state="running")

                    # 3. 等待所有 sink 完成（dist_wait 会等待整个 unified graph，含中间节点任务）
                    await loop.run_in_executor(None, lambda: dist_wait(futures))

                    # 收集真实结果，回写到 results，便于后续判定成功/失败
                    sink_runtime_results = {}
                    for (nid, _), future in zip(delayed_sinks, futures):
                        try:
                            sink_value = await loop.run_in_executor(None, future.result)
                            sink_runtime_results[nid] = sink_value
                            original_result = results.get(nid)
                            if isinstance(original_result, tuple):
                                replaced = False
                                new_items = []
                                for item in original_result:
                                    if _is_delayed(item) and not replaced:
                                        new_items.append(sink_value)
                                        replaced = True
                                    else:
                                        new_items.append(item)
                                results[nid] = tuple(new_items)
                            else:
                                results[nid] = sink_value
                        except Exception as e:
                            sink_runtime_results[nid] = f"Error: {e}"
                            raise

                    # 等待所有 queue monitor 任务 drain 完成
                    if all_monitor_tasks:
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(*all_monitor_tasks, return_exceptions=True),
                                timeout=120
                            )
                        except asyncio.TimeoutError:
                            logger.warning("[Phase2] Queue monitor timeout, continuing")
                            for mt in all_monitor_tasks:
                                if not mt.done():
                                    mt.cancel()

                    # 标记所有 sink 为完成（queue monitor 已发 final Done，stage monitor 需要补发）
                    for nid, _ in delayed_sinks:
                        node_data = graph.get(nid)
                        class_name = node_data.get("type") if node_data else None
                        NodeCls = NODE_CLASS_MAPPINGS.get(class_name) if class_name else None
                        progress_type = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY) if NodeCls else ProgressType.STATE_ONLY

                        node_result = results.get(nid)
                        result_msg = "Done"
                        if node_result:
                            for item in (node_result if isinstance(node_result, tuple) else (node_result,)):
                                if isinstance(item, str) and item.startswith("Error:"):
                                    result_msg = item
                                    break
                        if result_msg.startswith("Error:"):
                            await progress_callback(nid, 0, 100, result_msg, run_state="failed")
                        else:
                            # queue monitor final 消息会自动发 Done；stage monitor 需要显式补发
                            if progress_type == ProgressType.STAGE_BASED:
                                await progress_callback(nid, 100, 100, "Done", run_state="done")

                    logger.info("[Phase2] All sinks completed")
                else:
                    logger.warning("[Phase2] No Dask client available, skipping unified submit")

            await asyncio.sleep(0.5)

            # 统一终态消息：成功
            state_manager.set_execution_status(execution_id, ExecutionStatus.SUCCEEDED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",   # <-- 唯一权威终态事件
                "executionId": execution_id,
                "status": "succeeded",
                "message": "Workflow Finished Successfully"
            })
            state_manager.add_log("Workflow Finished Successfully", "success", execution_id=execution_id)
            # LEGACY COMPATIBILITY: done 事件，仅供旧前端使用，不应作为终态收口依据
            await state_manager.broadcast(execution_id, {
                "type": "done",
                "executionId": execution_id,
                "status": "succeeded",
                "message": "Workflow Finished"
            })
        else:
            msg = "Warning: No output nodes found."
            await state_manager.broadcast(execution_id, {"type": "log", "message": msg, "executionId": execution_id})
            state_manager.add_log(msg, "warning", execution_id=execution_id)

    except asyncio.CancelledError:
        should_cancel_dask_objects = True
        logger.warning("Execution Cancelled.")

        # 取消路径：统一终态消息
        state_manager.set_execution_status(execution_id, ExecutionStatus.CANCELLED)
        await state_manager.broadcast(execution_id, {
            "type": "execution_finished",
            "executionId": execution_id,
            "status": "cancelled",
            "message": "Execution Cancelled"
        })
        state_manager.add_log("Execution Cancelled", "warning", execution_id=execution_id)
    except Exception as e:
        should_cancel_dask_objects = True
        traceback.print_exc()

        # 检查当前状态：如果是 CANCELLING，优先收口为 CANCELLED 而非 FAILED
        session = state_manager.get_execution(execution_id)
        if session and session.status == ExecutionStatus.CANCELLING:
            # 取消过程中的异常，收口为 CANCELLED
            logger.warning(f"Exception during CANCELLING, finalizing as CANCELLED: {e}")
            state_manager.set_execution_status(execution_id, ExecutionStatus.CANCELLED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "cancelled",
                "message": f"Execution Cancelled (error during shutdown: {type(e).__name__})"
            })
            state_manager.add_log(f"Cancellation error: {type(e).__name__}", "warning", execution_id=execution_id)
        else:
            # 真正的失败路径
            state_manager.set_execution_status(execution_id, ExecutionStatus.FAILED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "failed",
                "message": str(e)
            })
            state_manager.add_log(f"Global Error: {str(e)}", "error", execution_id=execution_id)
    finally:
        # === 内存快照：execution 结束（清理前） ===
        mem_monitor.log_snapshot("execution_end_before_cleanup", client=client)

        # =========================================================================
        # 清理顺序：1. 停止 monitor → 2. 等待收口 → 3. 释放 Dask 对象 → 4. 其他清理
        # =========================================================================

        # 1. 通知所有 monitor 停止
        for ev in monitor_stop_events:
            try:
                ev.set()
            except Exception:
                pass

        # 2. 等待 monitor 任务完成（带超时，让它们有机会 drain 剩余消息）
        for mt in monitor_tasks:
            if not mt.done():
                try:
                    await asyncio.wait_for(asyncio.shield(mt), timeout=3)
                except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                    mt.cancel()

        # 3. 取消其他内部任务
        for t in tasks.values():
            if not t.done():
                t.cancel()

        # 4. 释放 Dask sink futures（正常/异常路径统一处理）
        if client:
            try:
                if sink_futures:
                    if should_cancel_dask_objects:
                        # 取消/异常路径：强制取消
                        client.cancel(sink_futures, force=True)
                        logger.info(f"[Cleanup] Force cancelled {len(sink_futures)} sink futures (abnormal exit)")
                    else:
                        # 正常完成路径：软释放
                        logger.info(f"[Cleanup] Released {len(sink_futures)} sink futures (normal completion)")
            except Exception as e:
                logger.debug(f"[Cleanup] Failed to release sink futures: {e}")

        # 5. 清理本地引用（帮助 Python GC）
        sink_futures.clear()
        monitor_tasks.clear()
        monitor_stop_events.clear()
        tasks.clear()
        results.clear()
        used_progress_queues.clear()

        # 6. 清理旧的已完成 executions
        state_manager.cleanup_old_executions()

        # 7. 记录清理完成
        logger.info(f"[Cleanup] Execution {execution_id} resources released, should_cancel={should_cancel_dask_objects}")

        # === 内存快照：清理后 ===
        mem_monitor.log_snapshot("execution_end_after_cleanup", client=client)

        # === 输出内存变化摘要 ===
        mem_monitor.log_delta("execution_start", "execution_end_before_cleanup")
        cleanup_result = mem_monitor.log_delta("execution_end_before_cleanup", "execution_end_after_cleanup")

        # 检测 cleanup 是否有效
        start_snapshot = mem_monitor.snapshots.get("execution_start")
        end_before_cleanup = mem_monitor.snapshots.get("execution_end_before_cleanup")
        end_after_cleanup = mem_monitor.snapshots.get("execution_end_after_cleanup")

        if start_snapshot and end_before_cleanup and end_after_cleanup:
            start_mb = start_snapshot.get("process_mb", 0)
            before_mb = end_before_cleanup.get("process_mb", 0)
            after_mb = end_after_cleanup.get("process_mb", 0)

            if start_mb and before_mb and after_mb:
                cleanup_released_mb = before_mb - after_mb
                total_delta_mb = after_mb - start_mb

                if total_delta_mb > 3000:
                    logger.warning(
                        f"[Memory] +{total_delta_mb:.0f}MB retained after execution "
                        f"(cleanup released {cleanup_released_mb:.0f}MB). "
                        f"This is expected for long-running dask workers; "
                        f"GPU VRAM should be freed between executions."
                    )
                else:
                    logger.info(
                        f"[Memory] Execution memory: +{total_delta_mb:.0f}MB total, "
                        f"cleanup released {cleanup_released_mb:.0f}MB. OK."
                    )

    return execution_id

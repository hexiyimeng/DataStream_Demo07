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
# 2. 输入准备
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
# 3. DAG 分析：引用计数 + 共享昂贵节点识别
# =============================================================================
def _compute_refcounts(graph: dict, output_nodes: list) -> dict:
    """
    计算图中每个节点被 output 节点链路引用的次数。
    返回 {node_id: ref_count}
    """
    refcounts = {nid: 0 for nid in graph}

    def _walk(node_id, visited_path: set):
        node_data = graph.get(node_id)
        if not node_data:
            return
        for val in node_data.get("inputs", {}).values():
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                if dep_id in graph:
                    refcounts[dep_id] += 1
                    # 防止在同一条 output 链中重复计数（只沿 DAG 向上，不重复访问）
                    if dep_id not in visited_path:
                        visited_path.add(dep_id)
                        _walk(dep_id, visited_path)

    for out_id in output_nodes:
        refcounts[out_id] += 1
        _walk(out_id, {out_id})

    return refcounts


def _find_shared_expensive_nodes(graph: dict, refcounts: dict) -> set:
    """
    找出 refcount > 1 且真正值得共享物化的节点。
    当前策略：只对 CHUNK_COUNT 且非 OUTPUT_NODE 的节点做共享物化。
    这符合“共享且昂贵的子图只 materialize 一次”的目标，
    避免把所有上游一律提前算完。
    """
    shared = set()
    for node_id, count in refcounts.items():
        if count <= 1:
            continue
        node_data = graph.get(node_id)
        if not node_data:
            continue
        class_name = node_data.get("type")
        NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
        if NodeCls is None:
            continue
        progress_type = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY)
        is_output = getattr(NodeCls, "OUTPUT_NODE", False)
        if progress_type == ProgressType.CHUNK_COUNT and not is_output:
            shared.add(node_id)
            logger.info(f"[DAGAnalysis] Shared expensive node identified: {node_id} ({class_name}), refcount={count}")
    return shared


def _toposort_shared_nodes(shared_nodes: set, graph: dict) -> list:
    """
    返回共享节点的拓扑有序列表（只考虑共享节点之间的相对顺序）。
    非共享依赖由 schedule_node() 递归处理。
    """
    if not shared_nodes:
        return []

    # 收集共享节点之间的依赖关系
    shared_deps = {nid: set() for nid in shared_nodes}
    for nid in shared_nodes:
        node_data = graph.get(nid)
        if not node_data:
            continue
        for val in node_data.get("inputs", {}).values():
            if isinstance(val, list) and len(val) == 2:
                dep_id = val[0]
                if dep_id in shared_nodes:
                    shared_deps[nid].add(dep_id)

    # Kahn 算法拓扑排序
    in_degree = {nid: len(deps) for nid, deps in shared_deps.items()}
    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    result = []

    while queue:
        nid = queue.pop(0)
        result.append(nid)
        for other_nid in shared_nodes:
            if nid in shared_deps.get(other_nid, set()):
                in_degree[other_nid] -= 1
                if in_degree[other_nid] == 0:
                    queue.append(other_nid)

    return result


# =============================================================================
# 4. 进度监控
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

                    if percent > last_percent or processed_chunks == total_chunks:
                        try:
                            future = asyncio.run_coroutine_threadsafe(
                                progress_callback_func(
                                    node_id, percent, 100, msg_text,
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

        # 最终 Done 消息
        final_msg = (
            f"Done: inference {completed_chunks}, "
            f"skipped {skipped_chunks}, failed {failed_chunks} / total {total_chunks}"
        )
        await progress_callback_func(node_id, 100, 100, final_msg)
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
                    future = asyncio.run_coroutine_threadsafe(
                        progress_callback_func(node_id, current, total, message),
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
# 5. 核心执行器
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
    persisted_refs = []
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
            "message": "🚀 Engine Started...",
            "executionId": execution_id
        })
        state_manager.add_log("🚀 Engine Started...", "info", execution_id=execution_id)

        # --- 找出所有 output 节点 ---
        output_nodes = [
            nid for nid, d in graph.items()
            if getattr(NODE_CLASS_MAPPINGS.get(d["type"]), "OUTPUT_NODE", False)
        ]
        if not output_nodes and graph:
            output_nodes = [list(graph.keys())[-1]]

        # --- DAG 分析：识别共享且昂贵的节点 ---
        refcounts = _compute_refcounts(graph, output_nodes)
        shared_expensive_nodes = _find_shared_expensive_nodes(graph, refcounts)

        if shared_expensive_nodes:
            msg = f"🔍 Shared expensive nodes detected (will be persisted): {[graph[n]['type'] for n in shared_expensive_nodes]}"
            await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
            state_manager.add_log(msg, "info", execution_id=execution_id)

        # --- 共享状态 ---
        results = {}          # node_id -> output tuple (可能是 persisted future)
        materialized_results = {}  # node_id -> {slot_idx: persisted_obj} 显式 materialized boundary
        tasks = {}            # node_id -> asyncio.Task（防止重复调度）
        monitor_tasks = []    # 进度监控任务列表
        sink_progress_meta = {}  # node_id -> sink monitoring metadata
        shared_persist_nodes = set()
        loop = asyncio.get_running_loop()

        # ==========================================================================
        # 进度回调
        # ==========================================================================
        async def progress_callback(node_id, current, total, msg="", extra=None):
            node_progress_type = ProgressType.STATE_ONLY
            try:
                node_data = graph.get(node_id)
                if node_data:
                    class_name = node_data.get("type")
                    if class_name:
                        NodeCls = NODE_CLASS_MAPPINGS.get(class_name)
                        if NodeCls:
                            node_progress_type = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY)
            except Exception as e:
                logger.warning(f"Failed to get progress type for {node_id}: {e}")

            if current < 0:
                progress_value = None
                logger.info(f"[Progress] {node_id} | {node_progress_type.value} | {msg}")
            else:
                p = int((current / total) * 100) if total > 0 else 0
                p = min(100, max(0, p))
                progress_value = p
                logger.info(f"[Progress] {node_id} | {node_progress_type.value} | {p}% | {msg}")

            state_manager.update_progress(
                node_id,
                progress_value if progress_value is not None else -1,
                msg,
                execution_id=execution_id,
                progress_type=node_progress_type.value,
            )

            # 构建广播消息
            broadcast_msg = {
                "type": "progress",
                "taskId": node_id,
                "executionId": execution_id,
                "progressType": node_progress_type.value,
                "progress": progress_value,
                "message": msg
            }
            # 合并额外字段（如 chunk 分类统计）
            if extra and isinstance(extra, dict):
                broadcast_msg.update(extra)

            await state_manager.broadcast(execution_id, broadcast_msg)

        # ==========================================================================
        # 单节点计算（含 persist 逻辑）
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
                    # 优先从 materialized_results 获取（共享边界）
                    if dep_id in materialized_results and slot_idx in materialized_results[dep_id]:
                        val = materialized_results[dep_id][slot_idx]
                    else:
                        # 回退到 results
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
                    raise ValueError(f"❌ Node '{class_name}' not found!")

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

                # 3. 执行
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

                # =================================================================
                # 5. 核心：共享昂贵节点 → slot 级 persist + 进度监控
                # =================================================================
                if node_id in shared_expensive_nodes and progress_type == ProgressType.CHUNK_COUNT:
                    client = dask_service.get_client()
                    if client:
                        shared_persist_nodes.add(node_id)
                        materialized_slots = {}  # slot_idx -> persisted_obj
                        new_output_list = []

                        for slot_idx, item in enumerate(output_list):
                            # 只对 dask array 做 persist
                            if da is not None and isinstance(item, da.Array):
                                total_chunks = item.npartitions
                                q_name = get_progress_queue_name(node_id, execution_id)
                                used_progress_queues.add(q_name)

                                # === 内存预估保护 ===
                                # 估算数组大小，超过阈值时警告
                                estimated_bytes = item.nbytes if hasattr(item, 'nbytes') else 0
                                estimated_gb = estimated_bytes / (1024 ** 3)
                                MAX_PERSIST_GB = 32  # 超过此阈值警告

                                logger.info(
                                    f"[Persist] Slot [{slot_idx}] of {node_id} ({class_name}), "
                                    f"{total_chunks} chunks, estimated size: {estimated_gb:.2f} GB"
                                )

                                if estimated_gb > MAX_PERSIST_GB:
                                    logger.warning(
                                        f"[Persist] Large array detected: {estimated_gb:.2f} GB > {MAX_PERSIST_GB} GB threshold. "
                                        f"Persist may cause memory pressure. Node: {node_id}"
                                    )
                                    # 广播警告给前端
                                    await state_manager.broadcast(execution_id, {
                                        "type": "log",
                                        "message": f"⚠️ Large array persist: {estimated_gb:.1f}GB (may cause memory pressure)",
                                        "executionId": execution_id
                                    })

                                await progress_callback(node_id, 0, 100, f"Computing (0/{total_chunks}) [shared slot {slot_idx}]")

                                stop_event = threading.Event()
                                monitor_stop_events.append(stop_event)
                                monitor_task = asyncio.create_task(
                                    monitor_queue_progress(node_id, total_chunks, progress_callback, execution_id=execution_id, stop_event=stop_event)
                                )
                                monitor_tasks.append(monitor_task)

                                # === 内存快照：persist 前 ===
                                mem_monitor.log_snapshot(f"before_persist_{node_id}", client=client, level="debug")

                                # persist 该 slot
                                persisted_obj = client.persist(item)
                                persisted_refs.append(persisted_obj)

                                # 等待 persist 物化完成（带超时保护）
                                PERSIST_TIMEOUT = 3600  # 1小时超时
                                try:
                                    await asyncio.wait_for(
                                        loop.run_in_executor(None, lambda p=persisted_obj: dist_wait(p)),
                                        timeout=PERSIST_TIMEOUT
                                    )
                                except asyncio.TimeoutError:
                                    logger.error(f"[Persist] Timeout after {PERSIST_TIMEOUT}s for node {node_id}")
                                    stop_event.set()
                                    raise TimeoutError(f"Persist timeout for node {node_id} ({estimated_gb:.2f} GB)")

                                # === 内存快照：persist 后 ===
                                mem_monitor.log_snapshot(f"after_persist_{node_id}", client=client, level="debug")
                                mem_monitor.log_delta(f"before_persist_{node_id}", f"after_persist_{node_id}")

                                # 记录到 materialized_slots
                                materialized_slots[slot_idx] = persisted_obj
                                new_output_list.append(persisted_obj)
                            else:
                                # 非 dask array 保持原样
                                new_output_list.append(item)

                        # 存入 materialized_results（显式共享边界）
                        if materialized_slots:
                            materialized_results[node_id] = materialized_slots
                            logger.info(
                                f"[Persist] Node {node_id} materialized slots: "
                                f"{list(materialized_slots.keys())}"
                            )

                        # results 保持原有 tuple 结构
                        output = tuple(new_output_list)
                        results[node_id] = output

                # =================================================================
                # 6. 非共享节点的正常进度监控
                # =================================================================
                elif is_lazy and is_dask_array and progress_type == ProgressType.CHUNK_COUNT and dask_obj is not None and node_id not in shared_persist_nodes:
                    total_chunks = dask_obj.npartitions
                    if total_chunks > 0:
                        q_name = get_progress_queue_name(node_id, execution_id)
                        used_progress_queues.add(q_name)
                        await progress_callback(node_id, 0, 100, f"Computing (0/{total_chunks})")
                        stop_event = threading.Event()
                        monitor_stop_events.append(stop_event)
                        monitor_task = asyncio.create_task(
                            monitor_dask_progress(node_id, dask_obj, progress_callback, execution_id=execution_id, stop_event=stop_event)
                        )
                        monitor_tasks.append(monitor_task)
                        logger.info(f"[{node_id}] Started progress monitoring for {total_chunks} chunks")
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

                results[node_id] = output
                return output

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
        # 分两阶段：
        #   阶段一（构图）：所有节点 execute()，构建 lazy graph。
        #                   共享昂贵节点 persist + 等待物化（保证只算一次）。
        #   阶段二（提交）：收集所有 DELAYED 类型的 sink 结果，
        #                   启动各自进度监控，client.compute([...]) 统一提交，
        #                   scheduler 看到一张大图，并发执行各 sink 的尾部。
        # ==========================================================================
        if output_nodes:
            logger.info(f"Targets: {output_nodes}")

            if shared_expensive_nodes:
                # ── Phase 1a: 预热/物化共享节点 ───────────────────────────────
                ordered_shared = _toposort_shared_nodes(shared_expensive_nodes, graph)
                if ordered_shared:
                    shared_types = [graph[n]['type'] for n in ordered_shared]
                    msg = f"⚙️ Phase 1a: Materializing {len(ordered_shared)} shared node(s): {shared_types}"
                    await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
                    state_manager.add_log(msg, "info", execution_id=execution_id)
                    for nid in ordered_shared:
                        # 调度共享节点，触发 persist + 阻塞等待物化
                        await schedule_node(nid)

                # ── Phase 1b: 并发构建所有 output_nodes ─────────────────────
                msg = f"⚙️ Phase 1b: Building {len(output_nodes)} output(s) concurrently"
                await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
                state_manager.add_log(msg, "info", execution_id=execution_id)
                await asyncio.gather(*(schedule_node(nid) for nid in output_nodes))

            else:
                msg = "⚙️ Phase 1: Building graphs (concurrent)"
                await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
                state_manager.add_log(msg, "info", execution_id=execution_id)
                await asyncio.gather(*(schedule_node(nid) for nid in output_nodes))

            # ── 阶段二：统一提交 DELAYED sink ────────────────────────────────────
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
                    msg = f"⚙️ Phase 2: Submitting {len(delayed_sinks)} sink(s) to scheduler (unified graph)"
                    await state_manager.broadcast(execution_id, {"type": "log", "message": msg})
                    state_manager.add_log(msg, "info", execution_id=execution_id)

                    # 为每个 DELAYED sink 启动进度监控
                    # Writer: CHUNK_COUNT → monitor_queue_progress（chunk 钩子已埋入 lazy graph）
                    # Stats:  STAGE_BASED → callback 直接透传，无需 queue 监控
                    sink_node_ids = [nid for nid, _ in delayed_sinks]
                    sink_delayed_objs = [d for _, d in delayed_sinks]

                    sink_monitor_tasks = []
                    for nid, _ in delayed_sinks:
                        node_data = graph.get(nid)
                        class_name = node_data.get("type") if node_data else None
                        NodeCls = NODE_CLASS_MAPPINGS.get(class_name) if class_name else None
                        progress_type = getattr(NodeCls, "PROGRESS_TYPE", ProgressType.STATE_ONLY) if NodeCls else ProgressType.STATE_ONLY

                        progress_meta = sink_progress_meta.get(nid)
                        if progress_type == ProgressType.CHUNK_COUNT and progress_meta and progress_meta.get("kind") == "queue":
                            total_chunks = int(progress_meta.get("total_chunks", 0) or 0)
                            if total_chunks > 0:
                                q_name = get_progress_queue_name(nid, execution_id)
                                used_progress_queues.add(q_name)
                                await progress_callback(nid, 0, 100, f"Computing (0/{total_chunks})")
                                stop_event = threading.Event()
                                monitor_stop_events.append(stop_event)
                                mt = asyncio.create_task(
                                    monitor_queue_progress(nid, total_chunks, progress_callback, execution_id=execution_id, stop_event=stop_event)
                                )
                                sink_monitor_tasks.append(mt)
                                monitor_tasks.append(mt)
                            else:
                                await progress_callback(nid, -1, 100, "Writing...")
                        elif progress_type == ProgressType.STAGE_BASED and progress_meta and progress_meta.get("kind") == "stage_queue":
                            await progress_callback(nid, -1, 100, "Queued...")
                            q_name = get_stage_progress_queue_name(nid, execution_id)
                            used_progress_queues.add(q_name)
                            stop_event = threading.Event()
                            monitor_stop_events.append(stop_event)
                            mt = asyncio.create_task(
                                monitor_stage_progress(nid, progress_callback, execution_id=execution_id, stop_event=stop_event)
                            )
                            sink_monitor_tasks.append(mt)
                            monitor_tasks.append(mt)
                        else:
                            await progress_callback(nid, -1, 100, "Queued...")

                    # 统一提交：scheduler 拿到一张覆盖所有 sink 的大图
                    futures = client.compute(sink_delayed_objs)
                    if not isinstance(futures, list):
                        futures = [futures]
                    sink_futures.extend(futures)

                    # 等待所有 sink 完成
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

                    # 等待进度监控任务
                    if sink_monitor_tasks:
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(*sink_monitor_tasks, return_exceptions=True),
                                timeout=120
                            )
                        except asyncio.TimeoutError:
                            logger.warning("[Phase2] Sink progress monitor timeout, continuing")
                            for mt in sink_monitor_tasks:
                                if not mt.done():
                                    mt.cancel()

                    # 标记所有 sink 为完成（无论 CHUNK_COUNT 还是 STAGE_BASED）
                    # monitor 可能因超时/取消而没来得及发 Done，这里统一收口
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
                            await progress_callback(nid, 0, 100, result_msg)
                        else:
                            await progress_callback(nid, 100, 100, "Done")

                    logger.info("[Phase2] All sinks completed")
                else:
                    logger.warning("[Phase2] No Dask client available, skipping unified submit")

            await asyncio.sleep(0.5)

            # 统一终态消息：成功
            state_manager.set_execution_status(execution_id, ExecutionStatus.SUCCEEDED)
            await state_manager.broadcast(execution_id, {
                "type": "execution_finished",
                "executionId": execution_id,
                "status": "succeeded",
                "message": "Workflow Finished Successfully"
            })
            state_manager.add_log("Workflow Finished Successfully", "success", execution_id=execution_id)
            # 兼容旧前端：同时发送 done 消息
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
        # 兼容旧前端
        await state_manager.broadcast(execution_id, {
            "type": "error",
            "executionId": execution_id,
            "status": "cancelled",
            "message": "Cancelled"
        })
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
            await state_manager.broadcast(execution_id, {
                "type": "error",
                "executionId": execution_id,
                "status": "cancelled",
                "message": "Cancelled"
            })
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
            # 兼容旧前端
            await state_manager.broadcast(execution_id, {
                "type": "error",
                "executionId": execution_id,
                "status": "failed",
                "message": str(e)
            })
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

        # 4. 释放 Dask 对象（区分正常完成和取消/异常路径）
        if client:
            try:
                if sink_futures:
                    if should_cancel_dask_objects:
                        # 取消/异常路径：强制取消
                        client.cancel(sink_futures, force=True)
                        logger.info(f"[Cleanup] Force cancelled {len(sink_futures)} sink futures (abnormal exit)")
                    else:
                        # 正常完成路径：软释放（让 scheduler 知道我们不再持有引用）
                        # 注意：不使用 force=True，允许结果保留在 scheduler 缓存中供后续查询
                        # 但需要显式告知 scheduler 我们已完成使用
                        logger.info(f"[Cleanup] Released {len(sink_futures)} sink futures (normal completion)")
            except Exception as e:
                logger.debug(f"[Cleanup] Failed to release sink futures: {e}")

            try:
                if persisted_refs:
                    if should_cancel_dask_objects:
                        # 取消/异常路径：强制取消 persisted 对象
                        client.cancel(persisted_refs, force=True)
                        logger.info(f"[Cleanup] Force cancelled {len(persisted_refs)} persisted objects (abnormal exit)")
                    else:
                        # 正常完成路径：persisted 对象已被下游消费，可以释放引用
                        # 但不 force cancel，因为结果可能已被 writer 使用
                        logger.info(f"[Cleanup] Released {len(persisted_refs)} persisted refs (normal completion)")
            except Exception as e:
                logger.debug(f"[Cleanup] Failed to release persisted refs: {e}")

        # 5. 清理本地引用（帮助 Python GC）
        # 这些是局部变量，函数返回后自动回收，但显式清理可以更快释放内存
        persisted_refs.clear()
        sink_futures.clear()
        monitor_tasks.clear()
        monitor_stop_events.clear()
        tasks.clear()
        results.clear()
        materialized_results.clear()
        used_progress_queues.clear()

        # 6. 不再主动 close distributed.Queue
        # Queue 由 scheduler 管理，execution 结束后自动失活
        # 主动 close 可能导致 worker put 时异常

        # 7. 清理旧的已完成 executions
        state_manager.cleanup_old_executions()

        # 8. 记录清理完成
        logger.info(f"[Cleanup] Execution {execution_id} resources released, should_cancel={should_cancel_dask_objects}")

        # === 内存快照：清理后 ===
        mem_monitor.log_snapshot("execution_end_after_cleanup", client=client)

        # === 输出内存变化摘要 ===
        mem_monitor.log_delta("execution_start", "execution_end_before_cleanup")
        mem_monitor.log_delta("execution_end_before_cleanup", "execution_end_after_cleanup")

        # 检测潜在的内存泄漏
        start_snapshot = mem_monitor.snapshots.get("execution_start")
        end_snapshot = mem_monitor.snapshots.get("execution_end_after_cleanup")
        if start_snapshot and end_snapshot:
            if start_snapshot.get("process_mb") and end_snapshot.get("process_mb"):
                delta_mb = end_snapshot["process_mb"] - start_snapshot["process_mb"]
                # 如果 execution 结束后内存增长超过 500MB，警告可能的泄漏
                if delta_mb > 500:
                    logger.warning(
                        f"[Memory] Potential memory leak detected: "
                        f"+{delta_mb:.0f}MB retained after execution {execution_id}"
                    )
                else:
                    logger.info(
                        f"[Memory] Memory change summary: {mem_monitor.log_summary()}"
                    )

    return execution_id

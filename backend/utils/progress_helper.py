"""
Unified progress reporting utility for BrainFlow nodes.

This module provides a centralized way to report progress from Dask workers
to the main execution engine using distributed.Queue.
"""

import logging
from typing import Optional

# Import logger
from core.logger import logger

try:
    from distributed import Queue
    HAS_DISTRIBUTED = True
except ImportError:
    HAS_DISTRIBUTED = False
    Queue = None
    logger.warning("distributed library not available, progress reporting disabled")


logger = logging.getLogger("BrainFlow.ProgressHelper")


# =============================================================================
# 统一 Queue 获取函数
# =============================================================================
def get_progress_queue(queue_name: str, client=None) -> Optional['Queue']:
    """
    统一的 Queue 获取函数。

    优先级：传入 client > dask_service client > 当前 client

    Args:
        queue_name: Queue 名称
        client: 可选的 Dask client

    Returns:
        Queue 对象或 None（失败时静默返回 None）
    """
    if not HAS_DISTRIBUTED or not queue_name:
        return None

    try:
        if client:
            return Queue(queue_name, client=client)

        # 尝试从 dask_service 获取
        try:
            from services.dask_service import dask_service
            ds_client = dask_service.get_client()
            if ds_client:
                return Queue(queue_name, client=ds_client)
        except Exception:
            pass

        # 最后尝试默认方式（使用当前 client context）
        return Queue(queue_name)
    except Exception as e:
        logger.debug(f"[ProgressHelper] Failed to get queue {queue_name}: {e}")
        return None


def get_progress_queue_name(node_id: Optional[str], execution_id: Optional[str] = None) -> Optional[str]:
    """生成进度队列名称（严格绑定 execution_id 避免串用）"""
    if not node_id:
        return None
    if execution_id:
        return f"queue_{execution_id}_{node_id}"
    return f"queue_{node_id}"


def get_stage_progress_queue_name(node_id: Optional[str], execution_id: Optional[str] = None) -> Optional[str]:
    """生成阶段进度队列名称（严格绑定 execution_id 避免串用）"""
    if not node_id:
        return None
    if execution_id:
        return f"stage_queue_{execution_id}_{node_id}"
    return f"stage_queue_{node_id}"


# =============================================================================
# 进度报告函数
# =============================================================================
def report_stage_progress(node_id: Optional[str], current: int, total: int, msg: str = "",
                         execution_id: Optional[str] = None, client=None) -> bool:
    """报告阶段进度（STAGE_BASED 类型节点使用）"""
    queue_name = get_stage_progress_queue_name(node_id, execution_id)
    if not queue_name:
        return False

    q = get_progress_queue(queue_name, client=client)
    if not q:
        return False

    payload = {"current": current, "total": total, "message": msg}
    try:
        q.put(payload)
        return True
    except Exception as e:
        logger.debug(f"[ProgressHelper] Stage progress report failed for node {node_id}: {e}")
        return False


def report_progress(node_id: Optional[str], execution_id: Optional[str] = None,
                   chunk_type: str = "completed", client=None) -> bool:
    """
    报告 chunk 级进度（CHUNK_COUNT 类型节点使用）。

    Args:
        node_id: 节点 ID
        execution_id: 执行 ID（用于队列隔离）
        chunk_type: chunk 类型：
            - "completed": 真实推理完成
            - "skipped": 空块跳过
            - "failed": 异常失败
        client: 可选的 Dask client

    Returns:
        bool: 是否成功报告
    """
    queue_name = get_progress_queue_name(node_id, execution_id)
    if not queue_name:
        return False

    q = get_progress_queue(queue_name, client=client)
    if not q:
        return False

    try:
        # 发送结构化消息
        q.put({"type": chunk_type})
        return True
    except Exception as e:
        logger.debug(f"[ProgressHelper] Progress report failed for node {node_id}: {e}")
        return False


def report_progress_batch(node_id: Optional[str], count: int = 1,
                         execution_id: Optional[str] = None, client=None) -> bool:
    """
    批量报告进度（一次报告多个 chunk）。
    """
    queue_name = get_progress_queue_name(node_id, execution_id)
    if not queue_name or count <= 0:
        return False

    q = get_progress_queue(queue_name, client=client)
    if not q:
        return False

    try:
        for _ in range(count):
            q.put({"type": "completed"})
        return True
    except Exception as e:
        logger.debug(f"[ProgressHelper] Batch progress report failed for node {node_id}: {e}")
        return False


def create_progress_queue(node_id: str, execution_id: Optional[str] = None, client=None) -> Optional['Queue']:
    """
    创建进度队列（executor 端使用）。

    Args:
        node_id: 节点 ID
        execution_id: 执行 ID
        client: 可选的 Dask client

    Returns:
        Queue 对象或 None
    """
    queue_name = get_progress_queue_name(node_id, execution_id)
    return get_progress_queue(queue_name, client=client)


def create_progress_callback(node_id: str, execution_id: Optional[str] = None):
    """
    创建进度报告回调函数。

    WARNING:
    - 此函数返回闭包，不要传入 Dask graph
    - 在 Dask worker 中使用模块级 report_progress 函数
    """
    def _callback(chunk):
        report_progress(node_id, execution_id=execution_id)
        return chunk
    return _callback


# =============================================================================
# 遗留兼容
# =============================================================================
def report_progress_legacy(node_id: Optional[str]) -> bool:
    """遗留的 Pub 方式进度报告（已废弃）"""
    if not node_id:
        return False

    try:
        try:
            from distributed import Pub
        except ImportError:
            from dask.distributed import Pub

        pub = Pub(f"progress_{node_id}")
        pub.put("1")
        return True
    except Exception as e:
        logger.debug(f"[ProgressHelper] Legacy progress report failed for node {node_id}: {e}")
        return False
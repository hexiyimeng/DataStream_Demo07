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
    logger.warning("distributed library not available, progress reporting disabled")


logger = logging.getLogger("BrainFlow.ProgressHelper")


def report_progress(node_id: Optional[str], client=None) -> bool:
    """
    Report progress for a specific node using distributed Queue.

    This is the unified progress reporting function that should be used
    by all nodes instead of inline progress reporting logic.

    Args:
        node_id: The node identifier for which to report progress
        client: Optional Dask client for Queue creation

    Returns:
        bool: True if progress was reported successfully, False otherwise
    """
    if not node_id:
        return False

    if not HAS_DISTRIBUTED:
        return False

    try:
        # Pass client if provided for proper Queue creation in workers
        if client:
            q = Queue(f"queue_{node_id}", client=client)
        else:
            # Use get_client() from the current context if available
            try:
                from services.dask_service import dask_service
                fallback_client = dask_service.get_client()
                if fallback_client:
                    q = Queue(f"queue_{node_id}", client=fallback_client)
                else:
                    q = Queue(f"queue_{node_id}")
            except Exception:
                q = Queue(f"queue_{node_id}")
        q.put(1)  # Send completion signal
        return True
    except Exception as e:
        logger.debug(f"Progress report failed for node {node_id}: {e}")
        return False


def report_progress_batch(node_id: Optional[str], count: int = 1) -> bool:
    """
    Report batch progress for multiple completed chunks.

    Args:
        node_id: The node identifier
        count: Number of items completed

    Returns:
        bool: True if progress was reported successfully
    """
    if not node_id or count <= 0:
        return False

    if not HAS_DISTRIBUTED:
        return False

    try:
        q = Queue(f"queue_{node_id}")
        for _ in range(count):
            q.put(1)
        return True
    except Exception as e:
        logger.debug(f"Batch progress report failed for node {node_id}: {e}")
        return False


def create_progress_queue(node_id: str) -> Optional['Queue']:
    """
    Create a progress queue for a specific node.

    Args:
        node_id: The node identifier

    Returns:
        Queue object or None if creation failed
    """
    if not HAS_DISTRIBUTED:
        return None

    try:
        return Queue(f"queue_{node_id}")
    except Exception as e:
        logger.debug(f"Failed to create progress queue for node {node_id}: {e}")
        return None


def create_progress_callback(node_id: str):
    """
    Create a progress reporting callback function.

    Returns a callback that can be used in map_blocks/map_overlap.

    Args:
        node_id: The node identifier

    Returns:
        Callback function
    """
    def _callback(chunk):
        report_progress(node_id)
        return chunk
    return _callback


# Legacy compatibility function for old Pub-based progress reporting
def report_progress_legacy(node_id: Optional[str]) -> bool:
    """
    Legacy progress reporting using Pub (deprecated).

    This function is provided for backward compatibility and should not
    be used in new code. Use report_progress() instead.

    Args:
        node_id: The node identifier

    Returns:
        bool: True if progress was reported successfully
    """
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
        logger.debug(f"Legacy progress report failed for node {node_id}: {e}")
        return False
from __future__ import annotations

import gc
import logging


logger = logging.getLogger("BrainFlow.Resources")


def collect_python_and_cuda() -> None:
    """Low-frequency cleanup used only during dispose/eviction/shutdown."""

    gc.collect()
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception as exc:
        logger.debug("CUDA cleanup skipped: %s", exc)

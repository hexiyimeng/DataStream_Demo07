from __future__ import annotations

import logging

import numpy as np

from core.model_ref import ModelRef
from core.model_registry import list_models
from core.registry import ProgressType, register_node
from core.resources.worker_resource_manager import (
    clear_idle_worker_resources,
    force_clear_worker_resources,
)
from nodes.base import SegmentationBlockMapNode


logger = logging.getLogger("BrainFlow.Cellpose")
logging.getLogger("cellpose.models").setLevel(logging.WARNING)


def _coerce_model_ref(model, model_type: str | None) -> ModelRef:
    if isinstance(model, ModelRef):
        return model
    if isinstance(model, dict):
        return ModelRef.from_dict(model)
    return ModelRef(provider="cellpose", name=model_type or "cpsam", options={})


def clear_cellpose_model_cache() -> bool:
    """Compatibility wrapper for old cleanup imports."""

    result = clear_idle_worker_resources()
    return not result.get("errors")


def force_clear_cellpose_model_cache() -> int:
    """Compatibility wrapper for old cleanup imports."""

    result = force_clear_worker_resources()
    return int(result.get("disposed", 0) or 0)


def cellpose_block(
    block,
    model=None,
    model_type="cpsam",
    diameter=0.0,
    flow_threshold=0.4,
    cellprob_threshold=0.0,
    gpu_batch_size=8,
    ctx=None,
):
    if ctx is None:
        raise RuntimeError("Cellpose block requires a BlockContext.")

    model_ref = _coerce_model_ref(model, model_type)
    cellpose_model = ctx.resources.resolve_model(model_ref)

    masks = None
    flows = None
    styles = None
    try:
        eval_kwargs: dict = {
            "batch_size": gpu_batch_size,
            "progress": None,
            "bsize": 256,
            "tile_overlap": 0.1,
            "resample": False,
        }

        if diameter and diameter > 0:
            eval_kwargs["diameter"] = diameter

        # Preserve existing behavior: any 3D-or-higher block is treated as a
        # 3D spatial block. Future C/T-aware segmentation should split those
        # axes explicitly before this node.
        if block.ndim >= 3:
            eval_kwargs["do_3D"] = True
            eval_kwargs["z_axis"] = 0
        else:
            eval_kwargs["do_3D"] = False
            eval_kwargs["flow_threshold"] = flow_threshold
            eval_kwargs["cellprob_threshold"] = cellprob_threshold

        masks, flows, styles = cellpose_model.eval(block, **eval_kwargs)
        return masks.astype(np.uint16, copy=False)
    finally:
        if masks is not None:
            del masks
        if flows is not None:
            del flows
        if styles is not None:
            del styles


@register_node("DaskCellpose")
class DaskCellpose(SegmentationBlockMapNode):
    CATEGORY = "BrainFlow/Segmentation"
    DISPLAY_NAME = "Cellpose Segmentation"
    DESCRIPTION = (
        "Runs Cellpose block-wise. Models are represented by lightweight "
        "ModelRef inputs and loaded lazily inside Dask workers."
    )
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    OUTPUT_DTYPE = np.uint16

    @classmethod
    def INPUT_TYPES(cls):
        models = list_models("cellpose") or ["cpsam"]
        default_model = "cpsam" if "cpsam" in models else models[0]
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "diameter": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 500.0}),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0}),
                "gpu_batch_size": ("INT", {"default": 8, "min": 1, "max": 64}),
            },
            "optional": {
                "model": ("MODEL[cellpose]",),
                "model_type": (models, {"default": default_model, "deprecated": True}),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint16]",)
    RETURN_NAMES = ("mask_dask",)
    FUNCTION = "execute"
    PROCESS_BLOCK = cellpose_block

    @classmethod
    def REQUIRED_MODEL_REFS(cls, inputs):
        if inputs.get("model") is not None:
            return []
        return [ModelRef(provider="cellpose", name=inputs.get("model_type") or "cpsam", options={})]

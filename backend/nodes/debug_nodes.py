import numpy as np

from core.registry import ProgressType, register_node
from nodes.base import BaseBlockMapNode


def _chunk_id_from_location(location) -> int:
    if location is None:
        return 0
    try:
        return sum((int(v) + 1) * (10 ** i) for i, v in enumerate(reversed(location)))
    except Exception:
        return 0


def chunk_marker_block(
    block: np.ndarray,
    mode: str,
    marker_strength: int,
    border_width: int,
    ctx,
) -> np.ndarray:
    shape = block.shape
    out = np.zeros(shape, dtype=np.uint16)
    if out.size == 0:
        return out

    strength = int(np.clip(marker_strength, 1, 65535))
    border_w = max(1, int(border_width))
    ndim = out.ndim

    if mode == "checkerboard":
        cell_size = max(4, min(border_w, 8))
        coords = np.indices(shape, sparse=False)
        pattern = np.zeros(shape, dtype=np.int16)
        for axis in range(ndim):
            pattern += (coords[axis] // cell_size).astype(np.int16)
        out[pattern % 2 == 0] = strength

    elif mode == "border_frame":
        out[...] = strength
        inner = []
        for size in shape:
            margin = min(border_w, max(size // 4, 1))
            inner.append(slice(margin, max(margin, size - margin)))
        out[tuple(inner)] = 0

    elif mode == "chunk_index":
        base_val = (_chunk_id_from_location(ctx.block_location) * 1000) % 65535
        marker_slices = tuple(slice(0, min(size, border_w * 2)) for size in shape)
        out[marker_slices] = base_val
        diag_len = min(shape)
        for i in range(min(diag_len, border_w * 4)):
            out[(i,) * ndim] = min(base_val + 2000, 65535)

    elif mode == "stripe_h":
        axis = max(ndim - 2, 0)
        stripe_h = max(4, border_w)
        coord = np.arange(shape[axis])
        mask_1d = (coord // stripe_h) % 2 == 0
        view_shape = [1] * ndim
        view_shape[axis] = shape[axis]
        out[np.broadcast_to(mask_1d.reshape(view_shape), shape)] = strength

    elif mode == "stripe_v":
        axis = ndim - 1
        stripe_w = max(4, border_w)
        coord = np.arange(shape[axis])
        mask_1d = (coord // stripe_w) % 2 == 0
        view_shape = [1] * ndim
        view_shape[axis] = shape[axis]
        out[np.broadcast_to(mask_1d.reshape(view_shape), shape)] = strength

    elif mode == "gradient":
        coords = np.indices(shape, sparse=False)
        denom = sum(max(size - 1, 1) for size in shape)
        gradient = np.zeros(shape, dtype=np.float32)
        for axis in range(ndim):
            gradient += coords[axis] / denom
        out[...] = np.clip(gradient * strength, 0, 65535).astype(np.uint16)

    else:
        out[...] = strength

    return out


@register_node("DaskChunkMarker")
class DaskChunkMarker(BaseBlockMapNode):
    CATEGORY = "BrainFlow/Debug"
    DISPLAY_NAME = "Chunk Marker (Debug)"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "mode": ([
                    "checkerboard",
                    "border_frame",
                    "chunk_index",
                    "stripe_h",
                    "stripe_v",
                    "gradient",
                ], {
                    "default": "checkerboard",
                }),
                "marker_strength": ("INT", {
                    "default": 40000,
                    "min": 1,
                    "max": 65535,
                }),
                "border_width": ("INT", {
                    "default": 8,
                    "min": 1,
                    "max": 64,
                }),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[uint16]",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "execute"

    PROCESS_BLOCK = chunk_marker_block

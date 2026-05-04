import numpy as np

from core.registry import register_node
from nodes.base import BaseBlockMapNode


def scale_shift_block(block: np.ndarray, scale: float, bias: float) -> np.ndarray:
    return (block.astype(np.float32, copy=False) * scale + bias).astype(np.float32, copy=False)


@register_node("DaskScaleShift")
class DaskScaleShift(BaseBlockMapNode):
    CATEGORY = "BrainFlow/Debug"
    DISPLAY_NAME = "Scale & Shift"


    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "scale": ("FLOAT", {
                    "default": 1.0,
                    "min": -10.0,
                    "max": 10.0,
                    "step": 0.1,
                }),
                "bias": ("FLOAT", {
                    "default": 0.0,
                    "min": -10000.0,
                    "max": 10000.0,
                    "step": 1.0,
                }),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[float32]",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "execute"

    PROCESS_BLOCK = scale_shift_block

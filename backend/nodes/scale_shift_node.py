import logging
import numpy as np

from core.registry import register_node
from nodes.base import BaseBlockMapNode

logger = logging.getLogger("BrainFlow.ScaleShift")


@register_node("DaskScaleShift")
class DaskScaleShift(BaseBlockMapNode):
    """
    简单的 BlockMap 测试节点。

    逐 block 操作：
        output = input * scale + bias

    用途：
    - 测试 BaseBlockMapNode 执行路径
    - 测试 map_blocks 包装
    - 测试参数传递
    - 测试输出 dtype 处理
    - 可选：测试 block_info 驱动的可见 chunk 标记
    """

    CATEGORY = "BrainFlow/Debug"
    DISPLAY_NAME = "Scale & Shift (BlockMap Test)"

    # 安全默认值：不跳过全零块，不吞异常
    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = False
    FAILURE_POLICY = "raise"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "scale": ("FLOAT", {
                    "default": 1.0,
                    "min": -10.0,
                    "max": 10.0,
                    "step": 0.1,
                    "tooltip": "Multiply each block by this factor"
                }),
                "bias": ("FLOAT", {
                    "default": 0.0,
                    "min": -10000.0,
                    "max": 10000.0,
                    "step": 1.0,
                    "tooltip": "Add this bias to each block"
                }),
                "output_dtype": (["same_as_input", "float32", "uint16", "uint8"], {
                    "default": "same_as_input"
                }),
            },
            "optional": {
                "mark_by_chunk": ("BOOLEAN", {
                    "default": False,
                    "tooltip": "For debugging only: add visible offset by chunk index"
                }),
                "mark_step": ("FLOAT", {
                    "default": 10.0,
                    "min": 0.0,
                    "max": 1000.0,
                    "step": 1.0,
                    "tooltip": "Chunk marker step when mark_by_chunk=True"
                }),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "execute"

    def infer_output_dtype(self, input_dtype, params):
        mode = params.get("output_dtype", "same_as_input")
        if mode == "float32":
            return np.float32
        if mode == "uint16":
            return np.uint16
        if mode == "uint8":
            return np.uint8
        return np.dtype(input_dtype)

    def process_block(self, block, block_info, params, runtime):
        scale = float(params.get("scale", 1.0))
        bias = float(params.get("bias", 0.0))
        mark_by_chunk = bool(params.get("mark_by_chunk", False))
        mark_step = float(params.get("mark_step", 10.0))

        out_dtype = self.infer_output_dtype(block.dtype, params)

        # 先转 float32 做运算，最后再转目标 dtype
        out = block.astype(np.float32, copy=False) * scale + bias

        # 可选：按 chunk 索引增加一个可见偏移，专门用来验证 block_info 注入
        # 不建议在正式分割前开启，只建议做 debug 可视化
        if mark_by_chunk and block_info:
            try:
                loc = block_info[0].get("chunk-location", None)
                if loc is not None:
                    marker = sum(int(x) for x in loc) * mark_step
                    out = out + marker
            except Exception:
                pass

        # 转回目标 dtype，并做基本裁剪
        if np.issubdtype(np.dtype(out_dtype), np.integer):
            info = np.iinfo(out_dtype)
            out = np.clip(out, info.min, info.max)
        else:
            info = np.finfo(out_dtype)
            out = np.clip(out, info.min, info.max)

        return out.astype(out_dtype, copy=False)
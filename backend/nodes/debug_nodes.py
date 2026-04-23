"""
Debug BlockMap nodes for visual verification of pipeline execution.

DaskChunkMarker: A BlockMap node that applies clearly visible per-chunk patterns,
making it easy to verify that the pipeline actually executed and which chunks
were processed.

Use case: Reader -> ROI -> DaskChunkMarker -> Writer
Look at the output zarr: you should see distinct patterns per chunk that prove
the node ran on every chunk, not just "a progress bar moving".
"""

import logging
import numpy as np

from core.registry import register_node, ProgressType
from nodes.base import BaseBlockMapNode

logger = logging.getLogger("BrainFlow.DebugNodes")


@register_node("DaskChunkMarker")
class DaskChunkMarker(BaseBlockMapNode):
    """
    BlockMap debug node that applies clearly visible per-chunk markers.

    Unlike DaskScaleShift (which adds subtle float offsets), this node applies
    STRONG, EASY-TO-SEE per-chunk visual patterns so users can verify execution:
    - Output data will show distinct patterns per chunk in any zarr viewer
    - Chunk borders become clearly visible
    - Different chunks get visually distinct values

    Modes:
      checkerboard  : alternating 0/max value checker pattern per cell
      border_frame  : bright solid border + interior = 0
      chunk_index   : each chunk gets a unique uint16 value (good for uint16 output)
      stripe_h      : horizontal stripes (good for any dtype)
      stripe_v      : vertical stripes
      gradient      : diagonal gradient (visible even after后续节点加工)

    Visual strength (肉眼可辨程度):
      checkerboard  > border_frame > chunk_index > stripe_h/v > gradient

    推荐链路验证:
      OMEZarrReader -> DaskROI -> DaskChunkMarker(checkerboard) -> OMEZarrWriter
      打开 output.zarr，每个 chunk 应该有明显不同的亮度/颜色
    """
    CATEGORY = "BrainFlow/Debug"
    DISPLAY_NAME = "Chunk Marker (Debug)"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    # Safe defaults: process all blocks, fail loudly on error
    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = False
    FAILURE_POLICY = "raise"

    # Force uint16 output so marker values are clearly distinct
    OUTPUT_DTYPE = np.uint16

    # Logging throttle: only log first N chunks per node instance
    _log_counter = 0
    _LOG_CHUNK_LIMIT = 4  # 安全阈值：每个节点最多打印前 4 个 chunk

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "mode": (["checkerboard", "border_frame", "chunk_index",
                          "stripe_h", "stripe_v", "gradient"], {
                    "default": "checkerboard",
                    "tooltip": "Visual pattern applied per chunk. checkerboard=最强可见性。"
                }),
                "marker_strength": ("INT", {
                    "default": 40000,
                    "min": 1,
                    "max": 65535,
                    "tooltip": "Marker 亮区像素值（uint16 范围 1-65535）"
                }),
                "border_width": ("INT", {
                    "default": 8,
                    "min": 1,
                    "max": 64,
                    "tooltip": "border_frame 模式的边框宽度（像素）"
                }),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "execute"

    def process_block(self, block, block_info, params, runtime):
        mode = params.get("mode", "checkerboard")
        strength = int(params.get("marker_strength", 40000))
        border_w = int(params.get("border_width", 8))

        # block_info[0] 是 Dask 注入的标准 block 元数据
        loc = None
        if block_info and isinstance(block_info, (list, tuple)) and len(block_info) > 0:
            loc = block_info[0].get("chunk-location")

        # 每个 chunk 计算唯一 ID（用于 chunk_index 模式）
        chunk_id = 0
        if loc is not None:
            chunk_id = sum((int(v) + 1) * (10 ** i) for i, v in enumerate(reversed(loc)))

        # ---- 可视化标记 ----
        out = self._apply_pattern(
            block, mode, strength, border_w, chunk_id,
            loc=loc, runtime=runtime
        )
        return out

    def _apply_pattern(self, block, mode, strength, border_w, chunk_id, loc=None, runtime=None):
        """Apply the selected visual pattern to the block."""
        dtype = block.dtype
        shape = block.shape
        ndim = block.ndim

        # 创建零底板（uint16）
        out = np.zeros(shape, dtype=np.uint16)

        if mode == "checkerboard":
            # 棋盘格：每个 cell 交替 0 / strength
            cell_size = max(4, min(border_w, 8))  # 默认 cell=8px
            for idx in np.ndindex(shape):
                val = strength if all(
                    (c // cell_size) % 2 == p % 2
                    for c, p in zip(idx, idx)
                ) else 0
                out[idx] = val

        elif mode == "border_frame":
            # 边框：边缘区域 = strength，中心 = 0
            slices = []
            for dim_size in shape:
                start = min(border_w, dim_size // 4)
                end = dim_size - start
                slices.append(slice(start, end))
            interior = tuple(slices)
            # 边框
            for idx in np.ndindex(shape):
                on_border = any(
                    c < border_w or c >= shape[i] - border_w
                    for i, c in enumerate(idx)
                )
                out[idx] = strength if on_border else 0

        elif mode == "chunk_index":
            # chunk_index：每个 chunk 有唯一值（高位 chunk_id，低位=chunk内偏移）
            base_val = (chunk_id * 1000) % 65535
            # 对整个 block 填充：边缘最亮
            out[:, :] = 0
            # 只在左上角写一个明显的标记值（容易在 zarr viewer 里看到）
            h, w = shape[-2], shape[-1]
            bh, bw = min(h, border_w * 2), min(w, border_w * 2)
            out[:bh, :bw] = base_val
            # 对角线辅助（更容易辨认方向）
            for d in range(min(bh, bw)):
                out[d, d] = min(base_val + 2000, 65535)

        elif mode == "stripe_h":
            # 横条纹
            stripe_h = max(4, border_w)
            for idx in np.ndindex(shape):
                stripe_idx = idx[0] // stripe_h
                out[idx] = strength if stripe_idx % 2 == 0 else 0

        elif mode == "stripe_v":
            # 竖条纹
            stripe_w = max(4, border_w)
            for idx in np.ndindex(shape):
                stripe_idx = idx[-1] // stripe_w
                out[idx] = strength if stripe_idx % 2 == 0 else 0

        elif mode == "gradient":
            # 对角渐变（即使后续节点做 normalize 也能看到趋势）
            max_coord = max(sum(1 for _ in shape), 1)
            for idx in np.ndindex(shape):
                coord_sum = sum(idx) / max_coord
                out[idx] = int(min(coord_sum * strength, 65535))
        else:
            # fallback：全亮
            out[:, :] = strength

        # ---- 受控日志：每个节点实例最多打印前 _LOG_CHUNK_LIMIT 个 chunk ----
        cls_name = type(self).__name__
        node_id = runtime.get("node_id") if runtime else "?"
        if loc is not None and self._log_counter < self._LOG_CHUNK_LIMIT:
            mode_str = mode
            strength_str = strength
            logger.info(
                f"[{cls_name}] node={node_id} | loc={loc} | chunk_id={chunk_id} "
                f"| mode={mode_str} | strength={strength_str} | shape={shape} | dtype={dtype}"
            )
            DaskChunkMarker._log_counter += 1

        return out

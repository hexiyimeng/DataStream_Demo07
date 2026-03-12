from core.registry import register_node, ProgressType
import dask.array as da
from core.logger import logger

# Import unified progress helper
from utils.progress_helper import report_progress


@register_node("DaskROI")
class DaskROI:
    CATEGORY = "BrainFlow/DataProcessing"
    DISPLAY_NAME = " ROI Crop (切块工具)"
    PROGRESS_TYPE = ProgressType.STATE_ONLY  # 仅状态，无百分比

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                # [修复] 移除错误的转义符 \"
                "start_x": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "end_x": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "start_y": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "end_y": ("INT", {"default": 0, "min": 0, "max": 999999}),
                "start_z": ("INT", {"default": 0, "min": 0, "max": 99999}),
                "end_z": ("INT", {"default": 0, "min": 0, "max": 99999}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("cropped_dask",)
    FUNCTION = "execute"

    def execute(self, dask_arr, start_x, end_x, start_y, end_y, start_z, end_z, **kwargs):
        node_id = kwargs.get('_node_id')

        logger.info(f"[ROI] Input shape: {dask_arr.shape}")
        logger.info(f"[ROI] Crop params: x=[{start_x}:{end_x}], y=[{start_y}:{end_y}], z=[{start_z}:{end_z}]")

        # 解析参数，0 代表不限制
        sl_z = slice(start_z, end_z if end_z > 0 else None)
        sl_y = slice(start_y, end_y if end_y > 0 else None)
        sl_x = slice(start_x, end_x if end_x > 0 else None)

        # 根据维度切片
        if dask_arr.ndim == 3:
            cropped = dask_arr[sl_z, sl_y, sl_x]
        elif dask_arr.ndim == 4:
            cropped = dask_arr[:, sl_z, sl_y, sl_x]
        elif dask_arr.ndim == 5:
            cropped = dask_arr[:, :, sl_z, sl_y, sl_x]
        else:
            cropped = dask_arr

        return (cropped,)
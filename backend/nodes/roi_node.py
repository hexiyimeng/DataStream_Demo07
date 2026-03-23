from core.logger import logger
from core.registry import register_node, ProgressType


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
        logger.info(f"[ROI] Input shape: {dask_arr.shape}, ndim: {dask_arr.ndim}")
        logger.info(f"[ROI] Crop params: x=[{start_x}:{end_x}], y=[{start_y}:{end_y}], z=[{start_z}:{end_z}]")

        # 边界校验：start 必须 < end（end=0 表示不限制，跳过校验）
        if end_x > 0 and start_x >= end_x:
            raise ValueError(f"[ROI] Invalid X range: start_x={start_x} >= end_x={end_x}")
        if end_y > 0 and start_y >= end_y:
            raise ValueError(f"[ROI] Invalid Y range: start_y={start_y} >= end_y={end_y}")
        if end_z > 0 and start_z >= end_z:
            raise ValueError(f"[ROI] Invalid Z range: start_z={start_z} >= end_z={end_z}")

        # 解析参数，0 代表不限制
        sl_x = slice(start_x, end_x if end_x > 0 else None)
        sl_y = slice(start_y, end_y if end_y > 0 else None)
        sl_z = slice(start_z, end_z if end_z > 0 else None)

        # 根据维度切片（支持 2D/3D/4D/5D）
        if dask_arr.ndim == 2:
            # 2D 数据：(Y, X)
            cropped = dask_arr[sl_y, sl_x]
        elif dask_arr.ndim == 3:
            # 3D 数据：(Z, Y, X)
            cropped = dask_arr[sl_z, sl_y, sl_x]
        elif dask_arr.ndim == 4:
            # 4D 数据：(C, Z, Y, X)
            cropped = dask_arr[:, sl_z, sl_y, sl_x]
        elif dask_arr.ndim == 5:
            # 5D 数据：(T, C, Z, Y, X)
            cropped = dask_arr[:, :, sl_z, sl_y, sl_x]
        else:
            logger.warning(f"[ROI] Unsupported ndim={dask_arr.ndim}, returning unchanged")
            cropped = dask_arr

        logger.info(f"[ROI] Output shape: {cropped.shape}")
        return (cropped,)
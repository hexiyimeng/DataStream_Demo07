from registry import register_node
import dask.array as da


@register_node("DaskROI")
class DaskROI:
    CATEGORY = "BrainFlow/DataProcessing"
    DISPLAY_NAME = " ROI Crop (切块工具)"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                # 设置为 0 表示“不限制”（从头开始或切到最后）
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

    def execute(self, dask_arr, start_x, end_x, start_y, end_y, start_z, end_z):
        # 获取原始形状
        d, h, w = dask_arr.shape  # Z, Y, X

        # 处理 0 值，表示“切到尽头”
        x2 = end_x if end_x > 0 else w
        y2 = end_y if end_y > 0 else h
        z2 = end_z if end_z > 0 else d

        # 简单的切片操作 (Dask 会懒执行，非常快)
        # 注意：Zarr/Dask 的顺序通常是 (Z, Y, X)
        cropped = dask_arr[start_z:z2, start_y:y2, start_x:x2]

        print(f" [ROI] 切割范围: Z[{start_z}:{z2}], Y[{start_y}:{y2}], X[{start_x}:{x2}]")
        print(f" [ROI] 结果形状: {cropped.shape}")

        return (cropped,)
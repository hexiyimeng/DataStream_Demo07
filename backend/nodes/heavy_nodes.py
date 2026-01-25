import numpy as np
from registry import register_node

try:
    import dask.array as da
    import scipy.ndimage

    HAS_LIBS = True
except ImportError:
    HAS_LIBS = False


# =============================================================================
# 节点 1: Gamma 矫正 (DaskGamma)

# =============================================================================
@register_node("DaskGamma")
class DaskGamma:
    CATEGORY = "BrainFlow/Test"
    DISPLAY_NAME = "Gamma Correction"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "gamma": ("FLOAT", {"default": 0.7, "min": 0.1, "max": 3.0, "step": 0.1}),
                "gain": ("FLOAT", {"default": 1.0, "min": 0.1, "max": 10.0}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    FUNCTION = "execute"

    def execute(self, dask_arr, gamma, gain):
        print(f" [Gamma] 执行 Gamma={gamma}, Gain={gain}")

        # 1. 归一化到 0-1 (假设输入是 uint8 或 uint16)
        # 注意：Dask 是懒执行，这里只构建图
        # 为了健壮性，先转 float32
        arr_f = dask_arr.astype(np.float32)

        # 获取最大值 (惰性计算，这里假设数据范围或者让用户指定更好，这里简化处理直接除以 max 可能的数值)
        # 既然是生物数据，通常是 uint16 (65535) 或 uint8 (255)
        # 我们用一个简单的启发式：如果 max > 255 归一化到 65535，否则 255
        # 但为了 Dask 性能，我们直接按 float 算，最后再修剪

        # 公式: V_out = A * V_in ^ gamma
        # 避免 0 的 gamma 次方报错，加个极小值
        result = gain * da.power(arr_f, gamma)

        return (result,)


# =============================================================================
# 节点 2: 反锐化掩膜 (DaskUnsharpMask)
# 作用: 让模糊的边缘变清晰。原理是 原图 + (原图 - 模糊图) * strength
# 算力消耗: ★★★☆☆ (涉及高斯卷积，需要邻域数据)
# =============================================================================
@register_node("DaskUnsharpMask")
class DaskUnsharpMask:
    CATEGORY = "BrainFlow/Test"
    DISPLAY_NAME = "🗡️ Unsharp Masking"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "sigma": ("FLOAT", {"default": 2.0, "min": 0.1, "max": 10.0}),
                "amount": ("FLOAT", {"default": 1.5, "min": 0.1, "max": 5.0}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    FUNCTION = "execute"

    def execute(self, dask_arr, sigma, amount):
        print(f" [Unsharp] 锐化处理, Sigma={sigma}, Amount={amount}")

        # 定义核心计算函数 (在每个块上运行)
        def process_chunk(chunk, s=1.0):
            # 1. 计算高斯模糊
            blurred = scipy.ndimage.gaussian_filter(chunk, sigma=s)
            # 2. 计算蒙版 (原图 - 模糊图)
            mask = chunk - blurred
            # 3. 叠加
            sharpened = chunk + (mask * amount)
            return sharpened

        # map_overlap 是处理图像最关键的函数，它会自动处理边界重叠
        # depth 设置为 sigma 的 3 倍以上，保证卷积边缘正确
        depth = int(np.ceil(sigma * 3))

        result = dask_arr.map_overlap(
            process_chunk,
            depth=depth,
            boundary='reflect',
            dtype=np.float32,
            s=sigma
        )

        return (result,)


# =============================================================================
# 节点 3: 3D 梯度幅值 (DaskGradientMagnitude)
# 作用: 提取结构。计算三维空间的变化率，边缘会发光，平坦区域变黑。
# 算力消耗: ★★★★★ (三次 Sobel 卷积 + 平方 + 开方，CPU 杀手)
# =============================================================================
@register_node("DaskGradientMagnitude")
class DaskGradientMagnitude:
    CATEGORY = "BrainFlow/Test"
    DISPLAY_NAME = " 3D Gradient Magnitude"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    FUNCTION = "execute"

    def execute(self, dask_arr):
        print(f" [Gradient] 计算 3D 梯度幅值 (Sobel Magnitude)")

        def compute_magnitude(chunk):
            # 这里的 chunk 是一个 3D numpy array
            # 分别计算 Z, Y, X 三个方向的梯度
            # mode='constant' 填充边界
            dz = scipy.ndimage.sobel(chunk, axis=0)
            dy = scipy.ndimage.sobel(chunk, axis=1)
            dx = scipy.ndimage.sobel(chunk, axis=2)

            # 幅值公式: sqrt(dx^2 + dy^2 + dz^2)
            mag = np.sqrt(dz ** 2 + dy ** 2 + dx ** 2)
            return mag

        # Sobel 算子核大小是 3x3x3，所以 depth=1 就够了，给 2 保险
        result = dask_arr.map_overlap(
            compute_magnitude,
            depth=2,
            boundary='reflect',
            dtype=np.float32
        )

        return (result,)
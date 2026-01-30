import numpy as np
from registry import register_node
import logging

# 尝试导入 cellpose
try:
    from cellpose import models
    import torch

    HAS_CELLPOSE = True
except ImportError:
    HAS_CELLPOSE = False

try:
    import dask.array as da

    HAS_DASK = True
except ImportError:
    HAS_DASK = False

logger = logging.getLogger("BrainFlow.Cellpose")


@register_node("DaskCellpose")
class DaskCellpose:
    CATEGORY = "BrainFlow/Segmentation"
    DISPLAY_NAME = "Cellpose Segmentation "

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "diameter": ("FLOAT", {"default": 15.0, "min": 5.0, "max": 100.0}),
                "model_type": (["cyto", "nuclei", "cyto2", "cyto3"],),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0}),
                "gpu_batch_size": ("INT", {"default": 2, "min": 1, "max": 16}),
            },
            "optional": {
                "overlap": ("INT", {"default": 30, "min": 0, "max": 100}),
                "skip_threshold": ("FLOAT", {"default": 0.02, "min": 0.0, "max": 1.0}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("mask_dask",)
    FUNCTION = "execute"

    def execute(self, dask_arr, diameter, model_type, flow_threshold, cellprob_threshold, gpu_batch_size, overlap=30,
                skip_threshold=0.02):
        if not HAS_CELLPOSE:
            raise ImportError("Cellpose not installed.")

        print(f" [Cellpose] 配置: Dia={diameter}, Model={model_type}, Overlap={overlap}")

        # =========================================================
        # 1. 主进程预加载 (防止文件锁冲突)
        # =========================================================
        try:
            print(" [Main] 正在校验模型文件...")
            # 即使有警告 model_type unused，这样初始化依然能触发下载逻辑
            models.CellposeModel(gpu=False, model_type=model_type)
            print(" [Main] 模型校验完毕。")
        except Exception as e:
            print(f" [Main Warning] 模型预加载异常 (可忽略): {e}")

        # --- Worker 核心函数 ---
        def segment_chunk(block, **kwargs):
            # 2. 空块跳过优化
            # 假设数据是 uint8/uint16，如果最大值很小说明是背景
            if block.max() < 10:
                return np.zeros_like(block, dtype=np.uint16)

            try:
                use_gpu = torch.cuda.is_available()
                device = torch.device('cuda') if use_gpu else torch.device('cpu')

                # 加载模型
                model = models.CellposeModel(
                    gpu=use_gpu,
                    model_type=kwargs.get('model_type', 'cyto'),
                    device=device
                )

                # 3. 执行推理 (关键修复位)
                # Cellpose v3/v4 对 3D 数据的要求变严了
                masks, flows, styles = model.eval(
                    block,
                    diameter=kwargs.get('diameter', 15),
                    channels=None,  # 新版通常不需要手动指定 [0,0]
                    do_3D=True,
                    z_axis=0,  # <---  关键修复：显式指定 Z 轴
                    flow_threshold=kwargs.get('flow_threshold', 0.4),
                    cellprob_threshold=kwargs.get('cellprob_threshold', 0.0),
                    batch_size=kwargs.get('gpu_batch_size', 2)
                )

                return masks.astype(np.uint16)

            except Exception as e:
                # 打印更详细的错误堆栈以便调试
                import traceback
                traceback.print_exc()
                print(f"Cellpose Error in chunk: {e}")
                return np.zeros_like(block, dtype=np.uint16)

        # --- Dask 调度 ---
        # 对于 3D 数据 (Z, Y, X)，我们在三个维度都加 Padding
        depth = {0: overlap, 1: overlap, 2: overlap}
        if dask_arr.ndim == 2:
            depth = {0: overlap, 1: overlap}

        result = dask_arr.map_overlap(
            segment_chunk,
            depth=depth,
            boundary='reflect',
            dtype=np.uint16,
            model_type=model_type,
            diameter=diameter,
            flow_threshold=flow_threshold,
            cellprob_threshold=cellprob_threshold,
            gpu_batch_size=gpu_batch_size
        )

        return (result,)
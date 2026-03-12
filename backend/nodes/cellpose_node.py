import numpy as np
import logging
from functools import lru_cache
from core.registry import register_node, ProgressType
from utils.progress_helper import report_progress

# 初始化日志
logger = logging.getLogger("BrainFlow.Cellpose")

@lru_cache(maxsize=4)
def _get_cached_model(model_type: str, device_str: str):
    """
    使用 LRU 缓存模型，避免重复加载导致的显存溢出。
    """
    from cellpose import models
    import torch
    device = torch.device(device_str)
    try:
        # 尝试加载 CellposeModel (通用类)
        model = models.CellposeModel(gpu=True, model_type=model_type, device=device)
    except Exception as e:
        logger.debug(f"CellposeModel 加载异常，尝试使用基础 Cellpose 类: {e}")
        model = models.Cellpose(gpu=True, model_type=model_type, device=device)
    return model

# 将 segment_chunk 移到模块级别，使其可序列化
def _segment_chunk(block, nid=None, m_type='cyto', diam=15.0, f_thresh=0.4, c_thresh=0.0, b_size=4):
    """
    处理单个 chunk 的 Cellpose 分割。
    注意：这个函数必须在模块级别，以便 Dask 可以序列化。
    """
    # 检查块是否全空，优化计算负载
    if np.all(block == 0):
        if nid:
            # 直接调用 report_progress，不传 client
            report_progress(nid)
        return np.zeros_like(block, dtype=np.uint16)

    try:
        import torch
        device_str = "cuda" if torch.cuda.is_available() else "cpu"
        model = _get_cached_model(m_type, device_str)

        # 配置计算参数
        eval_kwargs = {
            "diameter": diam,
            "channels": [0, 0],
            "do_3D": True,
            "z_axis": 0,
            "flow_threshold": f_thresh,
            "cellprob_threshold": c_thresh,
            "batch_size": b_size,
            "progress": None  # 禁用内置进度条，它会阻塞
        }

        # 执行分割
        masks, flows, styles = model.eval(block, **eval_kwargs)

        # 计算完成后上报进度
        if nid:
            report_progress(nid)

        return masks.astype(np.uint16)

    except Exception as e:
        logger.error(f"Worker 计算节点异常: {str(e)}")
        if nid:
            report_progress(nid)
        return np.zeros_like(block, dtype=np.uint16)


@register_node("DaskCellpose")
class DaskCellpose:
    """
    Cellpose 分割节点：支持大尺度数据的分布式 2D/3D 分割。
    """
    CATEGORY = "BrainFlow/Segmentation"
    DISPLAY_NAME = "Cellpose Segmentation (Universal)"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT  # 有真实的 chunk 级进度

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "diameter": ("FLOAT", {"default": 15.0, "min": 5.0, "max": 100.0}),
                "model_type": (["cyto", "nuclei", "cyto2", "cyto3"],),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0}),
                "overlap": ("INT", {"default": 10, "min": 0, "max": 100}),
                "gpu_batch_size": ("INT", {"default": 4, "min": 1, "max": 64}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("mask_dask",)
    FUNCTION = "execute"

    def execute(self, dask_arr, diameter, model_type, flow_threshold, cellprob_threshold, overlap, **kwargs):
        current_node_id = kwargs.get('_node_id')

        # 调试：打印输入数据形状
        logger.info(f"[Cellpose] Input shape: {dask_arr.shape}, chunks: {dask_arr.chunks}, overlap: {overlap}")

        # 处理重叠区域深度
        if dask_arr.ndim == 3:
            depth = {0: overlap, 1: overlap, 2: overlap}
        else:
            depth = {0: overlap, 1: overlap}

        # 映射 Dask 重叠块计算
        result = dask_arr.map_overlap(
            _segment_chunk,  # 使用模块级别的函数
            depth=depth,
            boundary='reflect',
            dtype=np.uint16,
            nid=current_node_id,
            m_type=model_type,
            diam=diameter,
            f_thresh=flow_threshold,
            c_thresh=cellprob_threshold,
            b_size=kwargs.get('gpu_batch_size', 4)
        )

        return (result,)
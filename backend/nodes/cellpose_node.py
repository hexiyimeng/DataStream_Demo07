import numpy as np
import logging
import threading
from typing import Dict, Tuple, Optional
from core.registry import register_node, ProgressType
from utils.progress_helper import report_progress

# 初始化日志
logger = logging.getLogger("BrainFlow.Cellpose")


# =============================================================================
# 模型缓存管理：线程安全 + 引用计数
# =============================================================================
class CellposeModelCache:
    """
    线程安全的 Cellpose 模型缓存管理器。

    特性：
    - 线程锁保护模型获取/创建
    - 引用计数追踪使用中的模型
    - 只有引用计数归零时才允许清理
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        # 模型缓存：key -> (model, ref_count)
        # key = (model_type, device_str)
        self._models: Dict[Tuple[str, str], Tuple[object, int]] = {}
        # 操作锁
        self._cache_lock = threading.Lock()

    def acquire_model(self, model_type: str, device_str: str) -> object:
        """
        获取模型（增加引用计数）。
        如果模型不存在则创建。

        Args:
            model_type: 模型类型 (cyto, nuclei, cyto2, cyto3)
            device_str: 设备 (cuda, cpu)

        Returns:
            模型对象
        """
        key = (model_type, device_str)

        with self._cache_lock:
            if key in self._models:
                model, ref_count = self._models[key]
                self._models[key] = (model, ref_count + 1)
                logger.debug(f"[CellposeCache] Acquired existing model {key}, ref_count={ref_count + 1}")
                return model

            # 创建新模型
            model = self._create_model(model_type, device_str)
            self._models[key] = (model, 1)
            logger.info(f"[CellposeCache] Created new model {key}, ref_count=1")
            return model

    def release_model(self, model_type: str, device_str: str) -> bool:
        """
        释放模型（减少引用计数）。
        只有引用计数归零时才真正清理。

        Args:
            model_type: 模型类型
            device_str: 设备

        Returns:
            是否已清理（引用计数归零）

        NOTE: 当前实现在 ref_count=0 时立即删除模型。
        这确保了 GPU 内存安全释放，但牺牲了跨 execution 复用能力。

        TODO: 未来可考虑更精细的缓存策略：
        - ref_count=0 后延迟 N 秒再清理（如 60s TTL）
        - 或在显式调用 clear_if_safe() 时才清理
        - 或使用 LRU 策略限制缓存大小

        当前选择立即清理的原因：
        1. Cellpose 模型占用大量 GPU 显存（~2GB）
        2. GPU 资源稀缺，及时释放比复用更重要
        3. 单 worker 模式下，跨 execution 复用收益有限
        """
        key = (model_type, device_str)

        with self._cache_lock:
            if key not in self._models:
                logger.warning(f"[CellposeCache] Release called for unknown model {key}")
                return False

            model, ref_count = self._models[key]
            # 确保计数不会减到负数
            new_count = max(0, ref_count - 1)

            # 仅仅更新引用计数，不再执行 del self._models[key] 和 empty_cache
            self._models[key] = (model, new_count)
            logger.debug(f"[CellposeCache] Released model {key}, ref_count={new_count} (Model kept in VRAM for reuse)")

            return False

    def _create_model(self, model_type: str, device_str: str) -> object:
        """创建 Cellpose 模型"""
        from cellpose import models
        import torch

        device = torch.device(device_str)
        try:
            model = models.CellposeModel(gpu=True, model_type=model_type, device=device)
        except Exception as e:
            logger.debug(f"CellposeModel load failed, trying Cellpose: {e}")
            model = models.Cellpose(gpu=True, model_type=model_type, device=device)

        return model

    def get_active_count(self) -> int:
        """获取当前活跃模型数量（引用计数 > 0）"""
        with self._cache_lock:
            return sum(1 for _, (_, ref) in self._models.items() if ref > 0)

    def get_total_ref_count(self) -> int:
        """获取总引用计数"""
        with self._cache_lock:
            return sum(ref for _, ref in self._models.values())

    def clear_if_safe(self) -> bool:
        """
        仅在所有模型引用计数为 0 时清理。
        返回是否成功清理。
        """
        with self._cache_lock:
            total_refs = sum(ref for _, ref in self._models.values())
            if total_refs > 0:
                logger.warning(
                    f"[CellposeCache] Cannot clear: {total_refs} active references across "
                    f"{len(self._models)} models"
                )
                return False

            self._models.clear()
            # 显式清理 GPU 显存
            try:
                import torch
                import gc
                gc.collect()
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    logger.info("[CellposeCache] GPU cache cleared")
            except Exception as e:
                logger.debug(f"[CellposeCache] GPU cache clear failed: {e}")
            logger.info("[CellposeCache] Cache cleared (no active references)")
            return True

    def force_clear(self) -> int:
        """
        强制清理所有模型（危险操作）。
        返回清理的模型数量。
        """
        with self._cache_lock:
            count = len(self._models)
            total_refs = sum(ref for _, ref in self._models.values())
            if total_refs > 0:
                logger.warning(
                    f"[CellposeCache] Force clearing {count} models with "
                    f"{total_refs} active references!"
                )
            self._models.clear()
            # 显式清理 GPU 显存
            try:
                import torch
                import gc
                gc.collect()
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    logger.info("[CellposeCache] GPU cache cleared after force clear")
            except Exception as e:
                logger.debug(f"[CellposeCache] GPU cache clear failed: {e}")
            logger.info(f"[CellposeCache] Force cleared {count} models")
            return count


# 全局单例
_model_cache = CellposeModelCache()


# =============================================================================
# 兼容接口（deprecated，逐步迁移）
# =============================================================================
def clear_cellpose_model_cache() -> bool:
    """
    清空 Cellpose 模型缓存（仅在安全时执行）。
    与 torch.cuda.empty_cache() 配合使用。

    Returns:
        是否成功清理
    """
    return _model_cache.clear_if_safe()


def force_clear_cellpose_model_cache() -> int:
    """
    强制清空 Cellpose 模型缓存（危险操作，仅供测试/关闭时使用）。

    Returns:
        清理的模型数量
    """
    return _model_cache.force_clear()


# =============================================================================
# 模块级 segment 函数（Dask 可序列化）
# =============================================================================
def _segment_chunk(block, nid=None, execution_id=None, m_type='cyto', diam=15.0,
                   f_thresh=0.4, c_thresh=0.0, b_size=4):
    import torch

    # Dask metadata inference / dummy probe guard
    if block is None:
        return np.zeros((0,), dtype=np.uint16)
    if not hasattr(block, "shape"):
        return np.array((), dtype=np.uint16)
    if block.size == 0:
        return np.zeros_like(block, dtype=np.uint16)

    if np.all(block == 0):
        if nid:
            report_progress(nid, execution_id=execution_id, chunk_type="skipped")
        return np.zeros_like(block, dtype=np.uint16)

    # ==== 核心改动：动态获取当前进程被分配的 GPU ====
    device_str = "cuda:0" if torch.cuda.is_available() else "cpu"
    try:
        from distributed import get_worker
        worker = get_worker()
        # 读取 DaskService 插件打上的标签
        if hasattr(worker, 'assigned_gpu'):
            device_str = worker.assigned_gpu
    except Exception:
        # 如果不是在 Dask Worker 里运行（比如本地单步调试），就默认用 cuda:0
        pass

    # 获取模型（缓存键包含了 device_str，所以不同卡的模型是独立缓存的）
    model = _model_cache.acquire_model(m_type, device_str)

    try:
        # 根据 block 维度动态配置 3D/2D 模式
        is_3d = block.ndim >= 3
        eval_kwargs = {
            "diameter": diam,
            "channels": [0, 0],
            "do_3D": is_3d,
            "flow_threshold": f_thresh,
            "cellprob_threshold": c_thresh,
            "batch_size": b_size,
            "progress": None  # 禁用内置进度条，它会阻塞
        }
        if is_3d:
            eval_kwargs["z_axis"] = 0

        # 执行分割
        masks, flows, styles = model.eval(block, **eval_kwargs)

        # 真实推理完成后上报进度
        if nid:
            report_progress(nid, execution_id=execution_id, chunk_type="completed")

        return masks.astype(np.uint16)

    except Exception as e:
        logger.error(f"[Cellpose] Worker exception on chunk (shape={block.shape}): {str(e)}")
        # 异常时上报 failed 类型
        if nid:
            report_progress(nid, execution_id=execution_id, chunk_type="failed")
        return np.zeros_like(block, dtype=np.uint16)

    finally:
        # 释放模型（减少引用计数）
        _model_cache.release_model(m_type, device_str)


# =============================================================================
# 节点定义
# =============================================================================
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

    def execute(self, dask_arr, diameter, model_type, flow_threshold,
                cellprob_threshold, overlap, gpu_batch_size, **kwargs):
        current_node_id = kwargs.get('_node_id')
        execution_id = kwargs.get('_execution_id')

        # 调试：打印输入数据形状
        logger.info(f"[Cellpose] Input shape: {dask_arr.shape}, chunks: {dask_arr.chunks}, overlap: {overlap}")

        # 处理重叠区域深度：根据实际维度配置 overlap
        depth = {}
        if dask_arr.ndim == 2:
            depth = {0: overlap, 1: overlap}
        elif dask_arr.ndim == 3:
            depth = {0: overlap, 1: overlap, 2: overlap}
        else:
            # 4D/5D 等高维数据：前面的维度（通道/时间）不做 overlap，仅空间维度做
            for i in range(dask_arr.ndim):
                if i >= dask_arr.ndim - 3:
                    depth[i] = overlap
                else:
                    depth[i] = 0
            logger.warning(f"[Cellpose] Input ndim={dask_arr.ndim}, only applying overlap to last 3 spatial dims")

        # 映射 Dask 重叠块计算
        result = dask_arr.map_overlap(
            _segment_chunk,
            depth=depth,
            boundary='reflect',
            dtype=np.uint16,
            meta=np.array((), dtype=np.uint16),
            nid=current_node_id,
            execution_id=execution_id,
            m_type=model_type,
            diam=diameter,
            f_thresh=flow_threshold,
            c_thresh=cellprob_threshold,
            b_size=gpu_batch_size
        )

        return (result,)

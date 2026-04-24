import numpy as np
import logging
import threading
import os
from typing import Dict, Tuple, Optional
from core.registry import register_node, ProgressType
from nodes.base import BaseBlockMapNode

# 初始化日志
logger = logging.getLogger("BrainFlow.Cellpose")

# 静默 cellpose 内部日志（只在首次加载时打印，chunk 复用时也会触发但不影响正确性）
logging.getLogger("cellpose.models").setLevel(logging.WARNING)

# ==========================================
# CUDA cache 清理控制（低频可配置策略）
# ==========================================
_cuda_cleanup_counter = 0
# 0 = 默认关闭（吞吐友好）；> 0 = 每 N 次调用执行一次同步清理
_CUDA_CLEANUP_INTERVAL = int(os.getenv("BRAINFLOW_CUDA_CLEANUP_INTERVAL", "0"))


def _do_cuda_cache_cleanup():
    """
    条件性执行 CUDA cache 清理。

    策略说明：
    - 默认关闭（CUDA_CLEANUP_INTERVAL=0），避免每个 chunk 都同步+清缓存造成吞吐抖动
    - 开启时每 N 次调用才执行一次，确保 GPU 操作全部落盘后再回收
    - 仅在显式启用时打印日志，保持日常运行低噪音
    """
    global _cuda_cleanup_counter

    if _CUDA_CLEANUP_INTERVAL <= 0:
        return

    _cuda_cleanup_counter += 1
    if _cuda_cleanup_counter % _CUDA_CLEANUP_INTERVAL == 0:
        try:
            import torch
            if torch.cuda.is_available():
                torch.cuda.synchronize()
                torch.cuda.empty_cache()
                logger.info(f"[Cellpose] CUDA cache cleaned at iteration {_cuda_cleanup_counter}")
        except Exception as e:
            logger.debug(f"[Cellpose] CUDA cache cleanup failed: {e}")


# =============================================================================
# 模型缓存管理：线程安全 + 引用计数
# =============================================================================
class CellposeModelCache:
    """
    线程安全的 Cellpose 模型缓存管理器。

    缓存作用域：worker-level（进程内共享）。
    - 同一 Dask worker 进程上的多个 execution 共享同一份模型缓存。
    - ref_count 在同一 worker 跨 execution 累加，不是 per-execution 的。
    - 这允许跨 execution 模型复用（一个 execution 结束后 ref>0，模型仍留缓存）。

    设计原则：
    - 引用计数归零时模型仍保留在缓存（跨 chunk 复用，避免重复加载开销）
    - 不在 release 路径做高频 GPU 同步/清缓存，保持 Dask worker 吞吐
    - 显存泄漏防护由 clear_if_safe() / force_clear() 显式触发，或由
      _do_cuda_cache_cleanup() 低频周期性执行

    清理策略分层：
    - clear_if_safe(): 仅在"所有 execution 都结束且 ref=0"时清 — runtime 首选
    - force_clear(): 忽略 refcount，强制清 — 仅用于 cluster stop / shutdown / admin
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
        # key = (resolved_type, device_str) — resolved_type 只有 _create_model() 返回后才知道
        self._models: Dict[Tuple[str, str], Tuple[object, int]] = {}
        # 别名表：request_key -> resolved_type
        # 同一 (model_type, device_str) 首次 resolve 后记录，后续可直接走 alias → resolved_key 命中
        self._resolved_aliases: Dict[Tuple[str, str], str] = {}
        self._cache_lock = threading.Lock()

    # -------------------------------------------------------------------------
    # 公开 API
    # -------------------------------------------------------------------------
    def acquire_model(self, model_type: str, device_str: str) -> object:
        """
        获取模型（增加引用计数）。模型不存在则创建。

        Args:
            model_type: 用户请求的模型类型名
            device_str: 设备字符串，如 "cuda:0"、"cpu"

        Returns:
            模型对象
        """
        request_key = (model_type, device_str)

        # ---- Phase 1: 锁内查询 ----
        with self._cache_lock:
            # 1) alias 命中：之前已解析过同一请求，直接走 resolved_key
            #    加脏 alias 保护：alias 存在但 resolved_key 已被清除时，忽略 alias 重新创建
            if request_key in self._resolved_aliases:
                resolved_type = self._resolved_aliases[request_key]
                key = (resolved_type, device_str)
                if key in self._models:
                    model, ref_count = self._models[key]
                    self._models[key] = (model, ref_count + 1)
                    logger.info(f"[CellposeCache] Hit(alias): {resolved_type}@{device_str}, ref={ref_count + 1}")
                    return model
                # alias 指向的 resolved_key 已不存在，说明缓存被清空，忽略 alias 继续创建
                self._resolved_aliases.pop(request_key, None)

            # 2) direct 命中：model_type 本身就是 resolved_type（无 alias 路径）
            if request_key in self._models:
                model, ref_count = self._models[request_key]
                self._models[request_key] = (model, ref_count + 1)
                logger.info(f"[CellposeCache] Hit(direct): {model_type}@{device_str}, ref={ref_count + 1}")
                return model

        # ---- Phase 2: miss → 锁外创建 ----
        model, resolved_type = self._create_model(model_type, device_str)

        # ---- Phase 3: 创建后双检 ----
        dup = None
        with self._cache_lock:
            resolved_key = (resolved_type, device_str)

            # 已被其他线程创建 → 丢弃刚创建的 duplicate
            if resolved_key in self._models:
                dup = model
                model, ref_count = self._models[resolved_key]
                self._models[resolved_key] = (model, ref_count + 1)
                # 同步写 alias（这样后续同一请求走 alias 路径）
                self._resolved_aliases[request_key] = resolved_type
                logger.info(f"[CellposeCache] Hit(after-create): {resolved_type}@{device_str}, ref={ref_count + 1}")
            # 真正未命中 → 写入主缓存 + alias
            else:
                self._models[resolved_key] = (model, 1)
                self._resolved_aliases[request_key] = resolved_type
                logger.info(f"[CellposeCache] Load: {resolved_type}@{device_str}, ref=1")

        # 锁外处理 duplicate（gc.collect / empty_cache 是慢操作，不能放在临界区内）
        if dup is not None:
            self._dispose_duplicate_model(dup)

        return model

    def release_model(self, model, device_str: str) -> None:
        """
        释放模型（减少引用计数）。

        清理策略（默认保守）：
        - 只递减引用计数，不在 release 路径做 GPU 同步/empty_cache
        - 引用归零时模型继续保留在缓存（供后续 chunk 复用）
        - 显存泄漏防护由 clear_if_safe() / force_clear() 显式触发，
          或由 _do_cuda_cache_cleanup() 低频周期性执行
        """
        # 从模型对象上读取实际加载时的 resolved_type
        resolved_type = getattr(model, "_resolved_type", None)
        if not resolved_type:
            logger.warning(f"[CellposeCache] Cannot release: model has no _resolved_type attribute")
            return

        key = (resolved_type, device_str)

        with self._cache_lock:
            if key not in self._models:
                logger.warning(f"[CellposeCache] Release unknown model: {key}")
                return

            _, ref_count = self._models[key]
            new_count = max(0, ref_count - 1)
            self._models[key] = (model, new_count)

            if new_count == 0:
                logger.debug(f"[CellposeCache] Released: {resolved_type}@{device_str}, ref=0 (kept in cache)")
            else:
                logger.debug(f"[CellposeCache] Released: {resolved_type}@{device_str}, ref={new_count}")

    def get_active_count(self) -> int:
        """获取当前引用计数 > 0 的模型数量。"""
        with self._cache_lock:
            return sum(1 for _, (_, ref) in self._models.items() if ref > 0)

    def get_total_ref_count(self) -> int:
        """获取所有模型的总引用计数。"""
        with self._cache_lock:
            return sum(ref for _, ref in self._models.values())

    def clear_if_safe(self) -> bool:
        """
        仅在所有模型引用计数为 0 时清理显存。
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
            self._resolved_aliases.clear()
            try:
                import torch
                import gc
                gc.collect()
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    logger.info("[CellposeCache] GPU cache cleared (all refs released)")
            except Exception as e:
                logger.debug(f"[CellposeCache] GPU cache clear failed: {e}")
            return True

    def force_clear(self) -> int:
        """
        强制清理所有模型（危险操作，仅供测试/停机时使用）。
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
            self._resolved_aliases.clear()
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

    # -------------------------------------------------------------------------
    # 内部
    # -------------------------------------------------------------------------
    def _dispose_duplicate_model(self, model) -> None:
        """
        低频路径：仅在并发竞争导致重复创建模型被丢弃时调用。
        执行 best-effort 的 GC 和 CUDA cache 清理，不保证彻底释放。
        """
        try:
            import gc
            import torch
            del model
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass

    def _create_model(self, model_type: str, device_str: str) -> object:
        """
        创建 Cellpose 模型（Cellpose v4 风格）。

        关键设计：Cellpose v4 只有 CellPoseModel（CP4/ViT-SAM），不支持 CP3/ResNet 模型。
        cyto3/cyto/nuclei/cyto2 在用户磁盘上虽然是合法的 CellPose 模型文件，
        但它们是 CP3 格式，与 Cellpose v4 不兼容。

        修复方案：
        1. 先检查 MODEL_DIR/model_type 是否存在
        2. 读取 checkpoint 检测格式（CP4 有 W2 key，CP3 有 diam_mean key）
        3. CP3 模型 → 抛出明确错误（而不是静默 fallback 到 cpsam）
        4. CP4 模型 → 正常加载
        """
        from cellpose import models
        import os
        import torch

        if not model_type:
            model_type = "cyto3"

        device = torch.device(device_str)

        # ---- 路径解析：找到模型文件/目录的完整路径 ----
        model_path = None
        if hasattr(models, 'MODEL_DIR') and models.MODEL_DIR:
            candidate = os.path.join(models.MODEL_DIR, model_type)
            if os.path.exists(candidate):
                model_path = candidate
        if model_path is None and os.path.exists(model_type):
            model_path = model_type

        # ---- CP3 vs CP4 格式检测（提前失败，而不是静默 fallback） ----
        if model_path is not None:
            try:
                # torch.load 完整文件来检测 key
                ckpt = torch.load(model_path, map_location='cpu', weights_only=False)
                if isinstance(ckpt, dict) and 'diam_mean' in ckpt:
                    raise ValueError(
                        f"[CellposeCache] 模型 {model_type!r} 是 CellPose 3 (CP3/ResNet) 格式，"
                        f"与 Cellpose v4 不兼容。Cellpose v4 只支持 CP4 (ViT-SAM) 格式模型。"
                        f"如需使用 {model_type}，请使用 CellPose 3.x 版本。"
                    )
            except ValueError:
                raise  # CP3 检测出的异常，直接抛出
            except Exception:
                # torch.load 失败（如路径是目录），忽略，继续让 CellPoseModel 处理
                pass

        # 都不存在则用原名（CellPoseModel 内部会处理）
        if model_path is None:
            model_path = model_type

        logger.debug(f"[CellposeCache] Creating CellposeModel(pretrained_model={model_type!r}, resolved_path={model_path!r}, gpu={device.type=='cuda'}, device={device})")

        # ---- 主路径：CellposeModel(pretrained_model=...) ----
        model = models.CellposeModel(
            gpu=(device.type == "cuda"),
            pretrained_model=model_path,
            device=device,
        )

        # 读取模型实际使用的路径（fallback 后 pretrained_model 属性会变）
        # 用它反推实际加载的模型名（取 basename）
        actual_path = getattr(model, "pretrained_model", None) or model_path
        resolved_type = os.path.basename(os.path.normpath(actual_path))

        logger.debug(f"[CellposeCache]   model.pretrained_model={actual_path!r}, resolved_type={resolved_type!r}")

        # 将实际类型标记在模型对象上，供 release_model 读取，保证 key 一致
        model._resolved_type = resolved_type

        if resolved_type != model_type:
            KNOWN_MODELS = {"cyto", "nuclei", "cyto2", "cyto3", "cpsam"}
            is_fallback = (resolved_type in KNOWN_MODELS and model_type in KNOWN_MODELS)
            log_fn = logger.warning if is_fallback else logger.error
            log_fn(
                f"[CellposeCache] 请求 {model_type!r} 但实际加载了 {resolved_type!r} "
                f"({'Cellpose 内部 fallback' if is_fallback else 'pretrained_model 参数被忽略，可能存在版本兼容问题'})"
            )

        return model, resolved_type


# 全局单例
_model_cache = CellposeModelCache()


# =============================================================================
# 兼容接口
# =============================================================================
def clear_cellpose_model_cache() -> bool:
    """清空 Cellpose 模型缓存（仅在安全时执行）。"""
    return _model_cache.clear_if_safe()


def force_clear_cellpose_model_cache() -> int:
    """强制清空 Cellpose 模型缓存（危险操作）。"""
    return _model_cache.force_clear()


# =============================================================================
# Cellpose 节点
# =============================================================================
@register_node("DaskCellpose")
class DaskCellpose(BaseBlockMapNode):
    """
    Cellpose 分布式分割节点：支持大尺度 2D/3D 数据的 Dask 分布式分割。

    使用 map_blocks 并行处理每个 chunk，模型在 Dask worker 上按需加载，
    通过引用计数缓存实现跨 chunk 复用。显存治理采用低频可配置策略，
    优先保证 Dask worker 吞吐量。

    本节点继承自 BaseBlockMapNode，框架行为（skip/failure/progress）由 Base 管理。
    process_block 只包含 Cellpose 分割的核心算法逻辑。
    """
    CATEGORY = "BrainFlow/Segmentation"
    DISPLAY_NAME = "Cellpose Segmentation"

    # BlockMap 配置
    OUTPUT_DTYPE = np.uint16
    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = True   # Cellpose 特殊：全零块跳过
    FAILURE_POLICY = "zeros_like"  # Cellpose 特殊：容错，单块失败不影响其他块

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "model_type": (["cpsam"],),
                "diameter": ("FLOAT", {"default": 0.0, "min": 0.0, "max": 500.0, "tooltip": "0 = 自动估计（推荐）"}),
                "flow_threshold": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0, "tooltip": "流阈值"}),
                "cellprob_threshold": ("FLOAT", {"default": 0.0, "min": -6.0, "max": 6.0, "tooltip": "细胞概率阈值"}),
                "gpu_batch_size": ("INT", {"default": 8, "min": 1, "max": 64, "tooltip": "GPU batch size"}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("mask_dask",)
    FUNCTION = "execute"

    def process_block(self, block, block_info, params, runtime):
        """
        Cellpose 单块分割算法。

        注意：以下框架逻辑由 BaseBlockMapNode 统一管理，
        不需要在此处理：
        - 空块跳过（SKIP_EMPTY_BLOCKS=True）
        - 全零块跳过（SKIP_ALL_ZERO_BLOCKS=True）
        - 异常兜底（FAILURE_POLICY="zeros_like"）
        - progress 上报（completed/skipped/failed）
        - device_hint 解析（runtime["device_hint"] 已注入）
        """
        device_hint = runtime["device_hint"]

        # 模型获取（引用计数 +1）
        model = _model_cache.acquire_model(params["model_type"], device_hint)

        masks = None
        flows = None
        styles = None
        #TODO=========================
        #result = process(block, **params)
        try:
            # 构造 eval 参数
            eval_kwargs: dict = {
                "batch_size": params["gpu_batch_size"],
                "progress": None,  # 禁用内置进度条（阻塞主线程）
            }

            diameter = params.get("diameter", 0.0)
            if diameter > 0:
                eval_kwargs["diameter"] = diameter

            eval_kwargs["bsize"] = 256
            eval_kwargs["tile_overlap"] = 0.1
            eval_kwargs["resample"] = False

            # 3D vs 非 3D 路径（互斥）
            use_3d = (block.ndim >= 3) and True  # do_3d=True

            if use_3d:
                eval_kwargs["do_3D"] = True
                eval_kwargs["z_axis"] = 0
                if params.get("anisotropy", 1.0) != 1.0:
                    eval_kwargs["anisotropy"] = params["anisotropy"]
            else:
                eval_kwargs["do_3D"] = False
                eval_kwargs["flow_threshold"] = params.get("flow_threshold", 0.4)
                eval_kwargs["cellprob_threshold"] = params.get("cellprob_threshold", 0.0)

            # 执行分割
            masks, flows, styles = model.eval(block, **eval_kwargs)

            return masks.astype(np.uint16)
            #return result.astype(type(RETURN_TYPES[0]))

        finally:
            # 释放模型（减少引用计数；不触发 GPU 同步/清缓存）
            _model_cache.release_model(model, device_hint)

            # 显式删除中间变量，帮助 Python GC 及时回收
            if masks is not None:
                del masks
            if flows is not None:
                del flows
            if styles is not None:
                del styles

            # 低频可配置的显存清理（默认关闭，避免每个 chunk 都同步拖慢吞吐）
            _do_cuda_cache_cleanup()

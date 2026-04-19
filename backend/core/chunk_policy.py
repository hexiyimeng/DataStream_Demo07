"""
统一的 Dask Chunk 策略管理

规则：
1. Reader: 决定"初始工作块"
2. 算子: 尽量沿用，不随便 rechunk
3. Writer: 仅在"块不规则到没法安全写出"时做一次最小修正

所有 rechunk 操作必须：
- 记录原因
- 打日志
- 经过统一 helper

新增（2026-04）：
- 自适应 chunk 计算：保守默认 + 用户可覆盖
- 风险评估：大 chunk + GPU 推理 + 3D 数据时告警
"""

import os
import dask.array as da
import logging
from enum import Enum
from typing import Optional, Tuple

logger = logging.getLogger("BrainFlow.ChunkPolicy")


def _get_gpu_vram_gb():
    """获取第一块 GPU 的显存大小（GB），检测失败返回 0"""
    try:
        import torch
        if torch.cuda.is_available():
            return torch.cuda.get_device_properties(0).total_memory / (1024 ** 3)
    except Exception:
        pass
    return 0


class RechunkReason(Enum):
    """Rechunk 原因枚举"""
    READER_INIT = "reader_init"  # Reader 初始分块
    READER_MANUAL = "reader_manual"  # Reader 手动配置
    WRITER_IRREGULAR = "writer_irregular"  # Writer 处理不规则分块
    WRITER_SINGLE_CHUNK = "writer_single_chunk"  # Writer 处理单个 chunk
    OPTIMIZATION = "optimization"  # 算子优化


# ==========================================
# 风险评估常量（可被环境变量覆盖）
# ==========================================
def _get_chunk_risk_threshold():
    """获取 chunk 风险阈值（MB）"""
    default = 256  # MB
    env_val = os.getenv("BRAINFLOW_CHUNK_RISK_THRESHOLD_MB")
    if env_val:
        logger.warning(f"   -> [Override] BRAINFLOW_CHUNK_RISK_THRESHOLD_MB={env_val}")
        return float(env_val)
    return default


# ==========================================
# 自适应 chunk 计算函数
# ==========================================
def compute_recommended_chunks(shape, dtype, ndim=None, is_gpu_task=False,
                                is_inference_task=False):
    """
    根据数据属性和任务类型，推荐合适的 chunk 大小。

    设计原则：
    - 保守默认：大体数据用小 chunk，避免单 task 内存爆炸
    - 用户可覆盖：手动配置 > 自动计算
    - 风险警告：大 chunk 时提示潜在问题

    Args:
        shape: 数据形状
        dtype: 数据类型
        ndim: 维度数（可选，默认从 shape 推断）
        is_gpu_task: 是否是 GPU 任务（如 Cellpose）
        is_inference_task: 是否是推理类任务（需要额外考虑 GPU 显存）

    Returns:
        dict: {
            "chunks": 推荐 chunk 元组,
            "risk_level": "low" / "medium" / "high",
            "warnings": [警告信息列表],
            "reason": 推导依据描述
        }
    """
    if ndim is None:
        ndim = len(shape)

    dtype_bytes = dtype.itemsize if hasattr(dtype, 'itemsize') else 8
    risk_threshold_mb = _get_chunk_risk_threshold()

    result = {
        "chunks": None,
        "risk_level": "low",
        "warnings": [],
        "reason": ""
    }

    # ---------- 基础 chunk 大小（保守默认值）----------
    # 核心原则：chunk 大小应该让 Dask 任务数保持在合理范围（< 10万量级）
    # 而不是单纯"越小越安全"
    if ndim >= 3:
        spatial_shape_3d = shape[-3:]
        # 大体数据（每个维度 > 2000）：用 256~512 的 chunk
        # 避免产生百万级 Dask 任务
        if all(s > 2000 for s in spatial_shape_3d):
            base_chunk = 256
            result["warnings"].append(
                f"大数据 3D 体数据检测 (shape={spatial_shape_3d})，"
                f"使用较大 chunk={base_chunk} 避免任务数爆炸"
            )
        elif all(s > 500 for s in spatial_shape_3d):
            # 中等数据：用 128
            base_chunk = 128
        else:
            # 小数据：用 64
            base_chunk = 64
    elif ndim == 2:
        base_chunk = 512
    else:
        base_chunk = 1024

    # ---------- GPU 任务：VRAM 限制 ----------
    if is_gpu_task:
        # GPU 显存限制：每块 512³ uint8 ≈ 128MB，512³ float16/uint16 ≈ 256MB
        # 24GB VRAM / 每块 256MB ≈ 100 块并发上限
        # 但实际 GPU chunk 只需要 ~50MB (64³)，所以 GPU 任务可以用较小的 chunk
        if gpu_vram_gb := _get_gpu_vram_gb():
            if gpu_vram_gb >= 20:
                # 3090/A100 等大容量 GPU：可以用 base_chunk
                pass
            elif gpu_vram_gb >= 8:
                # 3080/4090 等：适当减小
                base_chunk = max(64, int(base_chunk * 0.75))
            else:
                # 小显存 GPU（< 8GB）：更保守
                base_chunk = max(64, int(base_chunk * 0.5))
            result["warnings"].append(
                f"GPU 显存 {gpu_vram_gb:.0f}GB，chunk={base_chunk}"
            )

    # ---------- 推理任务：适当增大 ----------
    if is_inference_task:
        # 推理任务单 task 内存较大，但吞吐量更重要
        # 对于大数据，chunk 可以更大以减少调度开销
        if ndim >= 3 and all(s > 2000 for s in shape[-3:]):
            base_chunk = min(base_chunk * 2, 512)
        else:
            base_chunk = min(base_chunk * 1.5, 256)

    # ---------- 构建 chunk 元组 ----------
    new_chunks = []
    for i, dim_size in enumerate(shape):
        if i < ndim - 3:
            # 非空间维度：保持完整
            new_chunks.append(dim_size)
        else:
            new_chunks.append(min(base_chunk, dim_size))

    # ---------- 估算 Dask 任务数量 ----------
    n_chunks_approx = 1
    for dim_size in shape[-3:]:
        c = min(base_chunk, dim_size)
        n_chunks_approx *= max(1, (dim_size + c - 1) // c)

    if n_chunks_approx > 100000:
        result["warnings"].append(
            f"⚠️ 警告: 估算 ~{n_chunks_approx:,} 个 Dask chunk。"
            f"map_overlap 会将每个 chunk 展开为多个子任务（getitem/concatenate），"
            f"实际 Dask 任务数可能达到百万量级！"
            f"建议: 增大 chunk_size（推荐 256 或 512）以减少任务总数。"
        )

    # ---------- 风险评估 ----------
    # 计算单个 chunk 的估算大小（MB）
    chunk_elements = 1
    for c, s in zip(new_chunks[-3:], shape[-3:]):
        chunk_elements *= min(c, s)
    chunk_size_mb = (chunk_elements * dtype_bytes) / (1024 ** 2)

    if chunk_size_mb > risk_threshold_mb:
        result["risk_level"] = "high"
        result["warnings"].append(
            f"⚠️ 高风险: chunk 大小 ≈ {chunk_size_mb:.0f} MB > {risk_threshold_mb} MB 阈值。"
            f"可能导致: (1) 单 task 内存过高; (2) unmanaged memory 堆积; (3) worker restart。"
            f"建议: 减小 chunk_size 或增加 worker 数量。"
        )
    elif chunk_size_mb > risk_threshold_mb * 0.7:
        result["risk_level"] = "medium"
        result["warnings"].append(
            f"⚠️ 中风险: chunk 大小 ≈ {chunk_size_mb:.0f} MB。"
            f"内存压力中等，建议监控 worker 内存使用。"
        )

    # ---------- 3D 大体数据额外警告 ----------
    if ndim >= 3:
        spatial_shape = shape[-3:]
        if all(s > 5000 for s in spatial_shape):
            result["warnings"].append(
                f"⚠️ 3D 大体数据 ({spatial_shape}) 检测到。"
                f"大 chunk + 多 worker 可能导致高并发内存峰值。"
            )

    # ---------- 推导依据 ----------
    result["chunks"] = tuple(new_chunks)
    result["reason"] = (
        f"base_chunk={base_chunk}, dtype_bytes={dtype_bytes}, "
        f"ndim={ndim}, is_gpu_task={is_gpu_task}, is_inference_task={is_inference_task}, "
        f"n_chunks_approx={n_chunks_approx}"
    )

    return result


class ChunkPolicy:
    """统一的 Chunk 策略"""

    def __init__(self):
        self.source_chunks: Optional[Tuple] = None
        self.working_chunks: Optional[Tuple] = None
        self.output_chunks: Optional[Tuple] = None
        self.rechunk_reasons: list = []
        self.chunk_risk_level: str = "unknown"
        self.chunk_warnings: list = []

    def record_rechunk(self, reason: RechunkReason, old_chunks: Tuple, new_chunks: Tuple, location: str):
        """记录 rechunk 操作"""
        self.rechunk_reasons.append({
            "reason": reason.value,
            "old_chunks": old_chunks,
            "new_chunks": new_chunks,
            "location": location
        })
        logger.info(f"[ChunkPolicy] Rechunk at {location}: {old_chunks} -> {new_chunks} ({reason.value})")

    def set_risk_assessment(self, risk_level: str, warnings: list):
        """设置风险评估结果"""
        self.chunk_risk_level = risk_level
        self.chunk_warnings = warnings
        if warnings:
            for w in warnings:
                logger.warning(f"[ChunkPolicy] {w}")

    def is_uniform(self, arr: da.Array) -> bool:
        """检查分块是否规则（所有 chunk 大小一致）"""
        if arr.chunks is None:
            return False
        return all(len(c) == 1 or len(set(c)) == 1 for c in arr.chunks)

    def is_safe_for_zarr(self, arr: da.Array) -> bool:
        """
        检查分块是否可以安全写入 Zarr。

        对于本项目，标准 Dask 分块（包括自然边界小尾块）可直接写入 Zarr。
        Dask 的 to_zarr 使用 overwrite=True 时对各种 chunk 布局都很宽容。

        Returns:
            是否可以安全写入而不需要 rechunk
        """
        # 只要分块信息存在，就认为是可直接写的
        # 不再因为边界尾块较小而误判为 unsafe
        return arr.chunks is not None

    def needs_writer_rechunk(self, arr: da.Array) -> bool:
        """
        判断 Writer 是否需要 rechunk

        只在以下情况才需要：
        1. 单个 partition（需要拆分成多个以利用并行）

        其他情况（包括自然边界尾块、规则/不规则分块）均不需要 rechunk，
        保留上游 chunk 拓扑可避免重复计算上游。
        """
        if arr.npartitions == 1:
            self.record_rechunk(RechunkReason.WRITER_SINGLE_CHUNK, arr.chunks, None, "Writer")
            logger.info(f"[ChunkPolicy] Single chunk detected, will rechunk for better parallelism")
            return True

        # 不再因为 is_safe_for_zarr 返回 False 而触发 rechunk
        # 保留上游分块拓扑，避免 writer-side task 重复消费上游
        return False

    def get_safe_output_chunks(self, arr: da.Array) -> Tuple:
        """
        获取安全的输出分块配置

        只在必要时做最小修正，保持尽可能接近原分块
        """
        ndim = arr.ndim
        shape = arr.shape

        if ndim == 3:
            # 3D 数据：使用保守的固定小分块
            z_chunk = min(shape[0], 32)
            y_chunk = min(shape[1], 256)
            x_chunk = min(shape[2], 256)

            # 避免单个 chunk
            if z_chunk == shape[0] and z_chunk > 16:
                z_chunk = 16
            if y_chunk == shape[1] and y_chunk > 64:
                y_chunk = 64
            if x_chunk == shape[2] and x_chunk > 64:
                x_chunk = 64

            return (z_chunk, y_chunk, x_chunk)
        elif ndim == 2:
            # 2D 数据
            y_chunk = min(shape[0], 256)
            x_chunk = min(shape[1], 256)

            # 避免单个 chunk
            if y_chunk == shape[0] and y_chunk > 64:
                y_chunk = 64
            if x_chunk == shape[1] and x_chunk > 64:
                x_chunk = 64

            return (y_chunk, x_chunk)
        else:
            # 其他维度：使用非常保守的分块
            return tuple(min(s, 64) for s in shape)


def safe_rechunk(arr: da.Array, new_chunks: Tuple, reason: RechunkReason, location: str,
                 policy: Optional[ChunkPolicy] = None) -> da.Array:
    """
    安全的 rechunk 辅助函数

    Args:
        arr: Dask 数组
        new_chunks: 新的分块配置
        reason: Rechunk 原因
        location: 操作位置
        policy: ChunkPolicy 实例（可选）

    Returns:
        Rechunked 数组
    """
    old_chunks = arr.chunks

    # 记录操作
    if policy:
        policy.record_rechunk(reason, old_chunks, new_chunks, location)

    # 执行 rechunk
    arr = arr.rechunk(new_chunks)

    # 验证结果
    logger.info(f"[{location}] Rechunked: {old_chunks} -> {arr.chunksize} ({reason.value})")
    logger.info(f"[{location}] Number of chunks: {arr.npartitions}, Uniform: {all(len(c) == 1 or len(set(c)) == 1 for c in arr.chunks)}")

    return arr


def reader_init_chunks(arr: da.Array, chunk_size: int = 256, keep_first_dim: bool = True,
                       manual_config: Optional[str] = None, policy: Optional[ChunkPolicy] = None,
                       is_inference_task: bool = False) -> da.Array:
    """
    Reader 初始化分块

    Args:
        arr: Dask 数组
        chunk_size: 默认 chunk 大小（仅在 manual 时使用）
        keep_first_dim: 是否保持第一维度完整
        manual_config: 手动配置字符串（如 "10,512,512" 或 "auto,512,512"）
        policy: ChunkPolicy 实例
        is_inference_task: 是否是推理类任务（影响自适应 chunk 计算）

    Returns:
        Rechunked 数组
    """
    array_shape = arr.shape
    ndim = arr.ndim

    # ---------- 如果有手动配置，优先使用 ----------
    if manual_config and manual_config.strip():
        try:
            parts = [p.strip().lower() for p in manual_config.split(",")]

            # 检查是否是 "auto" 关键字
            has_auto = any(p == "auto" or p == "-1" for p in parts)

            if has_auto:
                # 混合模式：部分自动，部分手动
                new_chunks = []
                for i, part in enumerate(parts):
                    if part == "auto" or part == "-1":
                        # 该维度自动计算
                        dim_size = array_shape[i]
                        if i == 0 and keep_first_dim:
                            new_chunks.append(dim_size)
                        elif i >= ndim - 2:
                            new_chunks.append(min(chunk_size, dim_size))
                        else:
                            new_chunks.append(min(chunk_size, dim_size))
                    else:
                        # 手动指定
                        new_chunks.append(int(part))

                # 填充剩余维度
                while len(new_chunks) < ndim:
                    new_chunks.append(chunk_size)

                new_chunks = tuple(new_chunks[:ndim])
                return safe_rechunk(arr, new_chunks, RechunkReason.READER_MANUAL, "Reader", policy)

            else:
                # 纯手动配置
                manual_chunks = [int(p) for p in parts]

                # 填充或截断到正确维度
                if len(manual_chunks) < ndim:
                    manual_chunks.extend([chunk_size] * (ndim - len(manual_chunks)))
                elif len(manual_chunks) > ndim:
                    manual_chunks = manual_chunks[:ndim]

                # 确保 chunk 不超过数组大小
                final_chunks = tuple(min(mc, arr_size) for mc, arr_size in zip(manual_chunks, array_shape))

                return safe_rechunk(arr, final_chunks, RechunkReason.READER_MANUAL, "Reader", policy)

        except (ValueError, IndexError) as e:
            logger.warning(f"[Reader] Failed to parse chunk_config '{manual_config}': {e}")
            logger.info(f"[Reader] Falling back to default chunking")

    # ---------- 默认自动分块：使用自适应计算 ----------
    # 自动检测是否是 GPU/推理任务
    is_gpu_task = False
    try:
        import torch
        is_gpu_task = torch.cuda.is_available()
    except Exception:
        pass

    chunk_recommendation = compute_recommended_chunks(
        shape=array_shape,
        dtype=arr.dtype,
        ndim=ndim,
        is_gpu_task=is_gpu_task,
        is_inference_task=is_inference_task
    )

    new_chunks = chunk_recommendation["chunks"]

    # 如果 policy 存在，记录风险评估
    if policy:
        policy.set_risk_assessment(chunk_recommendation["risk_level"], chunk_recommendation["warnings"])
        logger.info(f"[ChunkPolicy] 自动 chunk 计算: {chunk_recommendation['reason']}")
        logger.info(f"[ChunkPolicy] 推荐 chunks: {new_chunks}, 风险等级: {chunk_recommendation['risk_level']}")

    return safe_rechunk(arr, new_chunks, RechunkReason.READER_INIT, "Reader", policy)


def writer_minimal_rechunk(arr: da.Array, policy: Optional[ChunkPolicy] = None) -> da.Array:
    """
    Writer 最小化 rechunk

    只在必要时修正分块，避免不必要的重分块

    Args:
        arr: Dask 数组
        policy: ChunkPolicy 实例

    Returns:
        可能 rechunked 的数组
    """
    # 检查是否需要 rechunk
    if not policy or not policy.needs_writer_rechunk(arr):
        logger.info(f"[Writer] No rechunk needed, using existing chunks: {arr.chunksize}")
        return arr

    # 获取安全的输出分块
    new_chunks = policy.get_safe_output_chunks(arr)

    # 执行 rechunk
    return safe_rechunk(arr, new_chunks, RechunkReason.WRITER_IRREGULAR, "Writer", policy)
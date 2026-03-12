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
"""

import dask.array as da
import logging
from enum import Enum
from typing import Optional, Tuple

logger = logging.getLogger("BrainFlow.ChunkPolicy")


class RechunkReason(Enum):
    """Rechunk 原因枚举"""
    READER_INIT = "reader_init"  # Reader 初始分块
    READER_MANUAL = "reader_manual"  # Reader 手动配置
    WRITER_IRREGULAR = "writer_irregular"  # Writer 处理不规则分块
    WRITER_SINGLE_CHUNK = "writer_single_chunk"  # Writer 处理单个 chunk
    OPTIMIZATION = "optimization"  # 算子优化


class ChunkPolicy:
    """统一的 Chunk 策略"""

    def __init__(self):
        self.source_chunks: Optional[Tuple] = None
        self.working_chunks: Optional[Tuple] = None
        self.output_chunks: Optional[Tuple] = None
        self.rechunk_reasons: list = []

    def record_rechunk(self, reason: RechunkReason, old_chunks: Tuple, new_chunks: Tuple, location: str):
        """记录 rechunk 操作"""
        self.rechunk_reasons.append({
            "reason": reason.value,
            "old_chunks": old_chunks,
            "new_chunks": new_chunks,
            "location": location
        })
        logger.info(f"[ChunkPolicy] Rechunk at {location}: {old_chunks} -> {new_chunks} ({reason.value})")

    def is_uniform(self, arr: da.Array) -> bool:
        """检查分块是否规则"""
        if arr.chunks is None:
            return False
        return all(len(c) == 1 or len(set(c)) == 1 for c in arr.chunks)

    def needs_writer_rechunk(self, arr: da.Array) -> bool:
        """
        判断 Writer 是否需要 rechunk

        只有在以下情况才需要：
        1. 分块不规则（无法安全写出）
        2. 单个 chunk（map_overlap 产生的问题）
        """
        # 检查是否不规则
        if not self.is_uniform(arr):
            self.record_rechunk(RechunkReason.WRITER_IRREGULAR, arr.chunksize, None, "Writer")
            return True

        # 检查是否单个 chunk
        if arr.npartitions == 1:
            self.record_rechunk(RechunkReason.WRITER_SINGLE_CHUNK, arr.chunksize, None, "Writer")
            return True

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


def safe_rechunk(arr: da.Array, new_chunks: Tuple, reason: RechunkReason, location: str, policy: Optional[ChunkPolicy] = None) -> da.Array:
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
    old_chunks = arr.chunksize

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
                       manual_config: Optional[str] = None, policy: Optional[ChunkPolicy] = None) -> da.Array:
    """
    Reader 初始化分块

    Args:
        arr: Dask 数组
        chunk_size: 默认 chunk 大小
        keep_first_dim: 是否保持第一维度完整
        manual_config: 手动配置字符串（如 "10,512,512" 或 "auto,512,512"）
        policy: ChunkPolicy 实例

    Returns:
        Rechunked 数组
    """
    array_shape = arr.shape
    ndim = arr.ndim

    # 如果有手动配置，优先使用
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

    # 默认自动分块
    new_chunks = []
    for i, dim_size in enumerate(array_shape):
        if i == 0 and keep_first_dim:
            # 第一维度保持完整（通道/时间）
            new_chunks.append(dim_size)
        elif i >= ndim - 2:
            # 最后两个维度（空间维度）使用指定值
            new_chunks.append(min(chunk_size, dim_size))
        else:
            # 中间维度也使用指定值
            new_chunks.append(min(chunk_size, dim_size))

    new_chunks = tuple(new_chunks)
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
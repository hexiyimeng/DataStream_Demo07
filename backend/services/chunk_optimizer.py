"""
Smart chunking optimizer for BrainFlow.

This module provides intelligent chunk size calculation based on data characteristics
and hardware capabilities to optimize Dask array processing performance.
"""

import numpy as np
import logging
import os
from typing import Tuple, Dict, Optional, Union

# Import logger and config with fallbacks
try:
    from core.logger import logger
except ImportError:
    logger = logging.getLogger("BrainFlow.ChunkOptimizer")

try:
    from core.config import config
except ImportError:
    # Fallback config
    class Config:
        N_WORKERS = 1
    config = Config()


logger = logging.getLogger("BrainFlow.ChunkOptimizer")


class ChunkOptimizer:
    """
    Intelligent chunk size calculator for Dask arrays.

    This class calculates optimal chunk sizes based on:
    - Data array shape and dtype
    - Available GPU memory
    - Number of Dask workers
    - Processing requirements
    """

    # Recommended chunk sizes in bytes
    IDEAL_CHUNK_SIZE = 256 * 1024 * 1024  # 256MB
    MIN_CHUNK_SIZE = 64 * 1024 * 1024     # 64MB
    MAX_CHUNK_SIZE = 1024 * 1024 * 1024   # 1GB

    # Processing time estimation (seconds per chunk)
    SECONDS_PER_CHUNK = 0.5

    @classmethod
    def calculate_chunks(cls, array_shape: tuple, dtype: Union[np.dtype, str],
                        gpu_memory_gb: float = 0, n_workers: int = 1) -> tuple:
        """
        Calculate optimal chunk shape for a given array.

        Args:
            array_shape: Shape of the data array
            dtype: Data type of the array
            gpu_memory_gb: Available GPU memory in GB (0 if CPU only)
            n_workers: Number of Dask workers

        Returns:
            Optimal chunk shape tuple
        """
        if isinstance(dtype, str):
            dtype = np.dtype(dtype)

        item_size = dtype.itemsize if hasattr(dtype, 'itemsize') else 4  # fallback to 4 bytes
        total_elements = np.prod(array_shape)
        total_bytes = total_elements * item_size

        # 1. Adjust target chunk size based on GPU memory
        target_chunk_bytes = cls.IDEAL_CHUNK_SIZE
        if gpu_memory_gb > 0:
            # GPU mode: chunk shouldn't exceed 10% of GPU memory per worker
            gpu_memory_bytes = gpu_memory_gb * 1024**3
            max_gpu_chunk = gpu_memory_bytes * 0.1 / n_workers
            target_chunk_bytes = min(target_chunk_bytes, max_gpu_chunk)

        # 2. Clamp to reasonable range
        target_chunk_bytes = np.clip(
            target_chunk_bytes,
            cls.MIN_CHUNK_SIZE,
            cls.MAX_CHUNK_SIZE
        )

        # 3. Calculate target number of elements
        target_elements = int(target_chunk_bytes / item_size)

        # 4. Distribute across dimensions (prefer power-of-2 sizes)
        chunks = cls._distribute_chunks(array_shape, target_elements)

        logger.debug(f"Calculated chunks {chunks} for shape {array_shape} "
                    f"(target: {target_chunk_bytes / 1024**2:.1f}MB)")

        return chunks

    @classmethod
    def _distribute_chunks(cls,array_shape: tuple, target_elements: int) -> tuple:
        """
        Distribute chunk sizes across array dimensions.

        Strategy:
        - For 3D/4D image data (T, Z, Y, X) or (C, Z, Y, X)
        - Keep first 1-2 dimensions intact when possible
        - Chunk spatial dimensions (Y, X) first, then Z
        - Use power-of-2 sizes for better memory alignment
        """
        ndim = len(array_shape)
        if ndim < 2:
            return array_shape

        chunks = list(array_shape)

        # Calculate how many chunks we need
        total_elements = np.prod(array_shape)
        n_chunks = max(1, total_elements // target_elements)

        # For image data, prioritize chunking spatial dimensions
        if ndim >= 2:
            # Start with X dimension (last)
            remaining_chunks = n_chunks
            for i in range(ndim - 1, -1, -1):
                if remaining_chunks <= 1:
                    break

                # Don't chunk the first dimension for time series (T)
                # or channel dimension (C)
                if i == 0 and ndim >= 4:
                    continue

                # Calculate chunk size for this dimension
                dim_size = array_shape[i]
                chunk_factor = min(remaining_chunks, dim_size)
                chunk_size = cls._nearest_power_of_2(dim_size // chunk_factor)

                if chunk_size > 0:
                    chunks[i] = max(1, chunk_size)
                    remaining_chunks = remaining_chunks // (dim_size // chunk_size)

        return tuple(chunks)

    @staticmethod
    def _nearest_power_of_2(n: int) -> int:
        """Return the nearest power of 2 to n."""
        if n <= 0:
            return 1
        return 2 ** int(np.log2(n) + 0.5)  # Round to nearest power of 2

    @classmethod
    def estimate_processing_time(cls, array_shape: tuple, dtype: Union[np.dtype, str],
                                n_workers: int = 1, chunk_shape: tuple = None) -> Dict[str, Union[float, int]]:
        """
        Estimate processing time for an array.

        Args:
            array_shape: Shape of the data array
            dtype: Data type
            n_workers: Number of workers
            chunk_shape: Optional specific chunk shape to use

        Returns:
            Dictionary with estimated time and chunk information
        """
        if chunk_shape is None:
            chunk_shape = cls.calculate_chunks(array_shape, dtype, 0, n_workers)

        # Calculate number of chunks
        n_chunks = 1
        for s, c in zip(array_shape, chunk_shape):
            n_chunks *= (s + c - 1) // c

        # Estimate time (this is a rough estimate and should be calibrated)
        total_seconds = (n_chunks / n_workers) * cls.SECONDS_PER_CHUNK

        return {
            "estimated_seconds": total_seconds,
            "n_chunks": n_chunks,
            "chunks_per_worker": max(1, n_chunks // n_workers),
            "chunk_shape": chunk_shape,
            "total_size_gb": np.prod(array_shape) * np.dtype(dtype).itemsize / 1024**3
        }

    @classmethod
    def auto_configure(cls, array_shape: tuple, dtype: Union[np.dtype, str],
                      available_memory_gb: float = None) -> Dict[str, any]:
        """
        Auto-configure processing parameters based on available resources.

        Args:
            array_shape: Data array shape
            dtype: Data type
            available_memory_gb: Available system memory (auto-detected if None)

        Returns:
            Configuration dictionary
        """
        if available_memory_gb is None:
            available_memory_gb = cls._detect_available_memory()

        # Get GPU info
        gpu_memory_gb = cls._detect_gpu_memory()

        # Calculate optimal chunks
        chunk_shape = cls.calculate_chunks(
            array_shape, dtype, gpu_memory_gb, config.N_WORKERS
        )

        # Estimate processing time
        time_estimate = cls.estimate_processing_time(
            array_shape, dtype, config.N_WORKERS, chunk_shape
        )

        return {
            "chunk_shape": chunk_shape,
            "recommended_workers": config.N_WORKERS,
            "gpu_memory_gb": gpu_memory_gb,
            "system_memory_gb": available_memory_gb,
            "time_estimate": time_estimate,
            "memory_per_chunk_gb": (
                np.prod(chunk_shape) * np.dtype(dtype).itemsize / 1024**3
            )
        }

    @staticmethod
    def _detect_available_memory() -> float:
        """Detect available system memory in GB."""
        try:
            import psutil
            return psutil.virtual_memory().available / 1024**3
        except ImportError:
            # Fallback: assume 8GB available
            return 8.0

    @staticmethod
    def _detect_gpu_memory() -> float:
        """Detect available GPU memory in GB."""
        try:
            import torch
            if torch.cuda.is_available():
                # Get memory for current device
                free_mem = torch.cuda.mem_get_info()[0]  # Free memory in bytes
                return free_mem / 1024**3
        except Exception:
            pass
        return 0.0


def optimize_dask_array(dask_arr, gpu_memory_gb: float = 0, target_chunk_size_mb: float = None):
    """
    Convenience function to optimize an existing Dask array's chunking.

    Args:
        dask_arr: Input Dask array
        gpu_memory_gb: Available GPU memory in GB
        target_chunk_size_mb: Target chunk size in MB (uses default if None)

    Returns:
        Rechunked Dask array if beneficial, otherwise original array
    """
    if target_chunk_size_mb:
        # Override default chunk size
        original_ideal = ChunkOptimizer.IDEAL_CHUNK_SIZE
        ChunkOptimizer.IDEAL_CHUNK_SIZE = int(target_chunk_size_mb * 1024 * 1024)

    try:
        optimal_chunks = ChunkOptimizer.calculate_chunks(
            dask_arr.shape,
            dask_arr.dtype,
            gpu_memory_gb,
            config.N_WORKERS
        )

        # Only rechunk if significantly different
        current_chunks = dask_arr.chunksize
        if optimal_chunks != current_chunks:
            chunk_diff = sum(abs(a - b) for a, b in zip(optimal_chunks, current_chunks))
            if chunk_diff > sum(current_chunks) * 0.1:  # 10% difference threshold
                logger.info(f"Rechunking array: {current_chunks} -> {optimal_chunks}")
                return dask_arr.rechunk(optimal_chunks)

    finally:
        if target_chunk_size_mb:
            # Restore original value
            ChunkOptimizer.IDEAL_CHUNK_SIZE = original_ideal

    return dask_arr

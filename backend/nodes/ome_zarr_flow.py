import os
import asyncio
import logging
import numcodecs
import numpy as np
import zarr
import dask.array as da
from core.registry import register_node, ProgressType
from services.dask_service import dask_service
from core.config import config
from core.chunk_policy import ChunkPolicy, reader_init_chunks, RechunkReason


# 创建logger
logger = logging.getLogger("BrainFlow.OMEZarr")


# =============================================================================
#  节点：OME-Zarr 读取器 (Reader)
# =============================================================================
@register_node("OMEZarrReader")
class OMEZarrReader:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "📂 OME-Zarr Reader (Dask)"
    DESCRIPTION = "读取 OME-Zarr 格式的数据，支持 2D/3D/4D/5D 数据。Chunk 模式：default_3d（推荐）/ manual（自定义）。"
    PROGRESS_TYPE = ProgressType.STATE_ONLY  # 仅状态，无百分比

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": "", "multiline": False, "placeholder": "C:/Data/brain.zarr"}),
            },
            "optional": {
                # Chunk 模式选择
                "chunk_mode": (["default_3d", "manual"], {"default": "default_3d"}),

                # 默认3D模式：x/y/z 方向的分块大小
                "chunk_z": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Z Chunk Size"}),
                "chunk_y": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Y Chunk Size"}),
                "chunk_x": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "X Chunk Size"}),

                # 手动模式：使用字符串配置
                "chunk_config": ("STRING", {
                    "default": "",
                    "multiline": False,
                    "label": "Chunk Config (e.g., '10,512,512')",
                    "placeholder": "Manual config"
                }),

                # 简化的单值配置
                "chunk_size": ("INT", {
                    "default": 512,
                    "min": 64,
                    "max": 4096,
                    "label": "Chunk Size (all spatial dims)"
                }),

                # 是否保持第一维度完整（通道/时间维度）
                "keep_first_dim": ("BOOLEAN", {"default": True, "label": "Keep First Dimension Intact"}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY", "DICT")
    RETURN_NAMES = ("dask_arr", "metadata")
    FUNCTION = "load_zarr"

    def load_zarr(self, file_path, chunk_mode="default_3d",
                  chunk_z=64, chunk_y=64, chunk_x=64,
                  chunk_config="", chunk_size=512, keep_first_dim=True,
                  callback=None, **kwargs):
        node_id = kwargs.get('_node_id', 'unknown')

        # 1. 基本验证
        if not file_path:
            raise ValueError(f"[Node {node_id}] File path cannot be empty")

        if isinstance(file_path, str):
            file_path = file_path.strip()
            file_path = file_path.strip('"').strip("'")
            if '\x00' in file_path:
                raise ValueError(f"[Node {node_id}] File path contains null bytes")
            if len(file_path) > 4096:
                raise ValueError(f"[Node {node_id}] File path too long")

        # 2. 标准化路径（先 resolve symlink）
        try:
            resolved_path = os.path.realpath(file_path)
        except (OSError, ValueError) as e:
            raise ValueError(f"[Node {node_id}] Invalid path")

        # 3. 校验通过，使用规范化后的路径
        file_path = resolved_path

        # 4. 存在性和类型检查
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"[Node {node_id}] File not found: {os.path.basename(file_path)}")

        if not os.path.isdir(file_path):
            raise ValueError(f"[Node {node_id}] OME-Zarr must be a directory")

        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"[Node {node_id}] No read permission")

        # 5. Zarr 格式验证
        try:
            zarr_files = [f for f in os.listdir(file_path)
                          if f in ('.zarray', '.zgroup') or f.endswith('.zarr')]
            if not zarr_files:
                raise ValueError(f"[Node {node_id}] Not a valid OME-Zarr format")
        except PermissionError:
            raise PermissionError(f"[Node {node_id}] Cannot list directory")

        logger.info(f"[ZarrReader] Loading: {file_path}")

        try:
            z_arr = None
            multiscales = []

            # =========================================================
            # 策略 1：先尝试直接打开 array（处理 s0 这种直接是 array 的情况）
            # =========================================================
            try:
                z_arr = zarr.open_array(file_path, mode='r')
                logger.info(f"[ZarrReader] Loaded array directly from path")
            except Exception as e:
                logger.debug(f"[ZarrReader] Not a direct array: {e}")

            # =========================================================
            # 策略 2：如果失败，尝试打开 group（标准 OME-Zarr 格式）
            # =========================================================
            if z_arr is None:
                try:
                    store = zarr.open_group(file_path, mode='r')
                    logger.info(f"[ZarrReader] Opened as group")

                    # 解析 OME-NGFF Multiscales 元数据
                    multiscales = store.attrs.get("multiscales", [])
                    dataset_path = "0"  # 默认路径
                    if multiscales:
                        datasets = multiscales[0].get("datasets", [])
                        if datasets:
                            dataset_path = datasets[0]["path"]

                    # 从 dataset_path 加载
                    if dataset_path in store:
                        try:
                            z_arr = store[dataset_path]
                            logger.info(f"[ZarrReader] Loaded array from dataset path: {dataset_path}")
                        except Exception as e:
                            logger.debug(f"Failed to load from dataset path '{dataset_path}': {e}")

                    # 如果失败，尝试遍历 keys
                    if z_arr is None:
                        keys = list(store.keys())
                        logger.info(f"[ZarrReader] Available keys in group: {keys}")
                        for key in keys:
                            try:
                                candidate = store[key]
                                if hasattr(candidate, 'shape'):
                                    z_arr = candidate
                                    logger.info(f"[ZarrReader] Loaded array from group key: {key}")
                                    break
                            except Exception as e:
                                logger.debug(f"Failed to load from key '{key}': {e}")
                                continue

                except Exception as e:
                    logger.debug(f"[ZarrReader] Failed to open as group: {e}")

            # =========================================================
            # 验证结果
            # =========================================================
            if z_arr is None:
                raise ValueError(f"No valid zarr array found at: {file_path}")

            if not hasattr(z_arr, 'shape'):
                raise ValueError(f"Loaded object is not a valid array")

            # 4. 构建 Dask Array (懒加载)
            dask_arr = da.from_zarr(z_arr)

            # ==================== 收集文件信息 ====================
            original_chunks = z_arr.chunks
            array_shape = z_arr.shape
            array_dtype = str(z_arr.dtype)
            ndim = z_arr.ndim

            # 计算文件大小估算
            total_elements = np.prod(array_shape)
            total_bytes = total_elements * z_arr.dtype.itemsize
            total_gb = total_bytes / (1024**3)

            # 解析维度名称
            dim_names = self._get_dimension_names(multiscales, ndim)

            # 构建可读的形状描述
            shape_desc = self._format_shape_description(array_shape, dim_names)

            file_info = f"""📊 Zarr File Information:
  Path: {file_path}
  Dimensions: {ndim}D {shape_desc}
  Data Type: {array_dtype}
  Total Size: {total_gb:.2f} GB
  Original Chunks: {original_chunks}
  Original Chunk Size: {np.prod(original_chunks) * z_arr.dtype.itemsize / 1024**2:.2f} MB
  Number of Chunks: {dask_arr.npartitions}
"""

            logger.info(f"[ZarrReader] File loaded: {shape_desc}, {total_gb:.2f}GB, {dask_arr.npartitions} chunks")

            # ==================== Chunk 配置 (统一策略) ====================
            policy = ChunkPolicy()
            policy.source_chunks = dask_arr.chunksize

            # chunk_mode 兼容性处理：旧的 "auto" 映射到 "default_3d"
            effective_chunk_mode = chunk_mode
            if chunk_mode == "auto":
                logger.warning("[ZarrReader] chunk_mode='auto' is deprecated, treating as 'default_3d'")
                effective_chunk_mode = "default_3d"

            if effective_chunk_mode == "manual" and chunk_config:
                # 手动模式：使用 chunk_config 字符串
                dask_arr = reader_init_chunks(
                    dask_arr,
                    chunk_size=chunk_size,
                    keep_first_dim=keep_first_dim,
                    manual_config=chunk_config,
                    policy=policy
                )
            elif effective_chunk_mode == "default_3d":
                # 默认3D模式：构造 chunk_config 字符串
                if ndim == 3:
                    config_str = f"{chunk_z},{chunk_y},{chunk_x}"
                elif ndim == 4:
                    config_str = f"-1,{chunk_z},{chunk_y},{chunk_x}"  # 保持第一维完整
                else:
                    config_str = ""  # 使用默认
                dask_arr = reader_init_chunks(
                    dask_arr,
                    chunk_size=chunk_size,
                    keep_first_dim=keep_first_dim,
                    manual_config=config_str,
                    policy=policy
                )
            else:
                # 默认：使用默认 chunk_size
                dask_arr = reader_init_chunks(
                    dask_arr,
                    chunk_size=chunk_size,
                    keep_first_dim=keep_first_dim,
                    manual_config=None,
                    policy=policy
                )

            policy.working_chunks = dask_arr.chunksize
            logger.info(f"[ZarrReader] Final chunks: {policy.working_chunks}")

            # ==================== 构建元数据 ====================
            metadata = {
                "source_path": file_path,
                "shape": dask_arr.shape,
                "dtype": str(dask_arr.dtype),
                "chunks": dask_arr.chunksize,
                "ndim": dask_arr.ndim,
                "npartitions": dask_arr.npartitions,
                "voxel_size": [1.0] * dask_arr.ndim,
                "axes": dim_names,
                "file_info": file_info,
                "original_chunks": original_chunks,
            }

            # 尝试从 OME 元数据中提取物理尺寸
            if multiscales:
                if not dim_names:
                    axes = multiscales[0].get("axes", [])
                    if axes:
                        dim_names = axes
                        metadata["axes"] = dim_names

                datasets = multiscales[0].get("datasets", [])
                if datasets:
                    transforms = datasets[0].get("coordinateTransformations", [])
                    for t in transforms:
                        if t.get("type") == "scale":
                            metadata["voxel_size"] = t.get("scale", [1.0] * dask_arr.ndim)

            return (dask_arr, metadata)

        except Exception as e:
            import traceback
            traceback.print_exc()
            raise RuntimeError(f"Failed to load OME-Zarr: {e}")

    @staticmethod
    def _get_dimension_names(multiscales, ndim):
        """从 OME 元数据或默认规则获取维度名称"""
        if multiscales:
            axes = multiscales[0].get("axes", [])
            if axes and len(axes) == ndim:
                return [ax.get("name", ax) for ax in axes]

        # 默认维度命名规则
        if ndim == 2:
            return ["Y", "X"]
        elif ndim == 3:
            return ["Z", "Y", "X"]
        elif ndim == 4:
            return ["C", "Z", "Y", "X"]
        elif ndim == 5:
            return ["T", "C", "Z", "Y", "X"]
        else:
            return [f"dim_{i}" for i in range(ndim)]

    @staticmethod
    def _format_shape_description(shape, dim_names):
        """格式化形状描述"""
        parts = []
        for name, size in zip(dim_names, shape):
            parts.append(f"{name}={size}")
        return "(" + ", ".join(parts) + ")"


# =============================================================================
#  节点：OME-Zarr 写入器 (Writer)
# =============================================================================
def _normalize_axes_for_ngff(axes, ndim):
    if not axes:
        if ndim == 2:
            return [{"name": "y"}, {"name": "x"}]
        if ndim == 3:
            return [{"name": "z"}, {"name": "y"}, {"name": "x"}]
        if ndim == 4:
            return [{"name": "c"}, {"name": "z"}, {"name": "y"}, {"name": "x"}]
        return [{"name": f"dim_{i}"} for i in range(ndim)]

    normalized = []
    for ax in axes:
        if isinstance(ax, dict):
            normalized.append(ax)
        else:
            normalized.append({"name": str(ax).lower()})
    return normalized


# =============================================================================
# Block-delayed writer helpers
# =============================================================================

def _chunk_origins_from_chunks(chunks):
    """
    Calculate origin offsets for each chunk along each dimension.

    Parameters
    ----------
    chunks : tuple of tuples
        e.g. ((64, 64, 32), (64, 64, 32)) for 2D or ((8, 4), (64, 64, 64), (64, 64, 64)) for 3D

    Returns
    -------
    origins_per_dim : list of lists
        origins_per_dim[dim][chunk_idx] = start_offset
    """
    origins_per_dim = []
    for dim_chunks in chunks:
        origins = []
        offset = 0
        for chunk_size in dim_chunks:
            origins.append(offset)
            offset += chunk_size
        origins_per_dim.append(origins)
    return origins_per_dim


def _init_zarr_store(abs_path, shape, chunks, dtype, compressor):
    """
    Initialize a zarr store / dataset "0" for block-by-block writing.

    Parameters
    ----------
    abs_path : str
        Root path to the zarr group
    shape : tuple
        Full array shape
    chunks : tuple
        Chunk sizes per dimension (must match dask array chunking)
    dtype : dtype
        Data type
    compressor : numcodecs.codec or None
        Compression codec
    """
    import shutil
    # Remove existing directory for clean overwrite
    if os.path.exists(abs_path):
        shutil.rmtree(abs_path)

    z = zarr.open_group(abs_path, mode='w')
    z.create_dataset(
        "0",
        shape=shape,
        chunks=chunks,
        dtype=dtype,
        compressor=compressor,
        overwrite=True,
    )
    logger.info(f"[ZarrWriter] Store initialized: {abs_path}, shape={shape}, chunks={chunks}")


def _write_block_to_region(block, abs_path, region_slices, node_id=None, execution_id=None):
    """
    Write a single numpy block to a specific region of the zarr dataset "0".

    Parameters
    ----------
    block : ndarray
        The computed block data (already a concrete ndarray, not lazy)
    abs_path : str
        Root path to the zarr group
    region_slices : tuple of slices
        Defines where to write this block in the full array
    node_id : str, optional
    execution_id : str, optional
    """
    try:
        z = zarr.open(abs_path, mode='r+')
        z["0"][region_slices] = block
    except Exception as e:
        logger.error(f"[ZarrWriter] Failed to write block at region {region_slices}: {e}")
        raise

    if node_id and execution_id:
        try:
            from utils.progress_helper import report_progress
            report_progress(node_id, execution_id=execution_id, chunk_type="completed")
        except Exception:
            pass


def _finalize_store(abs_path, ndim, metadata):
    """
    Write OME-NGFF metadata after all block writes complete.

    Parameters
    ----------
    abs_path : str
    ndim : int
    metadata : dict or None

    Returns
    -------
    str : abs_path
    """
    try:
        z = zarr.open(abs_path, mode='r+')

        axes = _normalize_axes_for_ngff(None, ndim)
        voxel_size = [1.0] * ndim
        if metadata:
            if metadata.get("axes"):
                axes = _normalize_axes_for_ngff(metadata["axes"], ndim)
            if metadata.get("voxel_size") and len(metadata["voxel_size"]) == ndim:
                voxel_size = metadata["voxel_size"]

        datasets_meta = [{
            "path": "0",
            "coordinateTransformations": [{"type": "scale", "scale": voxel_size}]
        }]
        z.attrs["multiscales"] = [{
            "version": "0.4",
            "name": "processed",
            "datasets": datasets_meta,
            "axes": axes,
            "type": "gaussian"
        }]
        logger.info(f"[ZarrWriter] OME-NGFF metadata written. Axes: {[a['name'] for a in axes]}, Scale: {voxel_size}")
    except Exception as e:
        logger.warning(f"[ZarrWriter] Failed to write OME metadata: {e}")

    logger.info(f"[ZarrWriter] Write complete: {abs_path}")
    return abs_path


def _wait_all_and_finalize(write_results, abs_path, ndim, metadata):
    """
    No-op collector that waits for all block write results to complete,
    then writes metadata and returns the path.

    write_results : list
        Ignored — only exists to create an explicit data dependency on all writes.
    """
    return _finalize_store(abs_path, ndim, metadata)


@register_node("OMEZarrWriter")
class OMEZarrWriter:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "💾 OME-Zarr Writer (Save)"
    DESCRIPTION = "构建写盘的 lazy 计划，由 executor 统一触发计算。"
    OUTPUT_NODE = True
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT  # 通过 map_blocks 钩子上报 chunk 进度

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "output_path": ("STRING", {"default": "output.zarr", "multiline": False}),
                "compressor_name": (["default", "zstd", "blosc", "lz4", "none"],),
            },
            "optional": {
                "metadata": ("DICT",),
            }
        }

    # 返回 DELAYED 类型，executor 识别后统一提交
    RETURN_TYPES = ("DELAYED",)
    RETURN_NAMES = ("write_task",)
    FUNCTION = "save_zarr"

    def save_zarr(self, dask_arr, output_path, compressor_name="default", metadata=None, **kwargs):
        import dask
        node_id = kwargs.get('_node_id', 'unknown_writer')
        execution_id = kwargs.get('_execution_id')

        # 路径处理
        if not output_path:
            output_path = "output.zarr"
        if not output_path.lower().endswith(".zarr"):
            output_path += ".zarr"
        abs_path = os.path.abspath(output_path)

        # 压缩器配置
        if compressor_name == "zstd":
            compressor = numcodecs.Zstd(level=3)
        elif compressor_name == "blosc":
            compressor = numcodecs.Blosc(cname='zstd', clevel=3, shuffle=numcodecs.Blosc.SHUFFLE)
        elif compressor_name == "lz4":
            compressor = numcodecs.LZ4(acceleration=1)
        elif compressor_name == "none":
            compressor = None
        else:
            compressor = numcodecs.Zstd(level=3)

        logger.info(f"[ZarrWriter] Building block-delayed write plan: {abs_path} | Compressor: {compressor_name}")

        # =========================================================
        # Block-delayed writer: 每个 block 一个显式写盘任务
        # 与 Stats 路径完全对等：都从 mask_dask.to_delayed() 分叉
        # =========================================================
        arr_to_save = dask_arr

        # 初始化 zarr store（一次性）
        _init_zarr_store(abs_path, arr_to_save.shape, arr_to_save.chunksize, str(arr_to_save.dtype), compressor)

        # 获取 per-block delayed 对象（与 CellposePostProcessor 完全相同的方式）
        delayed_blocks = arr_to_save.to_delayed().ravel().tolist()

        # 计算每个 block 的起始 offset（origin）
        origins_per_dim = _chunk_origins_from_chunks(arr_to_save.chunks)

        # 为每个 block 创建写盘任务
        write_tasks = []
        flat_index = 0
        for block_idx in np.ndindex(*arr_to_save.numblocks):
            # 计算该 block 在全局数组中的 region 起始位置
            origin = tuple(
                origins_per_dim[axis][block_idx[axis]]
                for axis in range(arr_to_save.ndim)
            )

            # 构建 region slices
            region_slices = tuple(
                slice(origin[axis], origin[axis] + arr_to_save.chunksize[axis])
                for axis in range(arr_to_save.ndim)
            )

            delayed_block = delayed_blocks[flat_index]
            flat_index += 1

            write_task = dask.delayed(_write_block_to_region)(
                delayed_block,
                abs_path,
                region_slices,
                node_id,
                execution_id,
            )
            write_tasks.append(write_task)

        logger.info(
            f"[ZarrWriter] Block-delayed: {len(write_tasks)} blocks, "
            f"chunks={arr_to_save.chunksize}, partitions={arr_to_save.npartitions}"
        )

        # =========================================================
        # 单一 finalize 收尾：显式依赖所有 write_tasks
        # 不再回到菱形依赖结构
        # =========================================================
        final_delayed = dask.delayed(_wait_all_and_finalize)(
            write_tasks,   # 闭包依赖：确保所有写盘完成后再写 metadata
            abs_path,
            arr_to_save.ndim,
            metadata,
        )

        return (final_delayed, {"sink_progress": {"kind": "queue", "total_chunks": len(write_tasks)}})

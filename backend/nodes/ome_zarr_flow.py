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
from utils.progress_helper import report_progress
from core.chunk_policy import ChunkPolicy, reader_init_chunks, writer_minimal_rechunk, RechunkReason

# 创建logger
logger = logging.getLogger("BrainFlow.OMEZarr")


# =============================================================================
#  节点：OME-Zarr 读取器 (Reader) - 改进版 v2
# =============================================================================
@register_node("OMEZarrReader")
class OMEZarrReader:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "📂 OME-Zarr Reader (Dask)"
    DESCRIPTION = "读取 OME-Zarr 格式的数据，支持智能/手动 Chunk 配置。自动识别 2D/3D/4D/5D 数据。"
    PROGRESS_TYPE = ProgressType.STATE_ONLY  # 仅状态，无百分比

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": "", "multiline": False, "placeholder": "C:/Data/brain.zarr"}),
            },
            "optional": {
                # Chunk 模式选择
                "chunk_mode": (["default_3d", "auto", "manual"], {"default": "default_3d"}),

                # 默认3D模式：x/y/z 方向的分块大小
                "chunk_z": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Z Chunk Size"}),
                "chunk_y": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Y Chunk Size"}),
                "chunk_x": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "X Chunk Size"}),

                # 智能模式：目标 chunk 大小（MB）
                "target_chunk_mb": ("FLOAT", {"default": 256, "min": 64, "max": 1024, "label": "Target Chunk Size (MB)"}),

                # 手动模式：使用字符串配置
                "chunk_config": ("STRING", {
                    "default": "",
                    "multiline": False,
                    "label": "Chunk Config (e.g., '10,512,512')",
                    "placeholder": "Manual config"
                }),

                # 或者使用简化的单值配置
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
                  target_chunk_mb=256,
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

        # 3. 路径白名单校验
        def _get_allowed_data_dirs():
            """获取允许的数据目录白名单（优先环境变量，其次默认值）"""
            env_dirs = os.getenv("BRAINFLOW_DATA_DIRS", "")
            if env_dirs:
                # 环境变量：冒号分隔（Windows 用分号）
                sep = ";" if os.name == "nt" else ":"
                dirs = [d.strip() for d in env_dirs.split(sep) if d.strip()]
                if dirs:
                    return dirs
            # 默认白名单：常见数据目录
            # 注意：生产环境应通过环境变量配置，不要在这里添加敏感路径
            default_dirs = ["./data", os.path.expanduser("~/data")]
            # Windows: 添加 E:\Users 作为默认允许（仅开发环境）
            if os.name == "nt" and os.path.exists("E:\\Users"):
                default_dirs.append("E:\\Users")
            return default_dirs

        def _is_path_allowed(path: str, allowed_dirs: list) -> bool:
            """检查路径是否在允许的目录内（严格父子关系判断）"""
            for base in allowed_dirs:
                try:
                    # 白名单目录也要 resolve（处理 symlink）
                    base_real = os.path.realpath(base)
                    # 使用 commonpath 判断严格的父子关系
                    # commonpath 要求 path 必须是 base 的子路径
                    common = os.path.commonpath([path, base_real])
                    if common == base_real:
                        return True
                except (ValueError, OSError):
                    # 不同驱动器或路径不存在，跳过
                    continue
            return False

        allowed_dirs = _get_allowed_data_dirs()
        if not _is_path_allowed(resolved_path, allowed_dirs):
            # 不泄露具体白名单路径，只提示需要配置
            raise PermissionError(
                f"[Node {node_id}] Access denied: path outside allowed data directories. "
                f"Set BRAINFLOW_DATA_DIRS environment variable to configure allowed paths."
            )

        # 校验通过，使用规范化后的路径
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
            # 使用统一的 chunk 策略，避免多处 rechunk
            policy = ChunkPolicy()
            policy.source_chunks = dask_arr.chunksize

            if chunk_mode == "manual" and chunk_config:
                # 手动模式：使用 chunk_config 字符串
                dask_arr = reader_init_chunks(
                    dask_arr,
                    chunk_size=chunk_size,
                    keep_first_dim=keep_first_dim,
                    manual_config=chunk_config,
                    policy=policy
                )
            elif chunk_mode == "default_3d":
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
                # 智能模式：使用默认 chunk_size
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
#  工具函数：模块级别的进度报告钩子
# =============================================================================
def _write_progress_hook(block, node_id=None, execution_id=None):
    """
    Writer 节点的进度报告钩子。
    在每个 chunk 写出完成后触发进度更新。
    注意：这个函数必须在模块级别，以便 Dask 可以序列化。
    """
    if node_id:
        report_progress(node_id, execution_id=execution_id)
    return block


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


def _write_ome_metadata(write_result, abs_path, ndim, metadata):
    """
    写入 OME-NGFF 元数据到 root group。
    显式依赖 write_result，确保数据写完后才写 metadata。
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
    return write_result


def _finalize_write_result(write_result, meta_result, abs_path):
    """
    最终汇合节点：串联 data write 和 meta write。
    meta_result 参数虽然函数体中未直接使用，但它的存在让 Dask
    建立了对 _write_ome_metadata delayed 的隐式依赖，
    确保 metadata 写完后才返回最终结果。
    """
    logger.info(f"[ZarrWriter] Write complete: {abs_path}")
    return abs_path


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

        logger.info(f"[ZarrWriter] Building lazy write plan: {abs_path} | Compressor: {compressor_name}")

        # ===== Chunk 策略（pure lazy，不触发计算）=====
        policy = ChunkPolicy()
        policy.working_chunks = dask_arr.chunksize
        arr_to_save = writer_minimal_rechunk(dask_arr, policy=policy)
        policy.output_chunks = arr_to_save.chunksize
        logger.info(f"[ZarrWriter] Planned chunks: {policy.output_chunks}, partitions: {arr_to_save.npartitions}")

        # ===== 注入进度钩子（map_blocks 埋点，lazy）=====
        arr_with_progress = arr_to_save.map_blocks(
            _write_progress_hook,
            dtype=arr_to_save.dtype,
            node_id=node_id,
            execution_id=execution_id
        )

        # ===== 构建 lazy 写盘 graph（compute=False）=====
        # to_zarr(compute=False) 返回 dask.delayed，不触发任何计算
        lazy_write = arr_with_progress.to_zarr(
            abs_path,
            component="0",
            compressor=compressor,
            overwrite=True,
            compute=False
        )

        # ===== 串联 OME metadata 写入（显式依赖写盘完成） =====
        ndim = arr_to_save.ndim
        lazy_meta = dask.delayed(_write_ome_metadata)(lazy_write, abs_path, ndim, metadata)

        # 把 data write 和 meta write 串联成一个最终 delayed
        final_delayed = dask.delayed(_finalize_write_result)(lazy_write, lazy_meta, abs_path)

        logger.info(f"[ZarrWriter] Lazy write plan ready, {arr_to_save.npartitions} chunks queued")
        return (final_delayed, {"sink_progress": {"kind": "queue", "total_chunks": arr_to_save.npartitions}})

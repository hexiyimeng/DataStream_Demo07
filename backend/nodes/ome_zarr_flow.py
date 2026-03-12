import os
import asyncio
import logging
import numcodecs
import numpy as np
import zarr
import dask.array as da
from core.registry import register_node, ProgressType
from services.dask_service import dask_service
from services.chunk_optimizer import ChunkOptimizer, optimize_dask_array
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

        # 2. 标准化路径
        try:
            file_path = os.path.realpath(file_path)
        except (OSError, ValueError) as e:
            raise ValueError(f"[Node {node_id}] Invalid path: {e}")

        # 3. 路径遍历保护 - 放宽限制以支持更多使用场景
        # 允许的目录基础路径
        allowed_bases = [
            os.getcwd(),
            os.path.expanduser("~/data"),
            os.path.expanduser("~"),  # 允许用户主目录
            os.path.dirname(file_path),  # 允许文件所在目录
        ]
        is_allowed = False
        for base in allowed_bases:
            try:
                base_real = os.path.realpath(base)
                if os.path.commonpath([file_path, base_real]) == base_real:
                    is_allowed = True
                    break
            except (ValueError, OSError):
                continue

        # 如果不在允许列表中，检查是否是绝对路径（作为额外的安全检查）
        if not is_allowed and os.path.isabs(file_path):
            # 对于绝对路径，只做基本的路径遍历检查
            if ".." in file_path.split(os.sep):
                # 检查是否试图向上遍历
                resolved = os.path.realpath(file_path)
                # 确保解析后的路径没有可疑的父目录引用
                if "/../" not in resolved.replace("\\", "/"):
                    is_allowed = True

        if not is_allowed:
            raise PermissionError(f"[Node {node_id}] Path must be within allowed directories. Current path: {file_path}")

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
            # 1. 打开 Group (只读模式)
            store = zarr.open_group(file_path, mode='r')

            # 2. 解析 OME-NGFF Multiscales 元数据
            multiscales = store.attrs.get("multiscales", [])
            dataset_path = "0"

            if multiscales:
                datasets = multiscales[0].get("datasets", [])
                if datasets:
                    dataset_path = datasets[0]["path"]

            # 3. 加载 Array
            # 首先尝试从 multiscales 获取正确的数据集路径
            dataset_path = "0"  # 默认路径
            if multiscales:
                datasets = multiscales[0].get("datasets", [])
                if datasets:
                    dataset_path = datasets[0]["path"]

            # 尝试从 store 获取数组
            z_arr = None
            if dataset_path in store:
                try:
                    z_arr = store[dataset_path]
                    logger.info(f"[ZarrReader] Loaded array from dataset path: {dataset_path}")
                except Exception as e:
                    logger.debug(f"Failed to load from dataset path '{dataset_path}': {e}")

            # 如果失败，尝试直接打开 zarr 文件
            if z_arr is None:
                try:
                    z_arr = zarr.open_array(file_path, mode='r')
                    logger.info(f"[ZarrReader] Loaded array directly from file path")
                except Exception as e:
                    logger.debug(f"Direct zarr array open failed: {e}, trying group keys")

            # 如果还是失败，尝试从 group keys 获取
            if z_arr is None:
                keys = list(store.keys())
                logger.info(f"[ZarrReader] Available keys in group: {keys}")

                if keys:
                    for key in keys:
                        try:
                            z_arr = store[key]
                            logger.info(f"[ZarrReader] Loaded array from group key: {key}")
                            break
                        except Exception as e:
                            logger.debug(f"Failed to load from key '{key}': {e}")
                            continue

            # 验证是否成功获取到数组
            if z_arr is None:
                raise ValueError(f"No valid array found in Zarr file. Available keys: {list(store.keys()) if hasattr(store, 'keys') else 'N/A'}")

            # 确保我们得到的是一个 Array，不是 Group
            if hasattr(z_arr, 'store') and hasattr(z_arr, 'shape'):
                # 这是一个有效的 Array
                pass
            elif hasattr(z_arr, 'group_keys'):
                # 这是一个 Group，需要获取第一个 array
                keys = list(z_arr.keys())
                logger.warning(f"[ZarrReader] Got a Group instead of Array, keys: {keys}")
                if keys:
                    for key in keys:
                        try:
                            z_arr = z_arr[key]
                            logger.info(f"[ZarrReader] Loaded array from group key: {key}")
                            if hasattr(z_arr, 'shape'):
                                break
                        except Exception as e:
                            logger.debug(f"Failed to load from key '{key}': {e}")
                            continue
                    if not hasattr(z_arr, 'shape'):
                        raise ValueError(f"No valid array found in Group. Keys: {keys}")

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

    @staticmethod
    def _apply_default_3d_config(dask_arr, array_shape, chunk_z, chunk_y, chunk_x, keep_first_dim, node_id):
        """应用默认3D chunk 配置：使用指定的z/y/x分块大小"""
        ndim = dask_arr.ndim
        new_chunks = []

        for i, dim_size in enumerate(array_shape):
            if i == 0 and keep_first_dim and ndim > 3:
                # 第一维度保持完整（通道/时间）
                new_chunks.append(dim_size)
            else:
                # 对于3D数据 (Z, Y, X) 或 (C, Z, Y, X)
                # 映射到chunk_z, chunk_y, chunk_x
                if ndim == 3:
                    # (Z, Y, X)
                    if i == 0:
                        new_chunks.append(min(chunk_z, dim_size))
                    elif i == 1:
                        new_chunks.append(min(chunk_y, dim_size))
                    else:
                        new_chunks.append(min(chunk_x, dim_size))
                elif ndim == 4:
                    # (C, Z, Y, X)
                    if i == 1:
                        new_chunks.append(min(chunk_z, dim_size))
                    elif i == 2:
                        new_chunks.append(min(chunk_y, dim_size))
                    elif i == 3:
                        new_chunks.append(min(chunk_x, dim_size))
                    else:
                        new_chunks.append(dim_size)
                elif ndim == 5:
                    # (T, C, Z, Y, X)
                    if i == 2:
                        new_chunks.append(min(chunk_z, dim_size))
                    elif i == 3:
                        new_chunks.append(min(chunk_y, dim_size))
                    elif i == 4:
                        new_chunks.append(min(chunk_x, dim_size))
                    else:
                        new_chunks.append(dim_size)
                elif ndim == 2:
                    # (Y, X)
                    if i == 0:
                        new_chunks.append(min(chunk_y, dim_size))
                    else:
                        new_chunks.append(min(chunk_x, dim_size))
                else:
                    # 其他情况使用默认值
                    new_chunks.append(min(64, dim_size))

        dask_arr = dask_arr.rechunk(tuple(new_chunks))
        logger.info(f"[ZarrReader] Default 3D chunk: {dask_arr.chunksize}")
        return dask_arr

    @staticmethod
    def _apply_manual_chunk_config(dask_arr, array_shape, chunk_config, chunk_size, keep_first_dim, node_id):
        """应用手动 chunk 配置"""
        ndim = dask_arr.ndim

        # 方式 1: 解析字符串配置 (如 "10,512,512" 或 "auto,512,512")
        if chunk_config and chunk_config.strip():
            try:
                parts = [p.strip().lower() for p in chunk_config.split(",")]

                # 检查是否是 "auto" 关键字
                has_auto = any(p == "auto" or p == "-1" for p in parts)

                if has_auto:
                    # 混合模式：部分自动，部分手动
                    new_chunks = []
                    for i, part in enumerate(parts):
                        if part == "auto" or part == "-1":
                            # 该维度自动计算（使用启发式规则）
                            dim_size = array_shape[i]
                            if i == 0 and keep_first_dim:
                                # 第一维度保持完整
                                new_chunks.append(dim_size)
                            elif i >= ndim - 2:
                                # 最后两个维度（通常是 Y, X）使用 chunk_size
                                new_chunks.append(min(chunk_size, dim_size))
                            else:
                                # 其他维度也使用 chunk_size
                                new_chunks.append(min(chunk_size, dim_size))
                        else:
                            # 手动指定
                            new_chunks.append(int(part))

                    # 填充剩余维度
                    while len(new_chunks) < ndim:
                        new_chunks.append(chunk_size)

                    dask_arr = dask_arr.rechunk(tuple(new_chunks[:ndim]))
                    logger.info(f"[ZarrReader] Manual chunk (mixed): {dask_arr.chunksize}")

                else:
                    # 纯手动配置
                    manual_chunks = [int(p) for p in parts]

                    # 填充或截断到正确维度
                    if len(manual_chunks) < ndim:
                        manual_chunks.extend([chunk_size] * (ndim - len(manual_chunks)))
                    elif len(manual_chunks) > ndim:
                        manual_chunks = manual_chunks[:ndim]

                    # 确保 chunk 不超过数组大小
                    final_chunks = [min(mc, arr_size) for mc, arr_size in zip(manual_chunks, array_shape)]

                    dask_arr = dask_arr.rechunk(tuple(final_chunks))
                    logger.info(f"[ZarrReader] Manual chunk: {dask_arr.chunksize}")

            except (ValueError, IndexError) as e:
                logger.warning(f"[ZarrReader] Failed to parse chunk_config '{chunk_config}': {e}")
                logger.info(f"[ZarrReader] Falling back to chunk_size mode")
                # 降级到方式 2

        else:
            # 方式 2: 使用单一 chunk_size 值
            new_chunks = []

            for i, dim_size in enumerate(array_shape):
                if i == 0 and keep_first_dim:
                    # 第一维度保持完整（通道/时间）
                    new_chunks.append(dim_size)
                elif i >= ndim - 2:
                    # 最后两个维度（空间维度 Y, X）使用指定值
                    new_chunks.append(min(chunk_size, dim_size))
                else:
                    # 中间维度也使用指定值
                    new_chunks.append(min(chunk_size, dim_size))

            dask_arr = dask_arr.rechunk(tuple(new_chunks))
            logger.info(f"[ZarrReader] Manual chunk (size-based): {dask_arr.chunksize}")

        return dask_arr

    @staticmethod
    def _apply_auto_chunk_config(dask_arr, target_chunk_mb, node_id):
        """应用智能 chunk 配置"""
        gpu_memory_gb = 0
        try:
            import torch
            if torch.cuda.is_available():
                gpu_memory_gb = torch.cuda.mem_get_info()[0] / 1024**3
        except Exception as e:
            logger.debug(f"Failed to detect GPU memory: {e}")

        # 使用指定的目标 chunk 大小
        original_ideal = ChunkOptimizer.IDEAL_CHUNK_SIZE
        ChunkOptimizer.IDEAL_CHUNK_SIZE = int(target_chunk_mb * 1024 * 1024)

        try:
            original_chunks = dask_arr.chunksize
            dask_arr = optimize_dask_array(dask_arr, gpu_memory_gb=gpu_memory_gb)
            new_chunks = dask_arr.chunksize

            if original_chunks != new_chunks:
                logger.info(f"[ZarrReader] Auto chunk: {original_chunks} -> {new_chunks}")

                # Log optimization details
                opt_config = ChunkOptimizer.auto_configure(
                    dask_arr.shape, dask_arr.dtype, gpu_memory_gb
                )
                logger.info(f"[ZarrReader] Estimated time: "
                          f"{opt_config['time_estimate']['estimated_seconds']:.1f}s, "
                          f"{opt_config['time_estimate']['n_chunks']} chunks")

        finally:
            ChunkOptimizer.IDEAL_CHUNK_SIZE = original_ideal

        return dask_arr


# =============================================================================
#  工具函数：模块级别的进度报告钩子
# =============================================================================
def _write_progress_hook(block, node_id=None):
    """
    Writer 节点的进度报告钩子。
    在每个 chunk 写出完成后触发进度更新。
    注意：这个函数必须在模块级别，以便 Dask 可以序列化。
    """
    if node_id:
        report_progress(node_id)
    return block


# =============================================================================
#  节点：OME-Zarr 写入器 (Writer)
# =============================================================================
@register_node("OMEZarrWriter")
class OMEZarrWriter:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "💾 OME-Zarr Writer (Save)"
    DESCRIPTION = "触发整个计算流，将结果保存为 OME-Zarr 格式。"
    OUTPUT_NODE = True
    PROGRESS_TYPE = ProgressType.STATE_ONLY  # 状态型进度（无真实 chunk 回报）

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

    RETURN_TYPES = ()
    FUNCTION = "save_zarr"

    async def save_zarr(self, dask_arr, output_path, compressor_name="default", metadata=None,
                        callback=None, global_progress_callback=None, **kwargs):
        import asyncio

        node_id = kwargs.get('_node_id', 'unknown_writer')

        # 路径处理
        if not output_path: output_path = "output.zarr"
        if not output_path.lower().endswith(".zarr"): output_path += ".zarr"
        abs_path = os.path.abspath(output_path)

        # 压缩器配置
        compressor = None
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

        logger.info(f"[ZarrWriter] Saving to: {abs_path} | Compressor: {compressor_name}")

        def run_save(arr_to_save):
            # ========== 统一 Chunk 策略 ==========
            # 使用 ChunkPolicy 进行最小化 rechunk，只在必要时修正
            policy = ChunkPolicy()
            policy.working_chunks = arr_to_save.chunksize

            logger.info(f"[ZarrWriter] Input chunks: {arr_to_save.chunksize}")
            logger.info(f"[ZarrWriter] Data shape: {arr_to_save.shape}")
            logger.info(f"[ZarrWriter] Chunks uniform: {policy.is_uniform(arr_to_save)}")

            # 只在必要时进行 minimal rechunk
            arr_to_save = writer_minimal_rechunk(arr_to_save, policy=policy)

            policy.output_chunks = arr_to_save.chunksize
            logger.info(f"[ZarrWriter] Final chunks: {policy.output_chunks}")
            logger.info(f"[ZarrWriter] Rechunk history: {policy.rechunk_reasons}")

            total_chunks = arr_to_save.npartitions
            logger.info(f"[ZarrWriter] Total chunks to save: {total_chunks}")

            if callback: callback(-1, 100, f"Saving {total_chunks} chunks to {abs_path}...")

            # ========== 标准 OME-NGFF 输出结构 ==========
            # 按 group/dataset 结构写入：
            # root group
            #   └── dataset "0"  (数据)
            # └── multiscales metadata

            # 步骤 1: 写入数据到 "0" dataset
            # 使用 component 参数指定数据集路径
            arr_to_save.to_zarr(
                abs_path,
                component="0",  # ← 关键：写入 "0" dataset
                compressor=compressor,
                overwrite=True,
                compute=True
            )

            logger.info(f"[ZarrWriter] Data written to dataset '0'")

            # 步骤 2: 补写 OME-NGFF 元数据到 root group
            try:
                z = zarr.open(abs_path, mode='r+')
                ndim = arr_to_save.ndim

                # 默认轴名称（遵循 OME-NGFF 规范）
                if ndim == 2:
                    axes = [{"name": "y"}, {"name": "x"}]
                elif ndim == 3:
                    axes = [{"name": "z"}, {"name": "y"}, {"name": "x"}]
                elif ndim == 4:
                    axes = [{"name": "c"}, {"name": "z"}, {"name": "y"}, {"name": "x"}]
                else:
                    axes = [{"name": f"dim_{i}"} for i in range(ndim)]

                voxel_size = [1.0] * ndim

                # 从输入 metadata 获取 axes 和 voxel_size
                if metadata:
                    if metadata.get("axes"):
                        axes = metadata["axes"]
                    if metadata.get("voxel_size") and len(metadata["voxel_size"]) == ndim:
                        voxel_size = metadata["voxel_size"]

                # 构建 multiscales 元数据
                datasets_meta = [{
                    "path": "0",  # ← 确保与实际写入路径一致
                    "coordinateTransformations": [{"type": "scale", "scale": voxel_size}]
                }]

                z.attrs["multiscales"] = [{
                    "version": "0.4",
                    "name": "processed",
                    "datasets": datasets_meta,
                    "axes": axes,
                    "type": "gaussian"  # 可选：表示数据类型
                }]

                logger.info(f"[ZarrWriter] OME-NGFF metadata written to root group")
                logger.info(f"[ZarrWriter] Dataset path: '0', Axes: {[a['name'] for a in axes]}, Scale: {voxel_size}")

            except Exception as e:
                logger.warning(f"[ZarrWriter] Failed to write OME metadata: {e}")

            if callback: callback(-1, 100, "Save Completed!")

        await asyncio.get_running_loop().run_in_executor(None, lambda: run_save(dask_arr))
        return ()

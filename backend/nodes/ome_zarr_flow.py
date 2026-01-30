# nodes/ome_zarr_flow.py
import os
import asyncio
import numcodecs
import numpy as np
import zarr
import dask.array as da
import sys
from registry import register_node

# ==========================================
# [修复] 稳健的导入逻辑
# ==========================================
# 1. 动态将父目录 (backend) 加入 Python 搜索路径
#    这样无论在哪里启动，都能找到 dask_monitor.py
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 2. 尝试导入监控工具
try:
    from dask_monitor import monitor_dask_progress
except ImportError:
    monitor_dask_progress = None
    print("Warning: 'dask_monitor.py' not found. Progress bars disabled.")

try:
    import dask.distributed

    HAS_LIBS = True
except ImportError:
    HAS_LIBS = False


# ==========================================
#              节点定义
# ==========================================

@register_node("OMEZarrReader")
class OMEZarrReader:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = " OME-Zarr Reader (Dask)"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": "", "multiline": False}),
                "chunk_multiple": ("INT", {"default": 1, "min": 1, "max": 16, "step": 1, "label": "Chunk Multiplier"}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY", "DICT")
    RETURN_NAMES = ("dask_arr", "metadata")
    FUNCTION = "load_zarr"

    def load_zarr(self, file_path, chunk_multiple=1, callback=None):
        if not file_path:
            raise ValueError("❌ 文件路径不能为空")

        file_path = os.path.abspath(file_path.strip())

        if callback:
            callback(0, 100, f"Reading: {file_path}")

        if not HAS_LIBS or not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ 找不到文件: {file_path}")

        try:
            store = zarr.open(file_path, mode='r')
            array_path = None

            if isinstance(store, zarr.Group):
                attrs = store.attrs.asdict()
                if 'multiscales' in attrs:
                    array_path = attrs['multiscales'][0]['datasets'][0]['path']
                else:
                    arrays = list(store.array_keys())
                    if arrays:
                        array_path = arrays[0]
                    elif '0' in store:
                        array_path = '0'

            if array_path:
                z_arr = store[array_path]
            else:
                z_arr = store

            native_chunks = z_arr.chunks

            if callback:
                callback(10, 100, f"Native Chunks: {native_chunks}")

            if chunk_multiple < 1: chunk_multiple = 1
            new_chunks = tuple(c * chunk_multiple for c in native_chunks)

            if callback:
                callback(20, 100, f"Dask Chunks: {new_chunks} ({chunk_multiple}x)")

            if array_path:
                dask_arr = da.from_zarr(file_path, component=array_path, chunks=new_chunks)
            else:
                dask_arr = da.from_zarr(file_path, chunks=new_chunks)

            dask_arr = dask_arr.astype(np.float32)

            return (dask_arr, {
                "source_path": file_path,
                "shape": dask_arr.shape,
                "dtype": str(dask_arr.dtype),
                "chunks": new_chunks
            })

        except Exception as e:
            if callback: callback(0, 0, f"Error: {e}")
            raise e


@register_node("OMEZarrFilter")
class OMEZarrFilter:
    CATEGORY = "BrainFlow/Process"
    DISPLAY_NAME = " Image Filter (Dask)"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "algorithm": (["gaussian", "median", "sobel", "invert"],),
                "sigma": ("FLOAT", {"default": 1.0, "min": 0.1, "max": 20.0}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("processed_dask",)
    FUNCTION = "apply_filter"

    def apply_filter(self, dask_arr, algorithm, sigma):
        def process_chunk(chunk, algo=None, s=1.0):
            import scipy.ndimage
            if algo == "gaussian": return scipy.ndimage.gaussian_filter(chunk, sigma=s)
            if algo == "invert": return 255 - chunk
            if algo == "sobel": return scipy.ndimage.sobel(chunk)
            return chunk

        depth = int(sigma * 3) + 1
        res = dask_arr.map_overlap(
            process_chunk,
            depth=depth,
            boundary='reflect',
            dtype=dask_arr.dtype,
            algo=algorithm, s=sigma
        )
        return (res,)


@register_node("OMEZarrWriter")
class OMEZarrWriter:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = " OME-Zarr Writer (Dask)"
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY",),
                "metadata": ("DICT",),
                "compression": (["default", "zstd"],),
            },
            "optional": {
                "output_path": ("STRING", {"default": "", "multiline": False, "placeholder": "默认自动保存"})
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("saved_path",)
    FUNCTION = "save_zarr"

    async def save_zarr(self, dask_arr, metadata, compression, output_path="", callback=None,
                        global_progress_callback=None):
        if not HAS_LIBS: raise ImportError("缺少必要库: dask, zarr, numcodecs")
        if metadata is None: metadata = {}

        # 1. 自动路径逻辑
        if not output_path or output_path.strip() == "":
            source = metadata.get("source_path", "")
            if source and "mock://" not in source:
                lower_source = source.lower()
                # [还原] 原始的健壮逻辑
                if ".zarr" in lower_source:
                    zarr_end_idx = lower_source.rfind(".zarr") + 5
                    zarr_root = source[:zarr_end_idx]
                    parent_dir = os.path.dirname(zarr_root)
                    base_name = os.path.basename(zarr_root)
                    name_only = os.path.splitext(base_name)[0]
                else:
                    source_clean = source.rstrip("/\\")
                    parent_dir = os.path.dirname(source_clean)
                    base_name = os.path.basename(source_clean)
                    name_only = os.path.splitext(base_name)[0]

                output_path = os.path.join(parent_dir, f"{name_only}_processed.zarr")
            else:
                output_path = "output_processed.zarr"

        abs_path = os.path.abspath(output_path)
        if callback: callback(0, 100, f"Target: {abs_path}")

        main_loop = asyncio.get_running_loop()
        total_chunks = dask_arr.npartitions

        # 2. 核心执行逻辑
        def run_task():
            import shutil

            # [稳健获取 Client]
            # 这里的 import 位于函数内部，且 sys.path 已经在文件头部修正，所以一定能找到
            try:
                from dask_monitor import monitor_dask_progress
            except ImportError:
                monitor_dask_progress = None

            try:
                from dask_manager import get_client
                client = get_client()
            except ImportError:
                from dask.distributed import Client
                try:
                    client = Client.current()
                except:
                    client = None

            # 清理旧文件
            if os.path.exists(abs_path):
                try:
                    shutil.rmtree(abs_path)
                except:
                    pass

            # [还原] 压缩参数设置
            if compression == "zstd":
                compressor = numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.BITSHUFFLE)
            else:
                compressor = numcodecs.Blosc(cname='lz4', clevel=5, shuffle=numcodecs.Blosc.SHUFFLE)

            # --- 分支 A: Dask 集群计算 (带进度监控) ---
            if client and monitor_dask_progress:
                if callback: callback(-1, 100, f"Submitting {total_chunks} chunks...")

                # 构建图
                delayed_task = dask_arr.to_zarr(
                    abs_path,
                    compressor=compressor,
                    overwrite=True,
                    compute=False
                )

                # 提交
                future = client.compute(delayed_task)

                # [核心] 阻塞并广播进度
                monitor_dask_progress(client, future, callback, global_progress_callback)

                if callback: callback(100, 100, "Dask Task Completed!")

            # --- 分支 B: 本地回退 (保持原样) ---
            else:
                if callback: callback(-1, 100, "Local fallback (slow/no-progress)...")
                dask_arr.to_zarr(abs_path, compressor=compressor, overwrite=True)
                if callback: callback(100, 100, "Local Task Completed!")

            # [还原] 补写元数据
            try:
                z = zarr.open(abs_path, mode='r+')
                z.attrs["multiscales"] = [{
                    "version": "0.4", "name": "processed", "datasets": [{"path": "0"}],
                    "axes": metadata.get("axes",
                                         [{"name": "t"}, {"name": "c"}, {"name": "z"}, {"name": "y"}, {"name": "x"}])
                }]
            except:
                pass

        await main_loop.run_in_executor(None, run_task)
        return (abs_path,)
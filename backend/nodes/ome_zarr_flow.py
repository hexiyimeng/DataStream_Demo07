# nodes/ome_zarr_flow.py
import os
import shutil  # 用于强制删除旧文件夹
import asyncio
import numpy as np
import warnings
from registry import register_node

# Dask 核心依赖
try:
    import zarr
    import dask.array as da
    from dask.diagnostics import Callback
    import scipy.ndimage
    import numcodecs

    HAS_LIBS = True
except ImportError:
    HAS_LIBS = False
    print("⚠️ 缺少必要库: pip install dask[complete] zarr scipy")


# ==========================================
#      Dask 进度条桥接器 (静音版)
# ==========================================
class DaskProgressBridge(Callback):
    def __init__(self, async_callback, loop=None):
        self.async_callback = async_callback
        self.loop = loop or asyncio.get_running_loop()
        self.total_tasks = 0
        self.finished_tasks = 0

    def _start_state(self, dsk, state):
        self.total_tasks = len(state['ready']) + len(state['waiting']) + len(state['running'])
        self._send(0, "🚀 开始计算...")

    def _posttask(self, key, result, dsk, state, worker_id):
        self.finished_tasks += 1
        if self.total_tasks > 0:
            progress = int((self.finished_tasks / self.total_tasks) * 100)
            if self.finished_tasks % 5 == 0 or progress == 100:
                self._send(progress, "")

    def _finish(self, dsk, state, errored):
        self._send(100, "✅ 完成")

    def _send(self, progress, msg):
        if self.async_callback:
            asyncio.run_coroutine_threadsafe(
                self.async_callback(progress, 100, msg), self.loop
            )


# ==========================================
#              ComfyUI 节点定义
# ==========================================

@register_node("OMEZarrReader")
class OMEZarrReader:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "📂 OME-Zarr Reader (Dask)"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": "", "multiline": False}),
                # 保留用户要求的功能：控制块大小倍数
                "chunk_multiple": ("INT", {"default": 1, "min": 1, "max": 16, "step": 1, "label": "Chunk Multiplier"}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY", "DICT")
    RETURN_NAMES = ("dask_arr", "metadata")
    FUNCTION = "load_zarr"

    def load_zarr(self, file_path, chunk_multiple=1):
        # 1. 强制转绝对路径
        if not file_path:
            raise ValueError("❌ 文件路径不能为空")

        file_path = os.path.abspath(file_path.strip())

        print(f" [Reader] 读取: {file_path}")
        if not HAS_LIBS or not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ 找不到文件: {file_path}")

        try:
            store = zarr.open(file_path, mode='r')
            array_path = None

            # 智能探测
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
            print(f"   > 📊 原始物理块大小: {native_chunks}")

            if chunk_multiple < 1: chunk_multiple = 1
            new_chunks = tuple(c * chunk_multiple for c in native_chunks)
            print(f"   > 🚀 设定 Dask 调度块: {new_chunks} (倍数: {chunk_multiple}x)")

            if array_path:
                dask_arr = da.from_zarr(file_path, component=array_path, chunks=new_chunks)
            else:
                dask_arr = da.from_zarr(file_path, chunks=new_chunks)

            dask_arr = dask_arr.astype(np.float32)

            return (dask_arr, {
                "source_path": file_path,  # 传递绝对路径
                "shape": dask_arr.shape,
                "dtype": str(dask_arr.dtype),
                "chunks": new_chunks
            })

        except Exception as e:
            print(f"❌ 读取失败: {e}")
            raise e


@register_node("OMEZarrFilter")
class OMEZarrFilter:
    CATEGORY = "BrainFlow/Process"
    DISPLAY_NAME = "⚡ Image Filter (Dask)"

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
            process_chunk, depth=depth, boundary='reflect',
            dtype=dask_arr.dtype, algo=algorithm, s=sigma
        )
        return (res,)


@register_node("OMEZarrWriter")
class OMEZarrWriter:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "💾 OME-Zarr Writer (Dask)"
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

    async def save_zarr(self, dask_arr, metadata, compression, output_path="", callback=None):
        if metadata is None: metadata = {}

        # 1. 自动路径逻辑
        if not output_path or output_path.strip() == "":
            source = metadata.get("source_path", "")

            if source and "mock://" not in source:
                # 🔥🔥🔥【智能路径修正】
                # 目标：找到输入文件所属的“根目录”，并存到它的旁边

                # 场景 1: 输入是 .../MyImage.zarr/image (OME-Zarr)
                lower_source = source.lower()
                if ".zarr" in lower_source:
                    # 截取到 .zarr 结尾
                    # 例子: E:\Data\File.zarr\image -> E:\Data\File.zarr
                    zarr_end_idx = lower_source.rfind(".zarr") + 5
                    zarr_root = source[:zarr_end_idx]

                    # 获取父级: E:\Data
                    parent_dir = os.path.dirname(zarr_root)

                    # 获取名字: File
                    base_name = os.path.basename(zarr_root)
                    name_only = os.path.splitext(base_name)[0]

                # 场景 2: 输入是普通文件夹
                else:
                    source_clean = source.rstrip("/\\")
                    parent_dir = os.path.dirname(source_clean)
                    base_name = os.path.basename(source_clean)
                    name_only = os.path.splitext(base_name)[0]

                # 结果: E:\Data\File_processed.zarr
                output_path = os.path.join(parent_dir, f"{name_only}_processed.zarr")
                print(f"[Writer] 自动定位原始目录: {output_path}")

            else:
                # 兜底：如果完全没有 source 信息，才用代码目录
                output_path = "output_processed.zarr"

        abs_path = os.path.abspath(output_path)
        print(f"[Writer] 写入路径: {abs_path}")

        main_loop = asyncio.get_running_loop()

        def run_dask():
            import numcodecs
            import shutil

            # 写入前强制清理 (保留用户功能)
            if os.path.exists(abs_path):
                try:
                    shutil.rmtree(abs_path)
                except Exception as e:
                    print(f"[Writer] ⚠️ 无法清理旧文件: {e}")

            with DaskProgressBridge(callback, loop=main_loop):
                compressor = None
                if compression == "zstd":
                    compressor = numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.BITSHUFFLE)
                else:
                    compressor = numcodecs.Blosc(cname='lz4', clevel=5, shuffle=numcodecs.Blosc.SHUFFLE)

                dask_arr.to_zarr(abs_path, compressor=compressor, overwrite=True)

                try:
                    z = zarr.open(abs_path, mode='r+')
                    z.attrs["multiscales"] = [{
                        "version": "0.4", "name": "processed", "datasets": [{"path": "0"}],
                        "axes": metadata.get("axes", [{"name": "t"}, {"name": "c"}, {"name": "z"}, {"name": "y"},
                                                      {"name": "x"}])
                    }]
                except:
                    pass

        await main_loop.run_in_executor(None, run_dask)
        return (abs_path,)
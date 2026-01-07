# nodes/ome_zarr_flow.py
import os
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
#      Dask 进度条桥接器 (核心组件)
# ==========================================

class DaskProgressBridge(Callback):
    """
    监听 Dask 任务进度，并通过 asyncio 桥接到 ComfyUI 前端
    """

    # 🔥 修复点 1：增加 loop 参数，允许从外部传入主线程的事件循环
    def __init__(self, async_callback, loop=None):
        self.async_callback = async_callback
        # 如果传入了 loop 就用传入的，否则尝试获取（在子线程会失败，所以必须传入）
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
            # 只有当进度变化明显时才发送，避免 WebSocket 拥堵
            if self.finished_tasks % 5 == 0 or progress == 100:
                self._send(progress, f"Computing... {progress}%")

    def _finish(self, dsk, state, errored):
        self._send(100, "✅ 完成")

    def _send(self, progress, msg):
        if self.async_callback:
            # 使用保存的 loop 线程安全地发送消息
            asyncio.run_coroutine_threadsafe(
                self.async_callback(progress, 100, msg), self.loop
            )


# ==========================================
#              ComfyUI 节点定义
# ==========================================

# --- 节点 1: Reader ---
@register_node("OMEZarrReader")
class OMEZarrReader:
    CATEGORY = "BrainFlow/IO"
    DISPLAY_NAME = "📂 OME-Zarr Reader (Dask)"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"file_path": ("STRING", {"default": "", "multiline": False})}}

    RETURN_TYPES = ("DASK_ARRAY", "DICT")
    RETURN_NAMES = ("dask_arr", "metadata")
    FUNCTION = "load_zarr"

    def load_zarr(self, file_path):
        print(f" [Reader] 读取: {file_path}")
        if not HAS_LIBS or not os.path.exists(file_path):
            return self._get_mock()

        try:
            # 1. 智能探测路径
            store = zarr.open(file_path, mode='r')
            array_path = None

            if isinstance(store, zarr.Group):
                print("   > 识别为 Group，正在寻找 Multiscales 元数据...")
                attrs = store.attrs.asdict()
                if 'multiscales' in attrs and len(attrs['multiscales']) > 0:
                    datasets = attrs['multiscales'][0]['datasets']
                    found_path = datasets[0]['path']
                    print(f"   > 🎯 命中 OME 元数据，路径: '{found_path}'")
                    array_path = found_path
                else:
                    print("   > ⚠️ 无 Multiscales，尝试暴力搜索数组...")
                    arrays = list(store.array_keys())
                    if arrays:
                        array_path = arrays[0]
                    elif '0' in store:
                        array_path = '0'

            # 2. Dask 读取
            if array_path:
                dask_arr = da.from_zarr(file_path, component=array_path)
            else:
                dask_arr = da.from_zarr(file_path)

            # 3. 数据类型修复 (防止 Big-Endian 死锁)
            if dask_arr.dtype.byteorder == '>':
                print(f"   > ⚠️ 检测到 Big-Endian ({dask_arr.dtype})，正在转码...")
            dask_arr = dask_arr.astype(np.float32)

            print(f"   > ✅ Dask 加载成功: Shape={dask_arr.shape}, Chunks={dask_arr.chunksize}")

            metadata = {
                "source_path": os.path.abspath(file_path),
                "shape": dask_arr.shape,
                "dtype": str(dask_arr.dtype)
            }
            return (dask_arr, metadata)

        except Exception as e:
            print(f"❌ 读取失败: {e}")
            import traceback
            traceback.print_exc()
            raise e

    def _get_mock(self):
        arr = da.random.randint(0, 255, size=(10, 512, 512), chunks=(1, 256, 256)).astype(np.float32)
        return (arr, {"source_path": "mock"})


# --- 节点 2: Filter ---
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


# --- 节点 3: Writer (修复 Loop 传递) ---
# nodes/ome_zarr_flow.py (只替换 Writer 部分)

# nodes/ome_zarr_flow.py 中的 Writer 部分

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

                # 🔥🔥🔥 【关键修复】挪到 required 里，否则前端看不见！
                # 虽然在 required 里，但给了默认值 ""，所以逻辑上依然是选填的
                "output_path": ("STRING", {"default": "", "multiline": False, "placeholder": "留空=自动保存在上一级"}),
            },
            # optional 暂时留空，防止前端渲染不出
            "optional": {}
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("saved_path",)
    FUNCTION = "save_zarr"

    async def save_zarr(self, dask_arr, metadata, compression, output_path="", callback=None):
        # 1. 智能路径计算逻辑
        # 注意：这里要做个 strip() 判断，防止用户输入了空格
        if not output_path or not output_path.strip():
            # 获取源文件路径
            source = metadata.get("source_path", "")

            if source and "mock://" not in source:
                base_name = os.path.basename(source.rstrip("/\\"))
                name_only = os.path.splitext(base_name)[0]

                # 获取上一级目录
                current_dir = os.path.dirname(source.rstrip("/\\"))
                parent_dir = os.path.dirname(current_dir)

                # 拼接新路径
                output_path = os.path.join(parent_dir, f"{name_only}_processed.zarr")
                print(f"[Writer] 自动定位上一级目录: {output_path}")
            else:
                output_path = "output_processed.zarr"

        abs_path = os.path.abspath(output_path)
        print(f"[Writer] 最终写入路径: {abs_path}")

        main_loop = asyncio.get_running_loop()

        def run_dask():
            # 确保 DaskProgressBridge 类可用
            with DaskProgressBridge(callback, loop=main_loop):
                compressor = None
                if compression == "zstd":
                    compressor = numcodecs.Zstd(level=3)

                dask_arr.to_zarr(
                    abs_path,
                    compressor=compressor,
                    overwrite=True
                )

                # 写元数据
                try:
                    z = zarr.open(abs_path, mode='r+')
                    multiscales = [{
                        "version": "0.4",
                        "name": "processed",
                        "datasets": [{"path": "0"}],
                        "axes": metadata.get("axes", [
                            {"name": "t", "type": "time"},
                            {"name": "c", "type": "channel"},
                            {"name": "z", "type": "space"},
                            {"name": "y", "type": "space"},
                            {"name": "x", "type": "space"}
                        ])
                    }]
                    z.attrs["multiscales"] = multiscales
                except Exception:
                    pass

        await main_loop.run_in_executor(None, run_dask)
        return (abs_path,)
# nodes/ome_zarr_flow.py
import os
import shutil
import asyncio
import numpy as np
import warnings
from registry import register_node

# ==========================================
#      Dask 依赖与显式集群启动
# ==========================================
try:
    import zarr
    import dask.array as da
    import scipy.ndimage
    import numcodecs
    from dask.distributed import Client, LocalCluster

    HAS_LIBS = True

    # 这里不需要再启动集群，全部交给 main.py 和 dask_manager 统一管理

except ImportError:
    HAS_LIBS = False


# ==========================================
#              ComfyUI 节点定义
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

    # [新增] 接收 callback 参数用于发送日志
    def load_zarr(self, file_path, chunk_multiple=1, callback=None):
        if not file_path:
            raise ValueError("❌ 文件路径不能为空")

        file_path = os.path.abspath(file_path.strip())

        # [修改] 使用 callback 发送日志到前端
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

            # [修改] 发送切块日志
            if callback:
                callback(10, 100, f"Native Chunks: {native_chunks}")

            if chunk_multiple < 1: chunk_multiple = 1
            new_chunks = tuple(c * chunk_multiple for c in native_chunks)

            # [修改] 发送调度日志
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

    async def save_zarr(self, dask_arr, metadata, compression, output_path="", callback=None):
        if not HAS_LIBS: raise ImportError("缺少必要库")
        if metadata is None: metadata = {}

        # 1. 自动路径逻辑
        if not output_path or output_path.strip() == "":
            source = metadata.get("source_path", "")
            if source and "mock://" not in source:
                lower_source = source.lower()
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

        # 计算块数
        total_chunks = dask_arr.npartitions

        # 定义阻塞函数
        def run_dask_with_monitor():
            import time
            import shutil

            # [稳健获取 Client]
            try:
                # 尝试从 dask_manager 获取
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

            # 准备压缩
            if compression == "zstd":
                compressor = numcodecs.Blosc(cname='zstd', clevel=5, shuffle=numcodecs.Blosc.BITSHUFFLE)
            else:
                compressor = numcodecs.Blosc(cname='lz4', clevel=5, shuffle=numcodecs.Blosc.SHUFFLE)

            if client:
                if callback: callback(-1, 100, f"Submitting {total_chunks} chunks to Dask...")

                # 仅生成图
                delayed_task = dask_arr.to_zarr(
                    abs_path,
                    compressor=compressor,
                    overwrite=True,
                    compute=False
                )

                # 提交计算
                future = client.compute(delayed_task)

                # 轮询逻辑
                while not future.done():
                    try:
                        info = client.scheduler_info()
                        workers = len(info['workers'])
                        processing = sum(len(w['processing']) for w in info['workers'].values())

                        # 这个消息会被前端 FlowContext.tsx 的 isSpam 过滤，不会进 Log
                        # 但是会更新节点上的灵动岛 UI
                        msg = f"Dask Running | Workers: {workers} | Active Chunks: {processing}"
                    except Exception as e:
                        msg = f"Dask Running..."

                    if callback:
                        callback(-1, 100, msg)

                    time.sleep(0.5)

                if future.status == 'error':
                    future.result()

                if callback: callback(100, 100, "Dask Task Completed!")

            else:
                if callback: callback(-1, 100, "Local fallback (slow)...")
                dask_arr.to_zarr(abs_path, compressor=compressor, overwrite=True)

            # 补写元数据
            try:
                z = zarr.open(abs_path, mode='r+')
                z.attrs["multiscales"] = [{
                    "version": "0.4", "name": "processed", "datasets": [{"path": "0"}],
                    "axes": metadata.get("axes",
                                         [{"name": "t"}, {"name": "c"}, {"name": "z"}, {"name": "y"}, {"name": "x"}])
                }]
            except:
                pass

        await main_loop.run_in_executor(None, run_dask_with_monitor)
        return (abs_path,)
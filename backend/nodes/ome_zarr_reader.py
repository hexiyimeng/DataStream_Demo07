import os
import logging
import numpy as np
import dask.array as da
from core.registry import register_node

logger = logging.getLogger("WorkFlow.OMEZarrReader")


@register_node("OMEZarrReader")
class OMEZarrReader:
    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "OME-Zarr Reader"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "file_path": ("STRING", {"default": ""}),
            },
            "optional": {
                "chunk_z": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Z Chunk Size"}),
                "chunk_y": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "Y Chunk Size"}),
                "chunk_x": ("INT", {"default": 64, "min": 1, "max": 1024, "label": "X Chunk Size"}),
                "keep_first_dim": ("BOOLEAN", {"default": False, "label": "Keep First Dimension Intact"}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY", "DICT")
    RETURN_NAMES = ("dask_arr", "metadata")
    FUNCTION = "load_zarr"

    def load_zarr(self, file_path,
                  chunk_z=64, chunk_y=64, chunk_x=64,
                  keep_first_dim=False,
                  callback=None, **kwargs):
        node_id = kwargs.get('_node_id', 'unknown')

        if not file_path:
            raise ValueError(f"[Node {node_id}] File path cannot be empty")

        file_path = os.path.realpath(file_path.strip().strip('"').strip("'"))

        if not os.path.exists(file_path) or not os.path.isdir(file_path):
            raise FileNotFoundError(f"[Node {node_id}] Not a valid zarr directory: {file_path}")

        import zarr
        multiscales = []
        dataset_path = None
        axes = None
        voxel_size = None
        try:
            z_arr = zarr.open_array(file_path, mode='r')
            array_path = file_path
            logger.info(f"[ZarrReader] Loaded direct array: {file_path}")
        except Exception:
            store = zarr.open_group(file_path, mode='r')
            multiscales = store.attrs.get("multiscales", [])
            dataset_path = "0"
            if multiscales:
                datasets = multiscales[0].get("datasets", [])
                if datasets:
                    dataset_path = datasets[0].get("path", "0")
            array_path = os.path.join(file_path, dataset_path)
            z_arr = store[dataset_path]
            logger.info(f"[ZarrReader] Loaded array from group, dataset={dataset_path}: {file_path}")

        shape = z_arr.shape
        ndim = z_arr.ndim
        total_gb = np.prod(shape) * z_arr.dtype.itemsize / (1024**3)
        logger.info(f"[ZarrReader] Loaded: shape={shape}, dtype={z_arr.dtype}, size={total_gb:.2f}GB")

        if ndim == 3:
            config_str = f"{chunk_z},{chunk_y},{chunk_x}"
        elif ndim == 4:
            config_str = f"-1,{chunk_z},{chunk_y},{chunk_x}"
        else:
            config_str = f"{chunk_z},{chunk_y},{chunk_x}"

        target_chunks = self._parse_chunk_config(
            config_str, shape, ndim, 512, keep_first_dim
        )
        logger.info(f"[ZarrReader] Target chunks: {target_chunks}")

        dask_arr = da.from_zarr(array_path, chunks=target_chunks)
        logger.info(f"[ZarrReader] Dask array: shape={dask_arr.shape}, chunks={dask_arr.chunksize}, npartitions={dask_arr.npartitions}")

        voxel_size = voxel_size or [1.0] * ndim
        axes = axes or self._get_dimension_names(ndim)
        if multiscales:
            datasets = multiscales[0].get("datasets", [])
            if datasets:
                for t in datasets[0].get("coordinateTransformations", []):
                    if t.get("type") == "scale":
                        voxel_size = t.get("scale", voxel_size)
                axes = multiscales[0].get("axes", axes)
                if isinstance(axes, list) and len(axes) == ndim:
                    axes = [a.get("name", a) if isinstance(a, dict) else str(a) for a in axes]

        metadata = {
            "source_path": file_path,
            "shape": dask_arr.shape,
            "dtype": str(dask_arr.dtype),
            "chunks": dask_arr.chunksize,
            "ndim": dask_arr.ndim,
            "npartitions": dask_arr.npartitions,
            "voxel_size": voxel_size,
            "axes": axes,
            "original_chunks": z_arr.chunks,
        }

        return (dask_arr, metadata)

    @staticmethod
    def _get_dimension_names(ndim):
        names = {2: ["Y", "X"], 3: ["Z", "Y", "X"], 4: ["C", "Z", "Y", "X"], 5: ["T", "C", "Z", "Y", "X"]}
        return names.get(ndim, [f"dim_{i}" for i in range(ndim)])

    @staticmethod
    def _parse_chunk_config(config_str, array_shape, ndim, chunk_size, keep_first_dim):
        if not config_str or not config_str.strip():
            return tuple(
                array_shape[i] if i == 0 and keep_first_dim else min(chunk_size, array_shape[i])
                for i in range(ndim)
            )

        parts = [p.strip().lower() for p in config_str.split(",")]
        new_chunks = []
        for i, part in enumerate(parts[:ndim]):
            if part in ("auto", "-1"):
                new_chunks.append(array_shape[i] if i == 0 and keep_first_dim else min(chunk_size, array_shape[i]))
            else:
                new_chunks.append(int(part))

        while len(new_chunks) < ndim:
            new_chunks.append(min(chunk_size, array_shape[len(new_chunks)]))

        return tuple(new_chunks)

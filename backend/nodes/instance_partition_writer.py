from __future__ import annotations

import os
import uuid

import numpy as np
import pandas as pd
import scipy.ndimage as ndimage

from core.registry import register_node
from nodes.base import BaseBlockMapNode


COLUMNS = [
    "chunk_id", "local_id",
    "centroid_z", "centroid_y", "centroid_x",
    "voxel_count", "touches_boundary", "boundary_overlap_chunks",
    "bbox_z0", "bbox_y0", "bbox_x0",
    "bbox_z1", "bbox_y1", "bbox_x1",
]


def _default_format():
    import importlib.util
    return "parquet" if importlib.util.find_spec("pyarrow") else "csv"


def _write_partition(df, path, file_format):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        if file_format == "parquet":
            df.to_parquet(tmp, index=False)
        else:
            df.to_csv(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _extract_instances(block, origin, chunk_id):
    """Extract per-instance rows from a 3D mask block."""
    if block.ndim != 3 or block.size == 0:
        return pd.DataFrame(columns=COLUMNS)

    ids = np.unique(block)
    ids = ids[ids > 0]
    if len(ids) == 0:
        return pd.DataFrame(columns=COLUMNS)

    slices = ndimage.find_objects(block)
    shape = block.shape
    cur = [int(x) for x in chunk_id.split("_")]
    rows = []

    for inst_id in ids:
        idx = int(inst_id)
        if idx - 1 >= len(slices) or slices[idx - 1] is None:
            continue
        sl = slices[idx - 1]
        mask = block[sl] == idx
        voxels = int(mask.sum())
        if voxels == 0:
            continue

        com = ndimage.center_of_mass(mask)
        cz = float(origin[0] + sl[0].start + com[0])
        cy = float(origin[1] + sl[1].start + com[1])
        cx = float(origin[2] + sl[2].start + com[2])

        touches = (sl[0].start == 0 or sl[0].stop == shape[0]
                  or sl[1].start == 0 or sl[1].stop == shape[1]
                  or sl[2].start == 0 or sl[2].stop == shape[2])

        neighbors = []
        if touches:
            for axis in range(3):
                if sl[axis].start == 0 and cur[axis] > 0:
                    n = list(cur)
                    n[axis] -= 1
                    neighbors.append("_".join(str(x) for x in n))
                if sl[axis].stop == shape[axis]:
                    n = list(cur)
                    n[axis] += 1
                    neighbors.append("_".join(str(x) for x in n))

        rows.append({
            "chunk_id": chunk_id,
            "local_id": idx,
            "centroid_z": cz,
            "centroid_y": cy,
            "centroid_x": cx,
            "voxel_count": voxels,
            "touches_boundary": bool(touches),
            "boundary_overlap_chunks": ",".join(neighbors),
            "bbox_z0": int(origin[0] + sl[0].start),
            "bbox_y0": int(origin[1] + sl[1].start),
            "bbox_x0": int(origin[2] + sl[2].start),
            "bbox_z1": int(origin[0] + sl[0].stop - 1),
            "bbox_y1": int(origin[1] + sl[1].stop - 1),
            "bbox_x1": int(origin[2] + sl[2].stop - 1),
        })

    return pd.DataFrame(rows, columns=COLUMNS) if rows else pd.DataFrame(columns=COLUMNS)


def _process_block(mask_block, output_dir, file_format,
                    write_empty_partitions, ctx):
    block_loc = tuple(int(i) for i in ctx.block_location)
    chunk_id = "_".join(str(i) for i in block_loc)
    origin = ctx.chunk_origin or (0,) * len(block_loc)

    df = _extract_instances(mask_block, origin, chunk_id)
    if len(df) > 0 or write_empty_partitions:
        ext = "parquet" if file_format == "parquet" else "csv"
        _write_partition(df, os.path.join(output_dir, f"part_{chunk_id}.{ext}"), file_format)

    return mask_block


@register_node("CellposeInstancePartitionWriter")
class CellposeInstancePartitionWriter(BaseBlockMapNode):
    CATEGORY = "WorkFlow/Instance"
    DISPLAY_NAME = "Cellpose Instance Partition Writer"
    RETURN_TYPES = ("DASK_ARRAY[same]",)
    RETURN_NAMES = ("mask_passthrough",)
    PROCESS_BLOCK = staticmethod(_process_block)

    @classmethod
    def INPUT_TYPES(cls):
        fmt = _default_format()
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
            },
            "optional": {
                "output_dir": ("STRING", {"default": ""}),
                "file_format": (["parquet", "csv"], {"default": fmt}),
                "write_empty_partitions": ("BOOLEAN", {"default": True}),
            },
        }

"""
Instance table extraction from Cellpose masks.

CellposePostProcessor:
  - Reads mask_dask (output from DaskCellpose)
  - Extracts per-chunk instance metadata WITHOUT re-running the model
  - Uses scipy.ndimage.find_objects to preserve original Cellpose instance IDs
  - Returns list[dask.delayed.Delayed] (one per chunk)
"""
import logging

import dask
import numpy as np
import pandas as pd
import scipy.ndimage as ndimage

from core.registry import register_node, ProgressType
from utils.chunk_helpers import _chunk_origins_from_chunks, _build_chunk_adjacency

logger = logging.getLogger("BrainFlow.InstanceNodes")


# =============================================================================
# Helper functions (shared with post_processing_nodes — see utils/chunk_helpers.py)
# =============================================================================


def _extract_chunk_instances(mask_block, origin, chunk_id, resolution_um=1.0):
    """
    Extract instance metadata from a single Cellpose mask block.

    Uses scipy.ndimage.find_objects to get instance slices WITHOUT re-labeling.
    Cellpose masks already have instance IDs (1, 2, 3...) - we preserve those.

    Parameters
    ----------
    mask_block : ndarray
        Cellpose mask (dtype=uint16, 0=background, N=instance ID)
    origin : tuple
        Global offset of this chunk (z, y, x)
    chunk_id : str
        Unique chunk identifier
    resolution_um : float
        Voxel size in microns

    Returns
    -------
    pd.DataFrame with columns:
        chunk_id, local_id, centroid_z/y/x, voxel_count,
        touches_boundary, boundary_overlap_chunks,
        bbox_z0, bbox_y0, bbox_x0, bbox_z1, bbox_y1, bbox_x1
    """
    if mask_block.size == 0:
        return pd.DataFrame(columns=[
            'chunk_id', 'local_id',
            'centroid_z', 'centroid_y', 'centroid_x',
            'voxel_count', 'touches_boundary', 'boundary_overlap_chunks',
            'bbox_z0', 'bbox_y0', 'bbox_x0', 'bbox_z1', 'bbox_y1', 'bbox_x1'
        ])

    labeled = mask_block
    unique_ids = np.unique(labeled)
    instance_ids = unique_ids[unique_ids > 0]  # exclude 0 (background)

    if len(instance_ids) == 0:
        return pd.DataFrame(columns=[
            'chunk_id', 'local_id',
            'centroid_z', 'centroid_y', 'centroid_x',
            'voxel_count', 'touches_boundary', 'boundary_overlap_chunks',
            'bbox_z0', 'bbox_y0', 'bbox_x0', 'bbox_z1', 'bbox_y1', 'bbox_x1'
        ])

    # find_objects returns (slice_z, slice_y, slice_x) for each instance
    # indexed by instance ID (1-based)
    slices = ndimage.find_objects(labeled)

    records = []
    shape = mask_block.shape

    for inst_id in instance_ids:
        inst_id = int(inst_id)
        sl = slices[inst_id - 1]  # find_objects is 1-indexed

        # Extract instance mask
        inst_mask = (labeled[sl] == inst_id)

        # Voxel count
        voxel_count = int(inst_mask.sum())

        # Centroid (in global coordinates)
        com = ndimage.center_of_mass(inst_mask)
        centroid_z = float(com[0]) + origin[0]
        centroid_y = float(com[1]) + origin[1]
        centroid_x = float(com[2]) + origin[2]

        # Boundary detection: does instance touch any chunk edge?
        # Check all 6 faces of the chunk boundary
        touches = False
        ndim = len(shape)

        if ndim == 3:
            if sl[0].start == 0 or sl[0].stop == shape[0]:
                touches = True
            if sl[1].start == 0 or sl[1].stop == shape[1]:
                touches = True
            if sl[2].start == 0 or sl[2].stop == shape[2]:
                touches = True
        elif ndim == 2:
            if sl[0].start == 0 or sl[0].stop == shape[0]:
                touches = True
            if sl[1].start == 0 or sl[1].stop == shape[1]:
                touches = True

        # Boundary overlap chunks - which neighboring chunks this instance touches
        # Format: list of chunk_ids like "1_0_2"
        overlap_chunks = []
        if touches:
            # Determine which faces are at boundary and infer neighbor chunk ids
            # Chunk id format: "i0_i1_i2" where each is the block index
            current_indices = [int(x) for x in chunk_id.split('_')]

            if ndim == 3:
                # z-
                if sl[0].start == 0 and current_indices[0] > 0:
                    neighbor = list(current_indices)
                    neighbor[0] -= 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # z+
                if sl[0].stop == shape[0]:
                    neighbor = list(current_indices)
                    neighbor[0] += 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # y-
                if sl[1].start == 0 and current_indices[1] > 0:
                    neighbor = list(current_indices)
                    neighbor[1] -= 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # y+
                if sl[1].stop == shape[1]:
                    neighbor = list(current_indices)
                    neighbor[1] += 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # x-
                if sl[2].start == 0 and current_indices[2] > 0:
                    neighbor = list(current_indices)
                    neighbor[2] -= 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # x+
                if sl[2].stop == shape[2]:
                    neighbor = list(current_indices)
                    neighbor[2] += 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
            elif ndim == 2:
                # y-
                if sl[0].start == 0 and current_indices[0] > 0:
                    neighbor = list(current_indices)
                    neighbor[0] -= 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # y+
                if sl[0].stop == shape[0]:
                    neighbor = list(current_indices)
                    neighbor[0] += 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # x-
                if sl[1].start == 0 and current_indices[1] > 0:
                    neighbor = list(current_indices)
                    neighbor[1] -= 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))
                # x+
                if sl[1].stop == shape[1]:
                    neighbor = list(current_indices)
                    neighbor[1] += 1
                    overlap_chunks.append('_'.join(str(x) for x in neighbor))

        # Bounding box (in global coordinates)
        bbox_z0 = int(sl[0].start) + origin[0]
        bbox_y0 = int(sl[1].start) + origin[1]
        bbox_x0 = int(sl[2].start) + origin[2]
        bbox_z1 = int(sl[0].stop - 1) + origin[0]  # inclusive
        bbox_y1 = int(sl[1].stop - 1) + origin[1]
        bbox_x1 = int(sl[2].stop - 1) + origin[2]

        records.append({
            'chunk_id': chunk_id,
            'local_id': inst_id,  # preserved from Cellpose output
            'centroid_z': centroid_z,
            'centroid_y': centroid_y,
            'centroid_x': centroid_x,
            'voxel_count': voxel_count,
            'touches_boundary': touches,
            'boundary_overlap_chunks': overlap_chunks,
            'bbox_z0': bbox_z0,
            'bbox_y0': bbox_y0,
            'bbox_x0': bbox_x0,
            'bbox_z1': bbox_z1,
            'bbox_y1': bbox_y1,
            'bbox_x1': bbox_x1,
        })

    return pd.DataFrame(records)


# =============================================================================
# CellposePostProcessor node
# =============================================================================

@register_node("CellposePostProcessor")
class CellposePostProcessor:
    """
    Extracts instance metadata from Cellpose mask output.

    Consumes mask_dask from DaskCellpose and produces per-chunk instance tables.
    Does NOT re-run Cellpose - only reads the existing mask.

    Output: dict with keys:
      - partitions: list[dask.delayed.Delayed] — one per-chunk DataFrame
      - numblocks: tuple — dask array numblocks
      - adjacency_edges: list[(chunk_a, chunk_b)] — adjacent chunk pairs
      - resolution_um: float

    Each partition DataFrame contains instance-level metadata:
      chunk_id, local_id, centroid_z/y/x, voxel_count,
      touches_boundary, boundary_overlap_chunks, bbox_*

    Use with DaskStats to produce final reconciled statistics.
    """
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "Instance Table Extractor"
    PROGRESS_TYPE = ProgressType.STATE_ONLY
    OUTPUT_NODE = False

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mask_dask": ("DASK_ARRAY",),
                "resolution_microns": ("FLOAT", {"default": 1.0, "min": 0.01, "max": 100.0}),
            }
        }

    RETURN_TYPES = ("INSTANCE_TABLE",)
    RETURN_NAMES = ("instance_partitions",)
    FUNCTION = "execute"

    def execute(self, mask_dask, resolution_microns=1.0, **kwargs):
        node_id = kwargs.get('_node_id')
        execution_id = kwargs.get('_execution_id')

        logger.info(f"[CellposePostProcessor] Input shape: {mask_dask.shape}, chunks: {mask_dask.chunks}")

        # Build chunk origins
        origins_per_dim = _chunk_origins_from_chunks(mask_dask.chunks)

        # Convert mask to delayed blocks
        delayed_blocks = mask_dask.to_delayed().ravel().tolist()

        # Build per-chunk instance table tasks
        partition_delayed_list = []
        flat_index = 0

        for block_idx in np.ndindex(*mask_dask.numblocks):
            origin = tuple(
                origins_per_dim[axis][block_idx[axis]]
                for axis in range(mask_dask.ndim)
            )
            chunk_id = '_'.join(str(i) for i in block_idx)

            delayed_block = delayed_blocks[flat_index]
            flat_index += 1

            # Create per-chunk task
            chunk_task = dask.delayed(_extract_chunk_instances)(
                delayed_block, origin, chunk_id, resolution_microns
            )
            partition_delayed_list.append(chunk_task)

        # Build adjacency edges from actual numblocks
        numblocks = mask_dask.numblocks
        adjacency_edges, _ = _build_chunk_adjacency(numblocks)

        logger.info(f"[CellposePostProcessor] Created {len(partition_delayed_list)} partition tasks, "
                    f"{len(adjacency_edges)} adjacent pairs for node {node_id}")

        # Return richer payload: dict instead of plain list
        payload = {
            'partitions': partition_delayed_list,   # list[dask.delayed.Delayed]
            'numblocks': numblocks,
            'adjacency_edges': adjacency_edges,       # list[(chunk_a, chunk_b)]
            'resolution_um': resolution_microns,
        }
        return (payload,)

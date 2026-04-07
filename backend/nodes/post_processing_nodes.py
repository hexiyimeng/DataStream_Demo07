import logging
import os

import dask
import numpy as np
import pandas as pd
from core.registry import register_node, ProgressType
from core.state_manager import state_manager
from utils.progress_helper import report_stage_progress
from utils.union_find import UnionFind

logger = logging.getLogger("BrainFlow.PostProcessing")

try:
    import dask_image.ndmeasure
    HAS_DASK_IMAGE = True
except ImportError:
    HAS_DASK_IMAGE = False


# =============================================================================
# DaskLabelStitcher (DEPRECATED)
# =============================================================================

@register_node("DaskLabelStitcher")
class DaskLabelStitcher:
    """
    DEPRECATED: This node is no longer supported.

    Use CellposePostProcessor + DaskStats instead for instance-aware processing.
    DaskLabelStitcher relied on dask_image.ndmeasure.label which destroys instance IDs.
    """
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "[DEPRECATED] Stitch & Re-Label"
    PROGRESS_TYPE = ProgressType.STATE_ONLY

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mask_dask": ("DASK_ARRAY",),
                "overlap": ("INT", {"default": 10}),
                "allow_fallback": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("stitched_mask",)
    FUNCTION = "execute"

    def execute(self, mask_dask, overlap, allow_fallback=False, **kwargs):
        raise NotImplementedError(
            "DaskLabelStitcher is deprecated. "
            "Use CellposePostProcessor + DaskStats for instance-aware processing."
        )


# =============================================================================
# Helper functions for new DaskStats (instance-table-based)
# =============================================================================

def _build_chunk_adjacency(numblocks):
    """
    Build chunk adjacency list from numblocks tuple.

    Returns
    -------
    adjacency_edges : list of (chunk_id_a, chunk_id_b) tuples
        All adjacent chunk pairs (each pair appears once)
    chunk_index_map : dict mapping chunk_id -> block_idx tuple
    """
    chunk_index_map = {}
    for block_idx in np.ndindex(numblocks):
        chunk_id = '_'.join(str(i) for i in block_idx)
        chunk_index_map[chunk_id] = block_idx

    adjacency_edges = set()
    for chunk_id, block_idx in chunk_index_map.items():
        for axis in range(len(block_idx)):
            # Positive direction neighbor
            if block_idx[axis] < numblocks[axis] - 1:
                neighbor_idx = list(block_idx)
                neighbor_idx[axis] += 1
                neighbor_id = '_'.join(str(i) for i in neighbor_idx)
                # Store canonical pair (smaller id first)
                pair = (chunk_id, neighbor_id) if chunk_id < neighbor_id else (neighbor_id, chunk_id)
                adjacency_edges.add(pair)

    return list(adjacency_edges), chunk_index_map


def _estimate_diameter(voxel_count):
    """Estimate equivalent spherical diameter from voxel count (assuming cubic voxels)."""
    if voxel_count <= 0:
        return 0.0
    # V = (4/3) * pi * r^3, d = 2r
    # d = 2 * (V * 3/4 / pi)^(1/3)
    return 2.0 * ((voxel_count * 3.0 / 4.0 / 3.14159) ** (1.0 / 3.0))


def _bbox_overlap_score(row_a, row_b):
    """
    Compute bbox overlap score between two instances.
    Returns IoU-like score in [0, 1]: 1 = perfect overlap, 0 = no overlap.
    """
    # z
    overlap_z = max(0, min(row_a['bbox_z1'], row_b['bbox_z1']) - max(row_a['bbox_z0'], row_b['bbox_z0']))
    union_z = max(row_a['bbox_z1'], row_b['bbox_z1']) - min(row_a['bbox_z0'], row_b['bbox_z0'])
    # y
    overlap_y = max(0, min(row_a['bbox_y1'], row_b['bbox_y1']) - max(row_a['bbox_y0'], row_b['bbox_y0']))
    union_y = max(row_a['bbox_y1'], row_b['bbox_y1']) - min(row_a['bbox_y0'], row_b['bbox_y0'])
    # x
    overlap_x = max(0, min(row_a['bbox_x1'], row_b['bbox_x1']) - max(row_a['bbox_x0'], row_b['bbox_x0']))
    union_x = max(row_a['bbox_x1'], row_b['bbox_x1']) - min(row_a['bbox_x0'], row_b['bbox_x0'])

    if union_z == 0 or union_y == 0 or union_x == 0:
        return 0.0

    overlap_vol = overlap_z * overlap_y * overlap_x
    union_vol = union_z * union_y * union_x

    return overlap_vol / union_vol if union_vol > 0 else 0.0


def _reconcile_chunk_pair(chunk_df_a, chunk_df_b):
    """
    First-version reconcile between two adjacent chunks.

    Uses BOTH centroid distance AND bbox overlap to catch split cells.
    A pair matches if EITHER criterion is strong enough:
      - centroid distance < 1.5 * max_diameter  OR
      - bbox overlap score > 0.1 (they occupy similar space in the overlap region)

    This is more robust than centroid distance alone for cross-chunk cells.
    """
    if chunk_df_a.empty or chunk_df_b.empty:
        return []

    edges = []

    # Precompute diameters for all instances
    diam_a = {row['local_id']: _estimate_diameter(row['voxel_count']) for _, row in chunk_df_a.iterrows()}
    diam_b = {row['local_id']: _estimate_diameter(row['voxel_count']) for _, row in chunk_df_b.iterrows()}

    for _, inst_a in chunk_df_a.iterrows():
        key_a = f"{inst_a['chunk_id']}:{inst_a['local_id']}"
        for _, inst_b in chunk_df_b.iterrows():
            key_b = f"{inst_b['chunk_id']}:{inst_b['local_id']}"

            max_d = max(diam_a[inst_a['local_id']], diam_b[inst_b['local_id']])

            # Criterion 1: centroid distance
            dist = np.sqrt(
                (inst_a['centroid_z'] - inst_b['centroid_z']) ** 2 +
                (inst_a['centroid_y'] - inst_b['centroid_y']) ** 2 +
                (inst_a['centroid_x'] - inst_b['centroid_x']) ** 2
            )
            centroid_ok = (max_d > 0 and dist < max_d * 1.5)

            # Criterion 2: bbox overlap (more robust for split cells)
            bbox_score = _bbox_overlap_score(inst_a, inst_b)
            bbox_ok = (bbox_score > 0.05)

            if centroid_ok or bbox_ok:
                score = max(
                    (1.0 - dist / max_d) if centroid_ok else 0.0,
                    bbox_score
                )
                edges.append({
                    'key_a': key_a,
                    'key_b': key_b,
                    'score': score,
                })

    return edges


def _run_instance_reconcile_and_stats(combined_df, resolution_um, node_id, output_dir, execution_id=None):
    """
    Core function: reconcile instances across chunks and compute final statistics.

    Phase 1: Already received (concatenated at unified graph level)
    Phase 2: Build adjacency and run pairwise reconcile
    Phase 3: Union-Find to assign global IDs
    Phase 4: Aggregate and write CSV

    Parameters
    ----------
    combined_df : pandas.DataFrame
        Already-concatenated instance table from all partitions.
        Produced by dask.delayed(pd.concat) at the unified graph level,
        NOT via dask.compute() inside this function.
    resolution_um : float
        Voxel size in microns
    node_id : str
    output_dir : str
        Directory to write the CSV file
    execution_id : str

    Returns
    -------
    str : path to output CSV file
    """
    if execution_id:
        report_stage_progress(node_id, 10, 100, "Collecting instance partitions...", execution_id=execution_id)

    # Phase 1: combined_df is already the concatenated DataFrame
    # (dask.compute inside this function has been eliminated)
    if combined_df.empty:
        if execution_id:
            report_stage_progress(node_id, 100, 100, "No instances found", execution_id=execution_id)
        return "No instances found"

    if execution_id:
        report_stage_progress(node_id, 30, 100, f"Reconciling {len(combined_df)} instances...", execution_id=execution_id)

    # Determine numblocks from chunk_ids
    # chunk_id format: "i0_i1_i2" where each is block index
    all_chunk_ids = combined_df['chunk_id'].unique()
    max_indices = [0, 0, 0]
    for cid in all_chunk_ids:
        parts = cid.split('_')
        for i, p in enumerate(parts):
            if i < 3:  # only first 3 dims
                max_indices[i] = max(max_indices[i], int(p) + 1)
    numblocks = tuple(max_indices)

    # Phase 2: Build adjacency and reconcile
    adjacency_edges, _ = _build_chunk_adjacency(numblocks)

    all_edges = []
    for chunk_a_id, chunk_b_id in adjacency_edges:
        df_a = combined_df[combined_df['chunk_id'] == chunk_a_id]
        df_b = combined_df[combined_df['chunk_id'] == chunk_b_id]

        if df_a.empty or df_b.empty:
            continue

        edges = _reconcile_chunk_pair(df_a, df_b)
        if edges:
            logger.info(f"[Stats] Chunk pair {chunk_a_id}/{chunk_b_id}: {len(edges)} merge candidates")
        all_edges.extend(edges)

    if execution_id:
        report_stage_progress(node_id, 50, 100, f"Found {len(all_edges)} candidate edges, running Union-Find...", execution_id=execution_id)

    # Phase 3: Union-Find
    uf = UnionFind()
    for edge in all_edges:
        if edge['score'] > 0.3:  # threshold for first version
            uf.union(edge['key_a'], edge['key_b'])

    components = uf.components()

    if execution_id:
        report_stage_progress(node_id, 70, 100, f"Found {len(components)} global instances...", execution_id=execution_id)

    # Assign global IDs
    # Step A: assign IDs to all merged components
    global_id_map = {}
    gid = 1
    for root_key in components:
        for member_key in components[root_key]:
            global_id_map[member_key] = gid
        gid += 1

    # Step B: assign unique IDs to singleton instances (never merged, not in any component)
    # Build all instance keys from combined_df
    all_keys = set(
        combined_df['chunk_id'] + ':' + combined_df['local_id'].astype(str)
    )
    for key in all_keys:
        if key not in global_id_map:
            global_id_map[key] = gid
            gid += 1

    logger.info(f"[Stats] Reconciled {len(all_keys)} local instances across {len(adjacency_edges)} chunk pairs → {len(components)} merged, {len(all_keys) - sum(len(m) for m in components.values())} singletons")

    # Vectorized global_id assignment (much faster than apply(axis=1))
    combined_df['instance_key'] = combined_df['chunk_id'] + ':' + combined_df['local_id'].astype(str)
    combined_df['global_id'] = combined_df['instance_key'].map(global_id_map).fillna(0).astype(int)

    # Phase 4: Aggregate
    if execution_id:
        report_stage_progress(node_id, 80, 100, "Aggregating statistics...", execution_id=execution_id)

    # Simple per-group aggregation: voxel_count sum + weighted centroid
    # global_id is the grouping key, other columns are aggregated
    stats_list = []
    for gid, g in combined_df.groupby('global_id'):
        total_voxels = g['voxel_count'].sum()
        if total_voxels == 0:
            continue
        stats_list.append({
            'id': gid,
            'volume_pixels': total_voxels,
            'volume_um3': float(total_voxels) * (resolution_um ** 3),
            'z': float((g['centroid_z'] * g['voxel_count']).sum()) / total_voxels,
            'y': float((g['centroid_y'] * g['voxel_count']).sum()) / total_voxels,
            'x': float((g['centroid_x'] * g['voxel_count']).sum()) / total_voxels,
            'source_chunks': str(set(g['chunk_id'])),
        })

    stats_df = pd.DataFrame(stats_list)

    # Reorder columns
    ordered_cols = ['id', 'volume_pixels', 'volume_um3', 'z', 'y', 'x', 'source_chunks']
    stats_df = stats_df[ordered_cols]

    if execution_id:
        report_stage_progress(node_id, 90, 100, "Writing CSV...", execution_id=execution_id)

    # Write CSV
    os.makedirs(output_dir, exist_ok=True)
    file_name = f"cell_stats_{node_id}.csv"
    full_path = os.path.abspath(os.path.join(output_dir, file_name))
    stats_df.to_csv(full_path, index=False)

    logger.info(f"[Stats] Saved to: {full_path} ({len(stats_df)} global instances)")
    if execution_id:
        report_stage_progress(node_id, 100, 100, "Done", execution_id=execution_id)

    return full_path


# =============================================================================
# DaskStats (REWRITTEN - instance-table-based)
# =============================================================================

@register_node("DaskStats")
class DaskStats:
    """
    Instance-table-based statistics node.

    DEPRECATED the old mask-based statistics path.
    CURRENT implementation:
      - Input: INSTANCE_TABLE (list of per-chunk instance DataFrames from CellposePostProcessor)
      - Does NOT read mask arrays
      - Does NOT fallback to mask.compute()
      - Runs first-version reconcile (centroid distance only)
      - Assigns global IDs via Union-Find
      - Aggregates statistics per global instance
      - Writes CSV

    TODO (v2): Upgrade reconcile to use overlap band pixel intersection
    TODO (v2): Add boundary_overlap_chunks for proper neighbor detection
    TODO (v2): Consider dask.dataframe pipeline for large-scale reconciliation
    """
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "Instance Statistics (Table-based)"
    OUTPUT_NODE = True
    PROGRESS_TYPE = ProgressType.STAGE_BASED

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "instance_partitions": ("INSTANCE_TABLE",),
                "resolution_microns": ("FLOAT", {"default": 1.0, "min": 0.01, "max": 100.0}),
                "output_dir": ("STRING", {"default": "./results", "tooltip": "Directory to write the CSV file"}),
            }
        }

    RETURN_TYPES = ("DELAYED",)
    RETURN_NAMES = ("stats_task",)
    FUNCTION = "execute"

    def execute(self, instance_partitions, resolution_microns=1.0, output_dir="./results", **kwargs):
        node_id = kwargs.get('_node_id')
        execution_id = kwargs.get('_execution_id')

        # instance_partitions is list[dask.delayed.Delayed]
        # The delayed objects will produce pandas.DataFrames when executed
        logger.info(f"[Stats] Received {len(instance_partitions)} partitions from CellposePostProcessor")

        # ============================================================
        # Key fix: Concatenate at the unified graph level.
        # Using dask.delayed(pd.concat) here (instead of dask.compute
        # inside _run_instance_reconcile_and_stats) ensures all instance
        # partition tasks are visible in the unified graph submitted to
        # client.compute(). This allows Dask to share the upstream
        # Cellpose computation with the Writer path.
        # ============================================================
        concat_delayed = dask.delayed(pd.concat)(instance_partitions, ignore_index=True)

        # Build the lazy execution graph — now receives a concrete DataFrame
        # (already concatenated via the outer graph), not a list to compute
        lazy_result = dask.delayed(_run_instance_reconcile_and_stats)(
            concat_delayed,
            resolution_microns,
            node_id,
            output_dir,
            execution_id,
        )

        logger.info(f"[Stats] Lazy stats plan ready for node {node_id}")
        return (lazy_result, {"sink_progress": {"kind": "stage_queue"}})

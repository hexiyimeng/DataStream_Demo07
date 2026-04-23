import logging
import os

import dask
import numpy as np
import pandas as pd
from core.registry import register_node, ProgressType
from utils.progress_helper import report_stage_progress
from utils.union_find import UnionFind
from utils.chunk_helpers import _build_chunk_adjacency

logger = logging.getLogger("BrainFlow.PostProcessing")


# =============================================================================
# DaskLabelStitcher (DEPRECATED)
#=============================================================================

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

def _estimate_diameter(voxel_count):
    """Estimate equivalent spherical diameter from voxel count (assuming cubic voxels)."""
    if voxel_count <= 0:
        return 0.0
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


def _reconcile_two_partitions(partition_a_delayed, partition_b_delayed, chunk_a_id, chunk_b_id):
    """
    Phase 1 — Distributed pairwise reconcile.

    Takes exactly two per-chunk delayed DataFrames (not the whole partitions list),
    filters to boundary-touching instances, runs O(n_a × n_b) comparison,
    returns edge list.

    Parameters
    ----------
    partition_a_delayed, partition_b_delayed : dask.delayed.Delayed
        The two per-chunk instance DataFrames for this adjacent pair.
    chunk_a_id, chunk_b_id : str

    Returns
    -------
    list[dict] — edge dicts with keys: key_a, key_b, score
    """
    partition_a = partition_a_delayed
    partition_b = partition_b_delayed

    if partition_a.empty or partition_b.empty:
        return []

    # Filter to boundary-touching instances only (reduces N significantly)
    a_boundary = partition_a[partition_a['touches_boundary'] == True]
    b_boundary = partition_b[partition_b['touches_boundary'] == True]

    if a_boundary.empty or b_boundary.empty:
        return []

    # Precompute diameters
    diam_a = {row['local_id']: _estimate_diameter(row['voxel_count']) for _, row in a_boundary.iterrows()}
    diam_b = {row['local_id']: _estimate_diameter(row['voxel_count']) for _, row in b_boundary.iterrows()}

    edges = []
    for _, inst_a in a_boundary.iterrows():
        key_a = f"{chunk_a_id}:{inst_a['local_id']}"
        for _, inst_b in b_boundary.iterrows():
            key_b = f"{chunk_b_id}:{inst_b['local_id']}"
            max_d = max(diam_a[inst_a['local_id']], diam_b[inst_b['local_id']])

            dist = np.sqrt(
                (inst_a['centroid_z'] - inst_b['centroid_z']) ** 2 +
                (inst_a['centroid_y'] - inst_b['centroid_y']) ** 2 +
                (inst_a['centroid_x'] - inst_b['centroid_x']) ** 2
            )
            centroid_ok = (max_d > 0 and dist < max_d * 1.5)

            bbox_score = _bbox_overlap_score(inst_a, inst_b)
            bbox_ok = (bbox_score > 0.05)

            if centroid_ok or bbox_ok:
                score = max(
                    (1.0 - dist / max_d) if centroid_ok else 0.0,
                    bbox_score
                )
                edges.append({'key_a': key_a, 'key_b': key_b, 'score': score})

    return edges


def _build_merge_map_from_edges(all_edges):
    """
    Phase 2 — Build small merge_map from edge list.

    Takes a flattened list of edge dicts, runs UnionFind on them,
    returns a dict mapping instance_key -> group_key (root key).

    This is the ONLY step that touches all edges.
    """
    if not all_edges:
        return {}

    uf = UnionFind()
    for edge in all_edges:
        if edge['score'] > 0.3:
            uf.union(edge['key_a'], edge['key_b'])

    merge_map = {}
    for root_key, members in uf.components().items():
        for member_key in members:
            merge_map[member_key] = root_key

    return merge_map


def _flatten_edge_lists(edge_lists_tuple):
    """
    Flatten a tuple of edge lists into a single flat list.
    Used as a lazy dask.delayed step between Phase 1 and Phase 2.
    """
    return [e for lst in edge_lists_tuple for e in lst]


def _partial_aggregate_partition(partition_df, chunk_id, merge_map, resolution_um):
    """
    Phase 3 — Distributed partial aggregation per partition.

    Each partition does a local groupby using group_key from merge_map.
    Outputs a small DataFrame (one row per group) with columns:
        group_key, voxel_count, z_num, y_num, x_num, source_chunks

    No full combined_df — each partition is independent.
    """
    if partition_df.empty:
        return pd.DataFrame(columns=['group_key', 'voxel_count', 'z_num', 'y_num', 'x_num', 'source_chunks'])

    partition_df = partition_df.copy()
    partition_df['instance_key'] = partition_df['chunk_id'] + ':' + partition_df['local_id'].astype(str)
    partition_df['group_key'] = partition_df['instance_key'].map(merge_map).fillna(partition_df['instance_key'])

    # Aggregate: sum voxels, weighted centroid numerators, collect source chunks
    agg = partition_df.groupby('group_key').agg(
        voxel_count=('voxel_count', 'sum'),
        z_num=('centroid_z', lambda col: (col * partition_df.loc[col.index, 'voxel_count']).sum()),
        y_num=('centroid_y', lambda col: (col * partition_df.loc[col.index, 'voxel_count']).sum()),
        x_num=('centroid_x', lambda col: (col * partition_df.loc[col.index, 'voxel_count']).sum()),
        source_chunks=('chunk_id', lambda col: ','.join(sorted(set(col))))
    ).reset_index()

    return agg


def _merge_two_partials(a, b):
    """
    Merge two partial aggregation DataFrames (tree-reduce leaf).
    Used by the tree-reduce in Phase 4.
    """
    if a.empty:
        return b
    if b.empty:
        return a
    return pd.concat([a, b], ignore_index=True)


def _tree_reduce(partial_tasks):
    """
    Build a balanced binary reduce tree over a list of dask.delayed.Delayed.

    Returns a single dask.delayed.Delayed that resolves to the fully
    reduced DataFrame after all pairwise merges complete.
    """
    if not partial_tasks:
        return dask.delayed(lambda: pd.DataFrame())()

    tasks = list(partial_tasks)
    while len(tasks) > 1:
        new_level = []
        for i in range(0, len(tasks), 2):
            if i + 1 < len(tasks):
                new_level.append(dask.delayed(_merge_two_partials)(tasks[i], tasks[i + 1]))
            else:
                new_level.append(tasks[i])
        tasks = new_level

    return tasks[0]


def _final_reduce(merged_partial_df, resolution_um, node_id, output_dir, execution_id):
    """
    Phase 4 — Final reduce on small partial tables.

    Concatenates all partial DataFrames, does a final groupby,
    computes volume + centroid, renumbers IDs to integers, writes CSV.
    """
    if execution_id:
        report_stage_progress(node_id, 80, 100, "Final reduction...", execution_id=execution_id)

    if merged_partial_df.empty:
        if execution_id:
            report_stage_progress(node_id, 100, 100, "No instances found", execution_id=execution_id)
        return "No instances found"

    # merged_partial_df is already the result of tree-reducing all partial tables.
    # Do the final groupby on this single (small) DataFrame.
    final_rows = []
    for group_key, g in merged_partial_df.groupby('group_key'):
        total_voxels = g['voxel_count'].sum()
        if total_voxels == 0:
            continue
        final_rows.append({
            'volume_pixels': total_voxels,
            'volume_um3': float(total_voxels) * (resolution_um ** 3),
            'z': float(g['z_num'].sum()) / total_voxels,
            'y': float(g['y_num'].sum()) / total_voxels,
            'x': float(g['x_num'].sum()) / total_voxels,
            'source_chunks': ','.join(sorted(set(g['source_chunks'].dropna()))),
        })

    final_df = pd.DataFrame(final_rows)
    final_df = final_df.sort_values('volume_pixels', ascending=False).reset_index(drop=True)
    final_df.insert(0, 'id', range(1, len(final_df) + 1))

    ordered_cols = ['id', 'volume_pixels', 'volume_um3', 'z', 'y', 'x', 'source_chunks']
    final_df = final_df[ordered_cols]

    if execution_id:
        report_stage_progress(node_id, 90, 100, "Writing CSV...", execution_id=execution_id)

    os.makedirs(output_dir, exist_ok=True)
    file_name = f"cell_stats_{node_id}.csv"
    full_path = os.path.abspath(os.path.join(output_dir, file_name))
    final_df.to_csv(full_path, index=False)

    logger.info(f"[Stats] Saved to: {full_path} ({len(final_df)} global instances)")
    if execution_id:
        report_stage_progress(node_id, 100, 100, "Done", execution_id=execution_id)

    return full_path


# =============================================================================
# DaskStats (REWRITTEN - instance-table-based)
# =============================================================================

@register_node("DaskStats")
class DaskStats:
    """
    4-phase distributed instance statistics pipeline.

    扩展瓶颈说明（重要）：
      - Phase 2 (_flatten_edge_lists → _build_merge_map_from_edges) 是全局收敛点。
        它的计算量取决于 boundary_edges 数量，而不是 chunk 总数。
        对于网格状邻接关系：boundary_edges ≈ O(N × D)，其中 N=chunk数，D=维度数。
        这意味着扩展瓶颈是"有多少跨 chunk 边界对"，而非"数据有多大"。
      - Phase 4 (_final_reduce) 同样为全局收敛点，但对小表操作，瓶颈通常是 CSV IO。
      - Phase 1 和 Phase 3 才是完全分布式的（每个邻接对 / 每个 partition 独立）。

    Phase 1: Distributed pairwise reconcile — each adjacent chunk pair = one delayed task.
             Filters to touches_boundary=True instances only. Returns small edge lists.
    Phase 2: [GLOBAL BARRIER] Collect all edges → UnionFind on full edge set → merge_map.
             ⚠️ 这是全局收敛点：必须等待所有 Phase 1 edge_tasks 完成，
               然后在单节点（或单 worker）上运行 UnionFind。
             ⚠️ flat_edges_delayed = dask.delayed(_flatten_edge_lists)(tuple(edge_tasks))
               会将所有 edge_tasks 的结果收集到内存，大图对象压力取决于 boundary_edges 数量。
    Phase 3: Distributed partial aggregation per partition — local groupby with group_key.
    Phase 4: [GLOBAL BARRIER] Tree-reduce partial tables → CSV with integer id.
             ⚠️ 这是全局收敛点：树状 reduce 在单节点合并所有 partial DataFrame。
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

        # instance_partitions is the payload dict from CellposePostProcessor
        partitions = instance_partitions['partitions']          # list[dask.delayed.Delayed]
        adjacency_edges = instance_partitions['adjacency_edges']  # list[(chunk_a, chunk_b)]
        resolution_um = instance_partitions.get('resolution_um', resolution_microns)

        logger.info(f"[Stats] 4-phase plan: {len(partitions)} partitions, {len(adjacency_edges)} chunk pairs")

        # ============================================================
        # Phase 1: Distributed pairwise reconcile
        # Each task depends ONLY on its two specific delayed partitions
        # (not the whole partitions list — no wide dependency).
        # partition_indices: chunk_id -> flat index in partitions list.
        # ============================================================
        if execution_id:
            report_stage_progress(node_id, 5, 100, f"Phase 1: {len(adjacency_edges)} reconcile tasks...", execution_id=execution_id)

        numblocks = instance_partitions['numblocks']
        partition_indices = {}
        for flat_idx, block_idx in enumerate(np.ndindex(numblocks)):
            partition_indices['_'.join(str(i) for i in block_idx)] = flat_idx

        edge_tasks = []
        for chunk_a_id, chunk_b_id in adjacency_edges:
            idx_a = partition_indices.get(chunk_a_id)
            idx_b = partition_indices.get(chunk_b_id)
            if idx_a is None or idx_b is None:
                continue
            # Dask only tracks dependency on these two specific delayed objects
            edge_task = dask.delayed(_reconcile_two_partitions)(
                partitions[idx_a], partitions[idx_b], chunk_a_id, chunk_b_id
            )
            edge_tasks.append(edge_task)

        # ============================================================
        # Phase 2: [GLOBAL BARRIER] Flatten all edge lists → UnionFind → merge_map
        #
        # ⚠️ CRITICAL SCALING CONSTRAINT:
        #   This step must collect ALL edge lists from Phase 1 before proceeding.
        #   The number of boundary edges ≈ O(N × D) for a grid adjacency:
        #     N = total chunk count, D = number of spatial dimensions
        #   This is NOT proportional to data size, but to chunk count × dimensions.
        #   For a 10×10×10 chunk grid (1000 chunks) in 3D: ~3000 boundary edges max.
        #
        #   Memory pressure: flat_edges_delayed materializes the full edge list.
        #   If each boundary pair produces ~10 edges (avg), that's ~30k edge dicts — manageable.
        #   But if boundary instances are many (dense segmentation), edge count grows.
        #
        #   Scaling rule: This phase becomes a bottleneck when boundary_edges ≫ 100k,
        #   not when chunk count or data size grows.
        #
        # Keep the graph fully lazy: wrap flatten in a delayed layer so
        # dask knows edge_tasks must resolve before merge_map can build.
        # When merge_map_delayed is eventually computed (at Phase 3 start),
        # dask automatically computes all edge_tasks in the graph first.
        # ============================================================
        if execution_id:
            report_stage_progress(node_id, 30, 100, "Phase 2: Building merge graph...", execution_id=execution_id)

        flat_edges_delayed = dask.delayed(_flatten_edge_lists)(tuple(edge_tasks))
        merge_map_delayed = dask.delayed(_build_merge_map_from_edges)(flat_edges_delayed)

        # ============================================================
        # Phase 3: Distributed partial aggregation per partition
        # Each task depends only on its one partition delayed + merge_map
        # (no dependency on any other partitions).
        # ============================================================
        if execution_id:
            report_stage_progress(node_id, 50, 100, f"Phase 3: {len(partitions)} partial aggregation tasks...", execution_id=execution_id)

        partial_tasks = []
        for flat_idx, block_idx in enumerate(np.ndindex(numblocks)):
            chunk_id = '_'.join(str(i) for i in block_idx)
            partial_task = dask.delayed(_partial_aggregate_partition)(
                partitions[flat_idx], chunk_id, merge_map_delayed, resolution_um
            )
            partial_tasks.append(partial_task)

        # ============================================================
        # Phase 4: [GLOBAL BARRIER] Tree-reduce partial tables → CSV
        #
        # Balanced binary merge tree: merges happen pairwise on workers first,
        # then progressively on fewer workers, finally on one node.
        # The final _final_reduce runs on a single node with the complete DataFrame.
        #
        # ⚠️ Bottleneck: Final CSV write is sequential; tree-reduce mitigates
        # merge bottleneck but the final concat + write is a sequential step.
        # ============================================================
        if execution_id:
            report_stage_progress(node_id, 80, 100, "Phase 4: Final reduce...", execution_id=execution_id)

        merged_partial = _tree_reduce(partial_tasks)
        final_delayed = dask.delayed(_final_reduce)(
            merged_partial, resolution_um, node_id, output_dir, execution_id
        )

        logger.info(f"[Stats] 4-phase plan ready: {len(edge_tasks)} reconcile + "
                    f"{len(partial_tasks)} partial agg + tree-reduce final")
        return (final_delayed, {"sink_progress": {"kind": "stage_queue"}})

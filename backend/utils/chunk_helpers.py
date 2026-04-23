"""
Shared chunk utility functions for BrainFlow.

These helpers operate on dask array chunk metadata (chunks tuple, numblocks tuple)
and are used by both instance_nodes and post_processing_nodes.
"""

import numpy as np


def _chunk_origins_from_chunks(chunks):
    """
    Calculate origin offsets for each chunk along each dimension.

    Parameters
    ----------
    chunks : tuple of tuples
        e.g. ((z1, z2, ...), (y1, y2, ...), (x1, x2, ...))

    Returns
    -------
    list of lists
        origins_per_dim[d][i] = global offset of chunk i along dimension d
    """
    origins_per_dim = []
    for dim_chunks in chunks:
        origins = []
        offset = 0
        for chunk_size in dim_chunks:
            origins.append(offset)
            offset += chunk_size
        origins_per_dim.append(origins)
    return origins_per_dim


def _build_chunk_adjacency(numblocks):
    """
    Build chunk adjacency list from numblocks tuple.

    Parameters
    ----------
    numblocks : tuple of int
        e.g. (nz, ny, nx) — number of blocks along each dimension

    Returns
    -------
    adjacency_edges : list of (chunk_id_a, chunk_id_b) tuples
        All adjacent chunk pairs (each pair appears once, canonical ordering)
    chunk_index_map : dict mapping chunk_id -> block_idx tuple
    """
    chunk_index_map = {}
    for block_idx in np.ndindex(numblocks):
        chunk_id = '_'.join(str(i) for i in block_idx)
        chunk_index_map[chunk_id] = block_idx

    adjacency_edges = set()
    for chunk_id, block_idx in chunk_index_map.items():
        for axis in range(len(block_idx)):
            if block_idx[axis] < numblocks[axis] - 1:
                neighbor_idx = list(block_idx)
                neighbor_idx[axis] += 1
                neighbor_id = '_'.join(str(i) for i in neighbor_idx)
                pair = (chunk_id, neighbor_id) if chunk_id < neighbor_id else (neighbor_id, chunk_id)
                adjacency_edges.add(pair)

    return list(adjacency_edges), chunk_index_map

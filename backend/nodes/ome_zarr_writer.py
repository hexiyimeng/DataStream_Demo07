from __future__ import annotations

import logging
import os
import shutil
import uuid

import numcodecs
import numpy as np
import zarr

from core.registry import register_node


logger = logging.getLogger("WorkFlow.OMEZarrWriter")


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


def _chunk_region_slices_from_chunks(chunks, block_idx):
    """
    Return exact region slices for one Dask block.

    This uses the concrete per-axis chunk lengths from ``arr.chunks``. Boundary
    chunks are often smaller than ``arr.chunksize`` and must not be expanded to
    the nominal chunk size when writing regions.
    """
    origins_per_dim = _chunk_origins_from_chunks(chunks)
    region = []
    for axis, chunk_axis_index in enumerate(block_idx):
        start = origins_per_dim[axis][chunk_axis_index]
        length = chunks[axis][chunk_axis_index]
        region.append(slice(start, start + length))
    return tuple(region)


def _validate_regular_chunks_for_region_writes(chunks):
    """
    Validate that chunks are regular except possibly for the final boundary chunk.

    OMEZarrWriter uses dask.array.map_blocks with explicit regions computed from
    chunk origins. Concurrent writes to the same zarr chunk from multiple tasks
    would corrupt data if internal chunks are irregular (vary in size).
    This guard rejects irregular internal chunks while allowing the final
    (smaller) boundary chunk along each axis.

    Raises ValueError if any internal chunk differs from the nominal size.
    """
    if not chunks:
        return
    for axis, axis_chunks in enumerate(chunks):
        if len(axis_chunks) <= 1:
            continue
        nominal = axis_chunks[0]
        for i, size in enumerate(axis_chunks):
            if size != nominal:
                if i == len(axis_chunks) - 1:
                    continue
                raise ValueError(
                    f"OMEZarrWriter requires regular chunks except possibly the final "
                    f"boundary chunk. Axis {axis} has irregular internal chunk {i}: "
                    f"expected {nominal}, got {size}. "
                    f"Please rechunk explicitly before writing."
                )


def _normalize_axes_for_ngff(axes, ndim):
    if not axes:
        if ndim == 2:
            return [{"name": "y"}, {"name": "x"}]
        if ndim == 3:
            return [{"name": "z"}, {"name": "y"}, {"name": "x"}]
        if ndim == 4:
            return [{"name": "c"}, {"name": "z"}, {"name": "y"}, {"name": "x"}]
        if ndim == 5:
            return [{"name": "t"}, {"name": "c"}, {"name": "z"}, {"name": "y"}, {"name": "x"}]
        return [{"name": f"dim_{i}"} for i in range(ndim)]

    normalized = []
    for ax in axes:
        if isinstance(ax, dict):
            normalized.append(ax)
        else:
            normalized.append({"name": str(ax).lower()})
    return normalized


def _prepare_compressor(compressor_name: str):
    if compressor_name == "zstd":
        return numcodecs.Zstd(level=3)
    if compressor_name == "blosc":
        return numcodecs.Blosc(cname="zstd", clevel=3, shuffle=numcodecs.Blosc.SHUFFLE)
    if compressor_name == "lz4":
        return numcodecs.LZ4(acceleration=1)
    if compressor_name == "none":
        return None
    return numcodecs.Zstd(level=3)


def _normalize_output_path(output_path: str) -> str:
    path = str(output_path or "output.zarr").strip().strip('"').strip("'")
    if "\x00" in path:
        raise ValueError("Output path contains a null byte.")
    if not path.lower().endswith(".zarr"):
        path += ".zarr"
    return os.path.abspath(path)


def _make_temp_output_path(final_path: str) -> str:
    parent = os.path.dirname(final_path) or os.getcwd()
    final_name = os.path.basename(final_path.rstrip(os.sep))
    os.makedirs(parent, exist_ok=True)

    for _ in range(20):
        candidate = os.path.join(parent, f".{final_name}.tmp-{uuid.uuid4().hex}.zarr")
        if not os.path.exists(candidate):
            return candidate
    raise RuntimeError(f"Failed to allocate a unique temp Zarr path near {final_path}.")


def _make_backup_path(final_path: str) -> str:
    parent = os.path.dirname(final_path) or os.getcwd()
    final_name = os.path.basename(final_path.rstrip(os.sep))
    for _ in range(20):
        candidate = os.path.join(parent, f".{final_name}.backup-{uuid.uuid4().hex}.zarr")
        if not os.path.exists(candidate):
            return candidate
    raise RuntimeError(f"Failed to allocate a backup path near {final_path}.")


def _remove_path_best_effort(path: str | None) -> bool:
    if not path or not os.path.exists(path):
        return True
    try:
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        return True
    except Exception as exc:
        logger.warning("[ZarrWriter] Failed to remove path %s: %s", path, exc)
        return False


def _init_zarr_store(abs_path, shape, chunks, dtype, compressor):
    if os.path.exists(abs_path):
        shutil.rmtree(abs_path)
    group = zarr.open_group(abs_path, mode="w")
    group.create_dataset(
        "0",
        shape=tuple(int(x) for x in shape),
        chunks=tuple(int(x) for x in chunks),
        dtype=np.dtype(dtype),
        compressor=compressor,
        overwrite=True,
    )
    logger.info(
        "[ZarrWriter] Store initialized: %s, shape=%s, chunks=%s, dtype=%s",
        abs_path,
        tuple(shape),
        tuple(chunks),
        np.dtype(dtype),
    )


def _finalize_store(abs_path, ndim, metadata):
    group = zarr.open(abs_path, mode="r+")
    axes = _normalize_axes_for_ngff(None, ndim)
    voxel_size = [1.0] * ndim
    if metadata:
        if metadata.get("axes"):
            axes = _normalize_axes_for_ngff(metadata["axes"], ndim)
        if metadata.get("voxel_size") and len(metadata["voxel_size"]) == ndim:
            voxel_size = metadata["voxel_size"]

    group.attrs["multiscales"] = [{
        "version": "0.4",
        "name": "processed",
        "datasets": [{
            "path": "0",
            "coordinateTransformations": [{"type": "scale", "scale": voxel_size}],
        }],
        "axes": axes,
        "type": "gaussian",
    }]
    logger.info(
        "[ZarrWriter] OME-NGFF metadata written: %s axes=%s scale=%s",
        abs_path,
        [a.get("name") for a in axes],
        voxel_size,
    )
    return abs_path


def _replace_final_with_temp(temp_path: str, final_path: str, overwrite: bool = True) -> str:
    if not os.path.exists(temp_path):
        raise FileNotFoundError(f"Temporary Zarr output does not exist: {temp_path}")

    os.makedirs(os.path.dirname(final_path) or os.getcwd(), exist_ok=True)
    backup_path = None

    if os.path.exists(final_path):
        if not overwrite:
            raise FileExistsError(f"Output path already exists: {final_path}")

        backup_path = _make_backup_path(final_path)
        try:
            os.rename(final_path, backup_path)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to move existing output to backup before replacement: "
                f"{final_path} -> {backup_path}: {exc}"
            ) from exc

    try:
        os.rename(temp_path, final_path)
    except Exception as exc:
        restore_error = None
        if backup_path and os.path.exists(backup_path) and not os.path.exists(final_path):
            try:
                os.rename(backup_path, final_path)
            except Exception as restore_exc:
                restore_error = restore_exc

        if restore_error is not None:
            raise RuntimeError(
                f"Failed to replace output with temp store and failed to restore backup. "
                f"Final path: {final_path}; temp path: {temp_path}; backup path: {backup_path}; "
                f"replace error: {exc}; restore error: {restore_error}"
            ) from exc
        raise RuntimeError(
            f"Failed to move finalized temp store into place. Existing output was preserved. "
            f"Final path: {final_path}; temp path: {temp_path}; error: {exc}"
        ) from exc

    if backup_path and os.path.exists(backup_path):
        if not _remove_path_best_effort(backup_path):
            logger.warning(
                "[ZarrWriter] Replacement succeeded but old-output backup remains: %s",
                backup_path,
            )

    return final_path


def _write_token_block(block, temp_path, dataset_path, origins_per_dim, node_id, execution_id, block_info):
    """
    Write one block region to the temp zarr store and return a tiny uint8 token.

    The token array is intentionally tiny so the executor waits on write completion
    without materializing full-size image data.
    """
    target = zarr.open(temp_path, mode="r+")[dataset_path]

    # Extract chunk origin from block_info.
    # block_info is a dict with numeric keys (0, None) — the actual per-chunk info
    # is in block_info[0] or block_info[None]. Dask provides array-location as
    # [(start0, end0), (start1, end1)] per axis, and chunk-location as (i, j, ...)
    # as the block index tuple.
    inner_info = None
    if isinstance(block_info, dict):
        if 0 in block_info:
            inner_info = block_info[0]
        elif None in block_info:
            inner_info = block_info[None]

    origin = None
    block_location = None
    if inner_info is not None:
        # Try array-location first — this gives exact per-axis slice starts
        array_loc = inner_info.get("array-location")
        if array_loc:
            starts = []
            for axis_loc in array_loc:
                if isinstance(axis_loc, (list, tuple)) and len(axis_loc) == 2:
                    starts.append(int(axis_loc[0]))
                else:
                    starts.append(0)
            origin = tuple(starts)
        # chunk-location is the block index (i, j, ...) for origins_per_dim fallback
        block_location = inner_info.get("chunk-location")

    # Fallback: compute origin from block_index and precomputed origins_per_dim
    if origin is None and block_location is not None and origins_per_dim is not None:
        origin = tuple(
            int(origins_per_dim[axis][int(block_index)])
            for axis, block_index in enumerate(block_location)
        )

    if origin is None:
        raise RuntimeError(
            "Cannot determine writer block origin. Dask block_info did not provide "
            "array-location and no chunk-origin fallback was available."
        )

    if len(origin) != block.ndim:
        raise ValueError(
            f"Writer origin ndim mismatch: origin={origin}, block shape={block.shape}."
        )

    region = tuple(
        slice(int(start), int(start) + int(length))
        for start, length in zip(origin, block.shape)
    )
    target[region] = block

    # Return tiny token — executor waits on these tokens, not on full image data
    return np.ones((1,) * block.ndim, dtype=np.uint8)


@register_node("OMEZarrWriter")
class OMEZarrWriter:
    """
    OME-Zarr writer node.

    A plain registered node (not a BaseMapBlockNode subclass).
    OUTPUT_NODE=True marks it as a final-output node whose returned Dask collection
    will be computed by the executor.

    Lifecycle:
      1. save_zarr() is called during GraphBuilding — initializes the temp zarr
         store and returns a lazy token array via dask_arr.map_blocks.
      2. The executor computes the token array in Phase 2 (Dask workers write regions).
      3. After successful compute, the executor calls postprocess() on this instance,
         which finalizes OME-NGFF metadata and moves temp→final.

    Writer-specific paths (temp_path, final_path, dataset_path) are private
    implementation details — not framework concepts.
    """

    CATEGORY = "WorkFlow/IO"
    DISPLAY_NAME = "OME-Zarr Writer (Save)"
    OUTPUT_NODE = True

    SKIP_EMPTY_BLOCKS = False
    SKIP_ALL_ZERO_BLOCKS = False
    FAILURE_POLICY = "raise"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "output_path": ("STRING", {"default": "output.zarr", "multiline": False}),
                "compressor_name": (["default", "zstd", "blosc", "lz4", "none"],),
            },
            "optional": {
                "metadata": ("DICT",),
            },
        }

    RETURN_TYPES = ("DASK_ARRAY[uint8]",)
    RETURN_NAMES = ("write_tokens",)
    FUNCTION = "save_zarr"

    def save_zarr(self, dask_arr, output_path="output.zarr", compressor_name="default",
                   metadata=None, **kwargs):
        """
        GraphBuilding phase — initialize writer state and return lazy token array.

        Must not compute the input data.
        """
        node_id = kwargs.get("_node_id")
        execution_id = kwargs.get("_execution_id")

        final_path = _normalize_output_path(output_path)
        temp_path = _make_temp_output_path(final_path)
        compressor_name = compressor_name or "default"
        compressor = _prepare_compressor(compressor_name)
        nominal_chunks = tuple(int(c) for c in dask_arr.chunksize)
        token_chunks = tuple((1,) * int(n) for n in dask_arr.numblocks)
        origins_per_dim = _chunk_origins_from_chunks(dask_arr.chunks)
        metadata = metadata if isinstance(metadata, dict) else None

        _validate_regular_chunks_for_region_writes(dask_arr.chunks)

        # Initialize the temp store during GraphBuilding.
        # The user-visible final path is not touched until all writes and
        # metadata finalization have succeeded.
        _init_zarr_store(
            temp_path,
            shape=dask_arr.shape,
            chunks=nominal_chunks,
            dtype=dask_arr.dtype,
            compressor=compressor,
        )

        # Store writer state for postprocess and cleanup
        self._writer_state = {
            "temp_path": temp_path,
            "final_path": final_path,
            "dataset_path": "0",
            "shape": tuple(dask_arr.shape),
            "chunks": nominal_chunks,
            "dtype": str(np.dtype(dask_arr.dtype)),
            "ndim": int(dask_arr.ndim),
            "metadata": metadata,
            "compressor_name": compressor_name,
            "overwrite": True,
        }
        self._preprocess_state = self._writer_state

        logger.info(
            "[ZarrWriter] save_zarr plan: final=%s, temp=%s, shape=%s, chunks=%s, "
            "numblocks=%s, token_chunks=%s, compressor=%s",
            final_path,
            temp_path,
            tuple(dask_arr.shape),
            dask_arr.chunks,
            tuple(dask_arr.numblocks),
            token_chunks,
            compressor_name,
        )

        # map_blocks returns tiny uint8 tokens — executor waits on these tokens,
        # not on the full image data, to confirm writes completed.
        tokens = dask_arr.map_blocks(
            _write_token_block,
            temp_path=temp_path,
            dataset_path="0",
            origins_per_dim=origins_per_dim,
            node_id=node_id,
            execution_id=execution_id,
            dtype=np.uint8,
            chunks=token_chunks,
            meta=np.array((), dtype=np.uint8),
            name=f"OMEZarrWriter_{node_id}" if node_id else "OMEZarrWriter",
        )
        return (tokens,)

    def postprocess(self, outputs=None, state=None, runtime=None, **kwargs):
        """
        Called by the executor after the returned Dask collection computed successfully.

        Writes OME-NGFF metadata to the temp store and replaces final_path with it.
        Raises on failure — the executor will mark the execution as failed.
        """
        writer_state = state or getattr(self, "_writer_state", None)
        if not writer_state:
            raise RuntimeError("OMEZarrWriter has no writer state to finalize.")

        temp_path = writer_state["temp_path"]
        final_path = writer_state["final_path"]
        ndim = writer_state["ndim"]
        metadata = writer_state.get("metadata")

        if not temp_path:
            raise RuntimeError(
                "OMEZarrWriter postprocess missing temp_path. "
                "Writer state was not preserved from execute phase."
            )
        if not final_path:
            raise RuntimeError(
                "OMEZarrWriter postprocess missing final_path. "
                "Writer state was not preserved from execute phase."
            )

        _finalize_store(temp_path, ndim, metadata)

        return _replace_final_with_temp(
            temp_path=temp_path,
            final_path=final_path,
            overwrite=writer_state.get("overwrite", True),
        )

    def cleanup(self):
        """
        Remove the temporary Zarr store on failure or cancellation.

        Called by the executor's cleanup phase when Phase 2 compute fails or
        the execution is cancelled. Idempotent: does nothing if temp_path
        does not exist.
        """
        writer_state = getattr(self, "_writer_state", None)
        if not writer_state:
            return
        temp_path = writer_state.get("temp_path")
        if temp_path:
            _remove_path_best_effort(temp_path)

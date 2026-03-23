import logging
import os

import dask
import numpy as np
import pandas as pd
from core.registry import register_node, ProgressType
from core.state_manager import state_manager
from utils.progress_helper import report_progress, report_stage_progress

logger = logging.getLogger("BrainFlow.PostProcessing")

try:
    import dask_image.ndmeasure

    HAS_DASK_IMAGE = True
except ImportError:
    HAS_DASK_IMAGE = False


def _stitch_progress_hook(block, block_info=None, node_id=None, execution_id=None):
    """
    用于 DaskLabelStitcher 的进度报告钩子。
    注意：这个函数必须在模块级别，以便 Dask 可以序列化。
    """
    if node_id:
        report_progress(node_id, execution_id=execution_id)
    return block


@register_node("DaskLabelStitcher")
class DaskLabelStitcher:
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "🧩 Stitch & Re-Label"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT  # 每块都会 report_progress

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
        node_id = kwargs.get('_node_id')
        execution_id = kwargs.get('_execution_id')

        # 1. 输入为 None -> 严格失败
        if mask_dask is None:
            raise ValueError("DaskLabelStitcher: mask_dask is None")

        # 2. dask-image 缺失
        if not HAS_DASK_IMAGE:
            if allow_fallback:
                logger.warning(f"[{node_id}] dask-image not installed, returning raw mask (DEGRADED)")
                state_manager.add_log(
                    f"⚠️ [{node_id}] Stitch result degraded: dask-image not installed",
                    "warning", execution_id=execution_id
                )
                return (mask_dask,)
            raise ImportError(
                "DaskLabelStitcher requires dask-image. Install with: pip install dask-image"
            )

        # 3. 空输入 -> 合法快速路径
        if mask_dask.size == 0:
            return (mask_dask,)

        # 4. 执行 stitch
        try:
            # dask-image 的 label 会返回 (labeled_array, num_features)
            labeled_result = dask_image.ndmeasure.label(mask_dask)
            stitched = labeled_result[0]

            # 注入进度监控 (使用 map_blocks 确保懒执行)
            final_output = stitched.map_blocks(
                _stitch_progress_hook,
                dtype=stitched.dtype,
                node_id=node_id,
                execution_id=execution_id
            )

            return (final_output,)

        except Exception as e:
            logger.error(f"[{node_id}] Stitch failed: {type(e).__name__}: {e}")
            if allow_fallback:
                logger.warning(f"[{node_id}] Falling back to raw mask (DEGRADED)")
                state_manager.add_log(
                    f"⚠️ [{node_id}] Stitch result degraded: {type(e).__name__}: {e}",
                    "warning", execution_id=execution_id
                )
                return (mask_dask,)
            raise


# 稀疏模式阈值：max_label / unique_labels > 此值时使用 sparse
SPARSE_THRESHOLD = 10


def _chunk_origins_from_chunks(chunks):
    origins_per_dim = []
    for dim_chunks in chunks:
        origins = []
        offset = 0
        for chunk_size in dim_chunks:
            origins.append(offset)
            offset += chunk_size
        origins_per_dim.append(origins)
    return origins_per_dim


def _summarize_label_block(block, origin):
    """
    对单个 label block 做局部统计。
    返回按 label 聚合的 count 与坐标和，便于后续全局合并。

    自适应选择 dense/sparse 模式：
    - 稀疏度 (max_label/unique_labels) > SPARSE_THRESHOLD 时用 sparse
    - 否则用传统 dense bincount
    """
    block = np.asarray(block)
    if block.size == 0:
        return {"mode": "sparse", "data": {}}  # 统一用 sparse 格式表示空

    labels = block.astype(np.int64, copy=False)
    flat_labels = labels.ravel()

    # 获取非零 label 的统计信息
    fg_mask = flat_labels > 0
    if not np.any(fg_mask):
        return {"mode": "sparse", "data": {}}

    unique_labels, counts = np.unique(flat_labels[fg_mask], return_counts=True)
    unique_count = len(unique_labels)
    max_label = int(unique_labels.max())

    # 判定是否使用 sparse 模式
    sparsity = max_label / unique_count if unique_count > 0 else 1
    use_sparse = sparsity > SPARSE_THRESHOLD

    ndim = labels.ndim

    if use_sparse:
        # === Sparse 模式：只存储实际存在的 label ===
        result = {}
        local_coords = np.indices(labels.shape, dtype=np.float64)

        for i, label_id in enumerate(unique_labels):
            label_id = int(label_id)
            label_mask = flat_labels == label_id
            coord_sums = []
            for axis, coord_grid in enumerate(local_coords):
                coord_sums.append(
                    float((coord_grid.ravel()[label_mask] + float(origin[axis])).sum())
                )
            result[label_id] = {
                "count": int(counts[i]),
                "sums": coord_sums
            }

        return {"mode": "sparse", "data": result}

    else:
        # === Dense 模式：保留原有 bincount 逻辑 ===
        dense_counts = np.zeros(max_label + 1, dtype=np.int64)
        for i, label_id in enumerate(unique_labels):
            dense_counts[label_id] = counts[i]

        local_coords = np.indices(labels.shape, dtype=np.float64)
        dense_sums = [np.zeros(max_label + 1, dtype=np.float64) for _ in range(ndim)]

        for i, label_id in enumerate(unique_labels):
            label_mask = flat_labels == label_id
            for axis, coord_grid in enumerate(local_coords):
                dense_sums[axis][label_id] = (
                    coord_grid.ravel()[label_mask] + float(origin[axis])
                ).sum()

        return {
            "mode": "dense",
            "max_label": max_label,
            "counts": dense_counts,
            "sums": dense_sums
        }


def _aggregate_stats_summaries(summaries, resolution_microns, node_id, execution_id=None):
    """
    聚合所有 block summary，并写出 CSV。

    统一使用稀疏字典 merge，避免按全局 max_label 预分配大数组。
    支持 dense 和 sparse 两种 summary 格式。
    """
    if execution_id:
        report_stage_progress(node_id, 20, 100, "Aggregating chunk summaries...", execution_id=execution_id)

    if not summaries:
        if execution_id:
            report_stage_progress(node_id, 100, 100, "No cells detected", execution_id=execution_id)
        return "No cells detected"

    # === 统一转成稀疏字典格式 ===
    merged = {}  # label_id -> {"count": int, "sums": [float, ...]}
    ndim = None

    for summary in summaries:
        mode = summary.get("mode", "dense")  # 兼容旧格式

        if mode == "sparse":
            # Sparse 格式：直接 merge
            sparse_data = summary.get("data", {})
            if not sparse_data:
                continue
            if ndim is None and sparse_data:
                first_item = next(iter(sparse_data.values()))
                ndim = len(first_item.get("sums", []))

            for label_id, data in sparse_data.items():
                label_id = int(label_id)
                count = data.get("count", 0)
                sums = data.get("sums", [])

                if label_id not in merged:
                    merged[label_id] = {"count": 0, "sums": [0.0] * len(sums) if sums else []}

                merged[label_id]["count"] += count
                for i, s in enumerate(sums):
                    merged[label_id]["sums"][i] += s

        else:
            # Dense 格式（兼容旧格式）：转成 sparse 再 merge
            max_label = summary.get("max_label", 0)
            counts = summary.get("counts")
            sums = summary.get("sums", [])

            if counts is None or max_label <= 0:
                continue

            if ndim is None:
                ndim = len(sums)

            # 只处理非零 label
            non_zero_indices = np.nonzero(counts[1:max_label + 1] > 0)[0] + 1

            for label_id in non_zero_indices:
                label_id = int(label_id)
                count = int(counts[label_id])

                if label_id not in merged:
                    merged[label_id] = {"count": 0, "sums": [0.0] * ndim}

                merged[label_id]["count"] += count
                for axis, sum_arr in enumerate(sums):
                    merged[label_id]["sums"][axis] += float(sum_arr[label_id])

    # === 从 merged 生成 DataFrame ===
    if not merged:
        if execution_id:
            report_stage_progress(node_id, 100, 100, "No cells detected", execution_id=execution_id)
        return "No cells detected"

    if execution_id:
        report_stage_progress(node_id, 40, 100, f"Computing stats for {len(merged)} cells...", execution_id=execution_id)

    # 提取数据
    ids = np.array(list(merged.keys()), dtype=np.int64)
    counts_fg = np.array([merged[i]["count"] for i in ids], dtype=np.int64)

    if execution_id:
        report_stage_progress(node_id, 60, 100, "Processing results...", execution_id=execution_id)

    # 确定坐标列名
    if ndim == 3:
        coord_columns = ['z', 'y', 'x']
    elif ndim == 2:
        coord_columns = ['y', 'x']
    else:
        coord_columns = [f'dim_{i}' for i in range(ndim)] if ndim else []

    data = {"id": ids, "volume_pixels": counts_fg}
    for axis, col in enumerate(coord_columns):
        axis_sum = np.array([merged[i]["sums"][axis] for i in ids])
        data[col] = axis_sum / counts_fg

    df = pd.DataFrame(data)
    pixel_size_um3 = resolution_microns ** 3
    df['volume_um3'] = df['volume_pixels'] * pixel_size_um3

    ordered_cols = ['id'] + [c for c in df.columns if c != 'id']
    df = df[ordered_cols]

    if execution_id:
        report_stage_progress(node_id, 80, 100, "Writing CSV...", execution_id=execution_id)

    output_dir = "./results"
    os.makedirs(output_dir, exist_ok=True)
    file_name = f"cell_stats_{node_id}.csv"
    full_path = os.path.abspath(os.path.join(output_dir, file_name))
    df.to_csv(full_path, index=False)

    logger.info(f"[Stats] Saved to: {full_path} ({len(merged)} cells)")
    if execution_id:
        report_stage_progress(node_id, 100, 100, "Done", execution_id=execution_id)
    return full_path


def _run_stats(mask_dask, resolution_microns, node_id, execution_id=None):
    """
    兼容 fallback：当上游没有保持为按块 delayed 时，退化到旧路径。
    优先目标仍是由 execute() 构造分块 summary graph，而不是整图物化。

    WARNING: 此路径会全量物化数组，可能导致内存峰值。仅用于小数据或兼容场景。
    """
    if not HAS_DASK_IMAGE:
        logger.error("dask-image not installed")
        return "Error: dask-image not installed"

    try:
        if execution_id:
            report_stage_progress(node_id, 0, 100, "Initializing...", execution_id=execution_id)
            report_stage_progress(node_id, 10, 100, "Preparing mask...", execution_id=execution_id)

        if dask.is_dask_collection(mask_dask):
            # === 内存预估保护 ===
            estimated_bytes = mask_dask.nbytes if hasattr(mask_dask, 'nbytes') else 0
            estimated_gb = estimated_bytes / (1024 ** 3)
            MAX_COMPUTE_GB = 8  # 超过此阈值警告

            if estimated_gb > MAX_COMPUTE_GB:
                logger.warning(
                    f"[Stats] Large array fallback compute: {estimated_gb:.2f} GB > {MAX_COMPUTE_GB} GB threshold. "
                    f"This may cause memory issues. Node: {node_id}"
                )
                if execution_id:
                    report_stage_progress(
                        node_id, 15, 100,
                        f"⚠️ Large array ({estimated_gb:.1f}GB) - may cause memory pressure",
                        execution_id=execution_id
                    )

            logger.info(f"[Stats] Fallback compute for node {node_id}, size: {estimated_gb:.2f} GB")
            mask = mask_dask.compute()
        else:
            mask = np.asarray(mask_dask)

        origin = tuple(0 for _ in range(mask.ndim))
        summary = _summarize_label_block(mask, origin)
        return _aggregate_stats_summaries([summary], resolution_microns, node_id, execution_id=execution_id)

    except Exception as e:
        import traceback
        logger.error(f"[Stats] Failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        if execution_id:
            report_stage_progress(node_id, 0, 100, f"Error: {type(e).__name__}: {str(e)}", execution_id=execution_id)
        return f"Error: {type(e).__name__}: {str(e)}"


@register_node("DaskStats")
class DaskStats:
    """
    统计分析节点：构建统计计算的 lazy delayed，由 executor 统一触发。
    进度通过 callback 参数传入 delayed 函数内部上报（STAGE_BASED）。
    """
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "📊 Cell Statistics"
    OUTPUT_NODE = True
    PROGRESS_TYPE = ProgressType.STAGE_BASED

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mask_dask": ("DASK_ARRAY",),
                "resolution_microns": ("FLOAT", {"default": 1.0, "min": 0.01, "max": 100.0}),
            }
        }

    # 返回 DELAYED 类型，executor 识别后统一提交
    RETURN_TYPES = ("DELAYED",)
    RETURN_NAMES = ("stats_task",)
    FUNCTION = "execute"

    def execute(self, mask_dask, resolution_microns=1.0, callback=None, **kwargs):
        node_id = kwargs.get('_node_id')
        execution_id = kwargs.get('_execution_id')

        if not HAS_DASK_IMAGE:
            logger.error("dask-image not installed")
            return (dask.delayed(lambda: "Error: dask-image not installed")(),)

        try:
            if hasattr(mask_dask, "to_delayed") and hasattr(mask_dask, "chunks"):
                delayed_blocks = mask_dask.to_delayed().ravel().tolist()
                origins_per_dim = _chunk_origins_from_chunks(mask_dask.chunks)

                block_summaries = []
                flat_index = 0
                for block_idx in np.ndindex(*mask_dask.numblocks):
                    origin = tuple(origins_per_dim[axis][block_idx[axis]] for axis in range(mask_dask.ndim))
                    delayed_block = delayed_blocks[flat_index]
                    flat_index += 1
                    block_summaries.append(dask.delayed(_summarize_label_block)(delayed_block, origin))

                lazy_stats = dask.delayed(_aggregate_stats_summaries)(
                    block_summaries,
                    resolution_microns,
                    node_id,
                    execution_id,
                )
            else:
                # fallback：如果输入已经是具体化对象，走兼容路径；
                # 如果仍是 dask collection，则保守退化为旧路径（存在内存峰值风险，但语义正确）。
                lazy_stats = dask.delayed(_run_stats)(
                    mask_dask,
                    resolution_microns,
                    node_id,
                    execution_id,
                )

            logger.info(f"[Stats] Lazy stats plan ready for node {node_id}")
            return (lazy_stats, {"sink_progress": {"kind": "stage_queue"}})
        except Exception as e:
            logger.error(f"[Stats] Failed to build lazy stats plan: {e}")
            return (dask.delayed(lambda msg=f'Error: {e}': msg)(), {"sink_progress": {"kind": "stage_queue"}})

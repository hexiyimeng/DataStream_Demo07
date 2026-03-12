import numpy as np
import pandas as pd
import os
import dask.array as da
import logging
from core.registry import register_node, ProgressType
from dask.distributed import get_worker, wait, get_client
from utils.progress_helper import report_progress

logger = logging.getLogger("BrainFlow.PostProcessing")

try:
    import dask_image.ndmeasure

    HAS_DASK_IMAGE = True
except ImportError:
    HAS_DASK_IMAGE = False


def _stitch_progress_hook(block, block_info=None, node_id=None):
    """
    用于 DaskLabelStitcher 的进度报告钩子。
    注意：这个函数必须在模块级别，以便 Dask 可以序列化。
    """
    if node_id:
        report_progress(node_id)
    return block


def attach_progress_reporter(dask_arr, node_id):
    """
    内部辅助函数：将进度上报钩子注入 Dask 计算图。
    这确保了进度条反映的是 Worker 真正的计算进度。
    """
    if not node_id:
        return dask_arr

    def _report_chunk_passthrough(block, block_info=None, nid=None):
        if nid:
            report_progress(nid)
        return block

    return dask_arr.map_blocks(
        _report_chunk_passthrough,
        dtype=dask_arr.dtype,
        nid=node_id
    )



@register_node("DaskLabelStitcher")
class DaskLabelStitcher:
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "🧩 Stitch & Re-Label"
    PROGRESS_TYPE = ProgressType.STATE_ONLY  # 仅状态，无百分比

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"mask_dask": ("DASK_ARRAY",), "overlap": ("INT", {"default": 10})}}

    RETURN_TYPES = ("DASK_ARRAY",)
    RETURN_NAMES = ("stitched_mask",)
    FUNCTION = "execute"

    def execute(self, mask_dask, overlap, **kwargs):
        node_id = kwargs.get('_node_id')

        # 1. 严格性检查：确保输入不是 None 且是 Dask 数组
        if mask_dask is None:
            logger.error("拼接节点输入为 None")
            return (None,)

        if not HAS_DASK_IMAGE:
            return (mask_dask,)

        try:
            # 2. 修复 nonzero 0d 错误：
            # 确保输入数组的维度正确，如果数据是空的，直接跳过计算
            if mask_dask.size == 0:
                return (mask_dask,)

            # 执行拼接逻辑
            # dask-image 的 label 会返回 (labeled_array, num_features)
            # 我们只需要第一个结果
            labeled_result = dask_image.ndmeasure.label(mask_dask)
            stitched = labeled_result[0]

            # 3. 注入进度监控 (使用 map_blocks 确保懒执行)
            # 使用模块级别的函数以确保可序列化
            final_output = stitched.map_blocks(
                _stitch_progress_hook,
                dtype=stitched.dtype,
                node_id=node_id
            )

            return (final_output,)

        except Exception as e:
            # 这里的报错就是你看到的 "nonzero on 0d arrays"
            # 究其原因是某些分块在计算中丢失了维度信息
            logger.error(f"标签拼接过程发生错误: {str(e)}")
            # 容错处理：如果拼接失败，返回原始 mask，保证流程不中断
            return (mask_dask,)


@register_node("DaskStats")
class DaskStats:
    """
    统计分析节点：计算每个细胞的体积、重心，并自动导出 CSV。
    这是一个聚合型节点，工作单位是 futures 和统计操作，不适合 chunk 百分比。
    使用状态型进度报告：Initializing → Computing max → Computing stats → Writing CSV → Done
    """
    CATEGORY = "BrainFlow/PostProcessing"
    DISPLAY_NAME = "📊 Cell Statistics"
    OUTPUT_NODE = True  # 标记为输出节点，强制计算
    PROGRESS_TYPE = ProgressType.STAGE_BASED  # 阶段性进度

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mask_dask": ("DASK_ARRAY",),
                "resolution_microns": ("FLOAT", {"default": 1.0, "min": 0.01, "max": 100.0}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("csv_path",)
    FUNCTION = "execute"

    def execute(self, mask_dask, resolution_microns=1.0, callback=None, **kwargs):
        node_id = kwargs.get('_node_id')

        if not HAS_DASK_IMAGE:
            if callback: callback(100, 100, "Error: dask-image not installed")
            return ("Error: dask-image not installed",)

        try:
            client = get_client()

            # 状态 1: 初始化
            if callback: callback(0, 100, "Initializing...")

            # 1. 计算总标签数
            if callback: callback(20, 100, "Computing max...")
            total_cells = int(mask_dask.max().compute())
            if total_cells == 0:
                if callback: callback(100, 100, "No cells detected")
                return ("No cells detected",)

            # 状态 2: 计算统计信息
            if callback: callback(40, 100, f"Computing stats for {total_cells} cells...")

            index = np.arange(1, total_cells + 1)
            com_dask = dask_image.ndmeasure.center_of_mass(mask_dask, mask_dask, index)
            vol_dask = dask_image.ndmeasure.sum(da.ones_like(mask_dask), mask_dask, index)

            # 提交分布式计算
            logger.info(f"正在计算 {total_cells} 个对象的统计信息...")
            future_com = client.compute(com_dask)
            future_vol = client.compute(vol_dask)

            # 等待计算完成
            wait([future_com, future_vol])

            centers = future_com.result()
            volumes = future_vol.result()

            # 状态 3: 数据整合
            if callback: callback(60, 100, "Processing results...")

            # 3. 数据整合与物理尺寸转换
            centers_arr = np.array(centers)
            if centers_arr.ndim == 2 and centers_arr.shape[1] >= 2:
                # 兼容 3D (Z, Y, X) 和 2D (Y, X)
                cols = ['z', 'y', 'x'] if centers_arr.shape[1] == 3 else ['y', 'x']
                df = pd.DataFrame(centers_arr, columns=cols)
            else:
                df = pd.DataFrame(index=index)

            df['id'] = index
            df['volume_pixels'] = volumes

            # 物理体积换算
            pixel_size_um3 = resolution_microns ** 3
            df['volume_um3'] = df['volume_pixels'] * pixel_size_um3

            # 调整列顺序
            final_cols = ['id'] + [c for c in df.columns if c not in ['id']]
            df = df[final_cols]

            # 状态 4: 写入 CSV
            if callback: callback(80, 100, "Writing CSV...")

            # 自动创建输出目录并保存 CSV
            output_dir = "./results"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            file_name = f"cell_stats_{node_id}.csv"
            full_path = os.path.abspath(os.path.join(output_dir, file_name))
            df.to_csv(full_path, index=False)

            logger.info(f"统计结果已保存到: {full_path}")

            if callback: callback(100, 100, "Done")
            return (full_path,)

        except Exception as e:
            logger.error(f"统计节点计算失败: {e}")
            if callback: callback(0, 100, f"Error: {str(e)}")
            return (f"Error: {str(e)}",)
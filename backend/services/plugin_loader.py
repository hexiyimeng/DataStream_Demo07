import os
import sys
import importlib
import logging
import traceback
from typing import List, Tuple

logger = logging.getLogger("BrainFlow.Plugins")

# 关键插件列表：这些插件失败应视为严重错误
# 可通过环境变量扩展：BRAINFLOW_CRITICAL_PLUGINS=nodes.cellpose_node,nodes.ome_zarr_flow
CRITICAL_PLUGINS = {
    "nodes.cellpose_node",      # 核心分割节点
    "nodes.ome_zarr_flow",      # IO 节点
    "nodes.post_processing_nodes",  # 后处理节点
}


def _get_critical_plugins() -> set:
    """获取关键插件列表（内置 + 环境变量扩展）"""
    critical = set(CRITICAL_PLUGINS)
    env_plugins = os.getenv("BRAINFLOW_CRITICAL_PLUGINS", "")
    if env_plugins:
        for p in env_plugins.split(","):
            p = p.strip()
            if p:
                critical.add(p)
    return critical


def load_all_plugins() -> Tuple[bool, List[str], List[str]]:
    """
    递归加载 backend/nodes 及其子目录下的所有 Python 插件。

    Returns:
        Tuple[bool, List[str], List[str]]: (是否全部成功, 成功列表, 失败列表)

    Raises:
        RuntimeError: 当关键插件加载失败时抛出
    """
    # 1. 确定目录位置
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_file_dir)

    # 兼容性处理
    if not os.path.exists(os.path.join(root_dir, "nodes")):
        root_dir = current_file_dir

    nodes_path = os.path.join(root_dir, "nodes")

    if not os.path.exists(nodes_path):
        logger.error(f"[Plugins] Nodes directory not found at: {nodes_path}")
        raise RuntimeError(f"Nodes directory not found: {nodes_path}")

    # 2. 确保 backend 根目录在 sys.path 中
    if root_dir not in sys.path:
        sys.path.append(root_dir)

    logger.info(f"[Plugins] Scanning plugins in: {nodes_path}")

    success_list = []
    failed_list = []
    critical_plugins = _get_critical_plugins()

    # 3. 递归扫描
    for root, dirs, files in os.walk(nodes_path):
        if "__pycache__" in dirs:
            dirs.remove("__pycache__")

        for filename in files:
            if filename.endswith(".py") and filename != "__init__.py":
                file_path = os.path.join(root, filename)
                rel_path = os.path.relpath(file_path, root_dir)
                module_name = rel_path.replace(os.sep, ".")[:-3]

                try:
                    importlib.import_module(module_name)
                    success_list.append(module_name)
                    logger.debug(f"[Plugins] ✓ Loaded: {module_name}")
                except Exception as e:
                    failed_list.append(module_name)
                    error_msg = str(e)

                    # 判断是否为关键插件
                    is_critical = module_name in critical_plugins

                    if is_critical:
                        logger.error(
                            f"[Plugins] ✗ CRITICAL plugin failed: {module_name}\n"
                            f"    Error: {type(e).__name__}: {error_msg}\n"
                            f"    This plugin is required for core functionality."
                        )
                        # 关键插件失败：抛出异常，阻止半残启动
                        raise RuntimeError(
                            f"Critical plugin '{module_name}' failed to load: {type(e).__name__}: {error_msg}\n"
                            f"Cannot start in degraded mode. Please fix the plugin or remove it from BRAINFLOW_CRITICAL_PLUGINS."
                        ) from e
                    else:
                        # 非关键插件失败：记录警告，继续启动
                        logger.warning(
                            f"[Plugins] ✗ Non-critical plugin failed: {module_name}\n"
                            f"    Error: {type(e).__name__}: {error_msg}\n"
                            f"    System will continue, but some nodes may be unavailable."
                        )

    # 4. 输出加载摘要
    total = len(success_list) + len(failed_list)
    logger.info(
        f"[Plugins] Load summary: {len(success_list)}/{total} succeeded, "
        f"{len(failed_list)} failed"
    )

    if failed_list:
        logger.warning(f"[Plugins] Failed plugins: {failed_list}")

    if success_list:
        logger.info(f"[Plugins] Successfully loaded: {success_list}")

    return len(failed_list) == 0, success_list, failed_list
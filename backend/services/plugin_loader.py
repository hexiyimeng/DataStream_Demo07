import os
import sys
import importlib
import logging

logger = logging.getLogger("BrainFlow.Plugins")


def load_all_plugins():
    """
    递归加载 backend/nodes 及其子目录下的所有 Python 插件。
    支持结构：
    backend/
      nodes/
        my_node.py          (加载为 nodes.my_node)
        subfolder/
          extra_node.py     (加载为 nodes.subfolder.extra_node)
    """
    # 1. 确定目录位置
    # 假设 plugin_loader.py 在 backend/services/ 下
    # 如果你在 main.py 里用，请调整 root_dir 的获取方式
    current_file_dir = os.path.dirname(os.path.abspath(__file__))

    # 向两级找到 backend 根目录 (services -> backend)
    root_dir = os.path.dirname(current_file_dir)

    # 兼容性处理：如果还是没找到 nodes，可能是在 main.py 同级运行
    if not os.path.exists(os.path.join(root_dir, "nodes")):
        root_dir = current_file_dir  # 回退到当前目录试一下

    nodes_path = os.path.join(root_dir, "nodes")

    if not os.path.exists(nodes_path):
        logger.warning(f" Nodes directory not found at: {nodes_path}")
        return

    # 2. 关键：确保 backend 根目录在 sys.path 中
    # 这样 python 才能识别 'nodes' 包
    if root_dir not in sys.path:
        sys.path.append(root_dir)

    logger.info(f" Scanning plugins recursively in: {nodes_path}")

    success_count = 0

    # 3. 递归扫描 (os.walk)
    for root, dirs, files in os.walk(nodes_path):
        # 忽略 __pycache__
        if "__pycache__" in dirs:
            dirs.remove("__pycache__")

        for filename in files:
            if filename.endswith(".py") and filename != "__init__.py":
                # 获取文件的完整路径
                file_path = os.path.join(root, filename)

                # 计算相对路径，例如: nodes/folder/script.py
                rel_path = os.path.relpath(file_path, root_dir)

                # 转换为模块路径: nodes.folder.script
                module_name = rel_path.replace(os.sep, ".")[:-3]

                try:
                    importlib.import_module(module_name)
                    # logger.info(f"   Loaded: {module_name}")
                    success_count += 1
                except Exception as e:
                    logger.error(f" Failed to load {module_name}: {e}")
                    # 打印堆栈以便调试具体的 ImportError
                    # import traceback
                    # traceback.print_exc()

    logger.info(f" Successfully loaded {success_count} plugin modules.")

# 如果你在 main.py 中直接使用，请确保调用了这个函数
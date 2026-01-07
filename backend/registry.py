# registry.py
from typing import Dict, Type

NODE_CLASS_MAPPINGS: Dict[str, Type] = {}
NODE_DISPLAY_NAME_MAPPINGS: Dict[str, str] = {}


def register_node(name: str):
    """
    装饰器：注册节点类
    """

    def decorator(cls):
        NODE_CLASS_MAPPINGS[name] = cls
        cls.NODE_TYPE_NAME = name
        if hasattr(cls, "DISPLAY_NAME"):
            NODE_DISPLAY_NAME_MAPPINGS[name] = cls.DISPLAY_NAME
        return cls

    return decorator


def get_node_info():
    """
    生成符合 ComfyUI 标准的前端协议 JSON
    """
    info = {}
    for name, cls in NODE_CLASS_MAPPINGS.items():
        # 1. 获取输入定义
        # ComfyUI 标准: INPUT_TYPES 必须是类方法
        if hasattr(cls, "INPUT_TYPES"):
            try:
                input_config = cls.INPUT_TYPES()
            except Exception as e:
                print(f"Error getting input types for {name}: {e}")
                input_config = {"required": {}, "optional": {}}
        else:
            input_config = {"required": {}, "optional": {}}

        # 2. 处理输出定义
        # RETURN_TYPES: 输出类型的列表 (e.g. ["IMAGE", "MASK"])
        # RETURN_NAMES: 输出插槽的名称 (e.g. ["Image", "Alpha"]) - 可选
        return_types = getattr(cls, "RETURN_TYPES", [])
        return_names = getattr(cls, "RETURN_NAMES", [])

        # 如果没有定义输出名称，默认生成 output_0, output_1... 或者直接用类型名
        if not return_names and return_types:
            return_names = return_types

        # 3. 获取其他元数据
        category = getattr(cls, "CATEGORY", "User/Custom")
        display_name = getattr(cls, "DISPLAY_NAME", name)
        description = cls.__doc__.strip() if cls.__doc__ else "No description."

        info[name] = {
            "name": name,
            "display_name": display_name,
            "category": category,
            "description": description,
            "input": input_config,
            "output": return_types,
            "output_name": return_names,
            # 告诉前端这个节点实际执行哪个函数（虽然前端不一定用，但这是协议的一部分）
            "output_node": getattr(cls, "OUTPUT_NODE", False)
        }
    return info
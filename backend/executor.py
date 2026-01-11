# executor.py
import asyncio
import traceback
import inspect
import functools
import psutil
from registry import NODE_CLASS_MAPPINGS


# ==========================================
# 1. 参数校验与默认值填充 (已修复 Dask Array 比较报错)
# ==========================================
def validate_and_prepare_inputs(node_cls, raw_inputs, node_id="Unknown"):
    """
    校验输入参数，补充默认值，并对必填项进行强制检查。
    """
    final_inputs = {}
    if hasattr(node_cls, "INPUT_TYPES"):
        input_defs = node_cls.INPUT_TYPES()
    else:
        input_defs = {"required": {}, "optional": {}}

    # 1. 必填项 (Required) - 必须有值，否则报错！
    required_defs = input_defs.get("required", {})
    for name, config in required_defs.items():
        val = raw_inputs.get(name)
        input_type = config[0]
        meta = config[1] if len(config) > 1 and isinstance(config[1], dict) else {}

        # 🔥🔥🔥【修复点 1】安全检查空值
        # 不能直接写 val == ""，因为如果 val 是 Array 会报错
        is_empty = False
        if val is None:
            is_empty = True
        elif isinstance(val, str) and val == "":
            is_empty = True

        # 尝试使用默认值
        if is_empty:
            if "default" in meta:
                val = meta["default"]
            elif isinstance(input_type, list) and len(input_type) > 0:
                val = input_type[0]

        # 🔥🔥🔥【修复点 2】再次安全检查
        # 经过默认值填充后，如果还是空的，且类型是 STRING，才报错
        # 这样 Dask Array (非字符串) 就不会触发这个检查
        is_still_empty = False
        if val is None:
            is_still_empty = True
        elif isinstance(val, str) and val == "":
            is_still_empty = True

        if is_still_empty and input_type == "STRING":
            raise ValueError(f"❌ 节点错误: 必填项 '{name}' 不能为空！")

        final_inputs[name] = val

    # 2. 选填项 (Optional) - 可以为空
    optional_defs = input_defs.get("optional", {})
    for name, config in optional_defs.items():
        val = raw_inputs.get(name)
        meta = config[1] if len(config) > 1 and isinstance(config[1], dict) else {}

        if val is None:
            if "default" in meta:
                val = meta["default"]

        final_inputs[name] = val

    # 3. 类型转换 (通用)
    for name, val in final_inputs.items():
        if val is not None:
            if isinstance(val, (str, int, float)):  # 简单类型转换
                # 重新获取 definition 确认类型
                def_info = required_defs.get(name) or optional_defs.get(name)
                if def_info:
                    def_type = def_info[0]
                    if def_type == "INT":
                        try:
                            final_inputs[name] = int(val)
                        except:
                            pass
                    elif def_type == "FLOAT":
                        try:
                            final_inputs[name] = float(val)
                        except:
                            pass

    return final_inputs


# ==========================================
# 2. 纯净监控
# ==========================================
async def run_system_monitor(websocket):
    try:
        while True:
            mem = psutil.virtual_memory()
            cpu = psutil.cpu_percent()
            msg = f"🖥️ [System] RAM: {mem.percent}% | CPU: {cpu}%"
            await websocket.send_json({"type": "log", "message": msg})
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        pass


# ==========================================
# 3. 核心执行器
# ==========================================
async def execute_graph(graph: dict, websocket):
    await websocket.send_json({"type": "log", "message": "🚀 引擎启动 (Local Mode)..."})
    results = {}

    monitor_task = asyncio.create_task(run_system_monitor(websocket))

    async def progress_callback(node_id, current, total, msg=""):
        if total == 0: total = 1
        p = int((current / total) * 100)
        await websocket.send_json({"type": "progress", "taskId": node_id, "progress": p, "message": msg})

    async def get_node_result(node_id):
        if node_id in results: return results[node_id]

        node_data = graph.get(node_id)
        class_name = node_data["type"]
        print(f"🛠️ [Debug] 解析节点: {class_name}", flush=True)

        raw_inputs = {}
        for k, v in node_data.get("inputs", {}).items():
            if isinstance(v, list) and len(v) == 2:
                src_out = await get_node_result(v[0])
                if isinstance(src_out, tuple):
                    idx = v[1]
                    raw_inputs[k] = src_out[idx] if idx < len(src_out) else src_out[0]
                else:
                    raw_inputs[k] = src_out
            else:
                raw_inputs[k] = v

        try:
            NodeCls = NODE_CLASS_MAPPINGS[class_name]
            # 🔥 传入 node_id 方便报错
            func_args = validate_and_prepare_inputs(NodeCls, raw_inputs, node_id)

            instance = NodeCls()
            method_name = getattr(NodeCls, "FUNCTION", "execute")
            method = getattr(instance, method_name)

            # 注入 callback
            if 'callback' in inspect.signature(method).parameters:
                func_args['callback'] = lambda c, t, m="": progress_callback(node_id, c, t, m)

            sig = inspect.signature(method)
            valid_args = {k: v for k, v in func_args.items() if k in sig.parameters}

            print(f"🚀 [Debug] 调用 {class_name}.{method_name}", flush=True)

            if asyncio.iscoroutinefunction(method):
                output = await method(**valid_args)
            else:
                loop = asyncio.get_running_loop()
                output = await loop.run_in_executor(None, functools.partial(method, **valid_args))

            print(f"✅ [Debug] 完成: {class_name}", flush=True)
            results[node_id] = output
            return output

        except Exception as e:
            traceback.print_exc()
            await websocket.send_json({"type": "error", "message": str(e)})
            raise e

    try:
        output_nodes = [nid for nid, d in graph.items() if
                        getattr(NODE_CLASS_MAPPINGS.get(d["type"]), "OUTPUT_NODE", False)]
        if not output_nodes:
            if graph: await get_node_result(list(graph.keys())[-1])
        else:
            for nid in output_nodes: await get_node_result(nid)

        await websocket.send_json({"type": "done", "message": "Done"})

    finally:
        monitor_task.cancel()
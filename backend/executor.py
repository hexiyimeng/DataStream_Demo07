# executor.py
import asyncio
import traceback
import inspect
import functools
import psutil
from registry import NODE_CLASS_MAPPINGS


# ==========================================
# 1. 参数校验与默认值填充 (逻辑不变)
# ==========================================
def validate_and_prepare_inputs(node_cls, raw_inputs):
    final_inputs = {}
    if hasattr(node_cls, "INPUT_TYPES"):
        input_defs = node_cls.INPUT_TYPES()
    else:
        input_defs = {"required": {}, "optional": {}}

    all_defs = {**input_defs.get("required", {}), **input_defs.get("optional", {})}

    for name, config in all_defs.items():
        val = raw_inputs.get(name)
        input_type = config[0]
        meta = {}
        if len(config) > 1 and isinstance(config[1], dict):
            meta = config[1]

        if val is None:
            if "default" in meta:
                val = meta["default"]
            elif isinstance(input_type, list) and len(input_type) > 0:
                val = input_type[0]

        if val is not None:
            if input_type == "INT":
                try:
                    val = int(val)
                except:
                    pass
            elif input_type == "FLOAT":
                try:
                    val = float(val)
                except:
                    pass

        final_inputs[name] = val
    return final_inputs


# ==========================================
# 2. 纯净监控 (删除了所有 Dask Dashboard 代码)
# ==========================================
async def run_system_monitor(websocket):
    # 🔥 彻底移除 Client 启动代码，防止端口冲突报错
    try:
        while True:
            mem = psutil.virtual_memory()
            cpu = psutil.cpu_percent()
            msg = f"🖥️ [System] RAM: {mem.percent}% | CPU: {cpu}%"
            # 发送日志
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
            # 参数补全
            func_args = validate_and_prepare_inputs(NodeCls, raw_inputs)

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
                # 本地模式下，Callback 会在这里正常工作
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
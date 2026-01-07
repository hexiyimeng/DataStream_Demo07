import os
import sys
import importlib
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# 导入核心组件
from registry import get_node_info
from executor import execute_graph

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BrainFlow")

app = FastAPI(title="BrainFlow Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_all_plugins():
    logger.info("🔌 Loading plugins...")

    # 只尝试加载 nodes/ 子目录下的插件，移除硬编码导入
    nodes_dir = os.path.join(os.path.dirname(__file__), "nodes")
    if os.path.exists(nodes_dir):
        # 临时将 nodes 加入 path 以便 import
        if nodes_dir not in sys.path:
            sys.path.append(nodes_dir)

        for filename in os.listdir(nodes_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]
                try:
                    # 动态导入 nodes.xxx
                    if os.path.exists(os.path.join(nodes_dir, "__init__.py")):
                        importlib.import_module(f"nodes.{module_name}")
                    else:
                        importlib.import_module(module_name)

                    logger.info(f"✅ Extension Loaded: {module_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to load {module_name}: {e}")
    else:
        logger.info(f"ℹ️ No 'nodes/' directory found. Running in core-only mode.")



# 启动时加载
load_all_plugins()


# === API 路由 ===

@app.get("/object_info")
async def get_node_definitions():
    """前端获取节点定义的接口"""
    return get_node_info()


@app.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 执行通道"""
    await websocket.accept()
    client_ip = websocket.client.host
    logger.info(f"Client connected: {client_ip}")

    try:
        while True:
            data = await websocket.receive_json()

            if data.get("command") == "execute_graph":
                graph = data.get("graph")
                if graph:
                    logger.info(f"Received graph execution request from {client_ip}")
                    # 调用 executor 执行
                    await execute_graph(graph, websocket)
                else:
                    await websocket.send_json({"type": "error", "message": "Graph data is empty"})

            elif data.get("command") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {client_ip}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)


if __name__ == "__main__":
    import uvicorn

    # 生产环境建议关掉 reload，开发环境开启
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
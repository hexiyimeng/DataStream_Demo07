import logging
import os
import sys
import importlib

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from dask_manager import start_dask_cluster, stop_dask_cluster, get_client  # 确保引入 get_client

from registry import get_node_info
from executor import execute_graph
import asyncio as _real_asyncio

asyncio = _real_asyncio

import os
# 阈值设为 0 表示释放即归还，或者设为 10000 (10KB)
os.environ["MALLOC_TRIM_THRESHOLD_"] = "0"

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Flow")

# =========================================================
# [新增] 1. 启动消息缓冲区
# 用来暂存服务器启动时的重要信息，等前端连上后回放
# =========================================================
STARTUP_MESSAGES = []


def log_and_buffer(msg: str):
    """同时打印到控制台并存入缓冲区"""
    logger.info(msg)
    STARTUP_MESSAGES.append(msg)


# =========================================================
# 2. 修改生命周期：使用 buffer 记录启动日志
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 启动时
    log_and_buffer(">>> Backend Starting...")

    # 启动 Dask (并获取 client 对象以提取信息)
    client = start_dask_cluster()

    if client:
        log_and_buffer(" Dask Cluster Started Successfully!")
        # 这里把最重要的 Dashboard 地址存下来
        log_and_buffer(f" Dask Dashboard: {client.dashboard_link}")
        log_and_buffer("   (Ctrl+Click the link above to monitor tasks)")
    else:
        log_and_buffer("⚠ Dask Cluster failed to start (Running in local mode).")

    yield

    # 2. 关闭时
    logger.info("<<< Backend Shutting down...")
    stop_dask_cluster()


# === 应用初始化 ===
app = FastAPI(title="Flow Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_all_plugins():
    logger.info(" Loading plugins...")
    nodes_dir = os.path.join(os.path.dirname(__file__), "nodes")
    if os.path.exists(nodes_dir):
        if nodes_dir not in sys.path:
            sys.path.append(nodes_dir)

        for filename in os.listdir(nodes_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]
                try:
                    if os.path.exists(os.path.join(nodes_dir, "__init__.py")):
                        importlib.import_module(f"nodes.{module_name}")
                    else:
                        importlib.import_module(module_name)

                    logger.info(f"Extension Loaded: {module_name}")
                except Exception as e:
                    logger.error(f" Failed to load {module_name}: {e}")
    else:
        logger.info(f"ℹ No 'nodes/' directory found. Running in core-only mode.")


# 启动时加载
load_all_plugins()


# === API 路由 ===

@app.get("/object_info")
async def get_node_definitions():
    """前端获取节点定义的接口"""
    return get_node_info()


@app.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_ip = websocket.client.host
    logger.info(f"Client connected: {client_ip}")

    # =========================================================
    # [新增] 3. 连接建立后的“日志回放”
    # =========================================================

    # A. 回放启动时的历史消息
    for msg in STARTUP_MESSAGES:
        await websocket.send_json({"type": "log", "message": msg})

    # B. 双重保险：实时获取一次 Dask 状态并发送 (防止启动时 buffer 漏了)
    try:
        dask_client = get_client()
        if dask_client:
            # 发送一条醒目的系统消息
            await websocket.send_json({
                "type": "log",
                "message": f"[System] Dask Ready: {dask_client.dashboard_link}"
            })
    except:
        pass
    # =========================================================

    # 用于追踪当前正在运行的任务
    current_execution_task: asyncio.Task = None

    try:
        while True:
            data = await websocket.receive_json()
            command = data.get("command")

            if command == "execute_graph":
                if current_execution_task and not current_execution_task.done():
                    logger.info("Cancelling previous task...")
                    current_execution_task.cancel()
                    try:
                        await current_execution_task
                    except asyncio.CancelledError:
                        pass

                graph = data.get("graph")
                if graph:
                    logger.info(f"Starting execution for {client_ip}")
                    current_execution_task = asyncio.create_task(execute_graph(graph, websocket))
                else:
                    await websocket.send_json({"type": "error", "message": "Graph data is empty"})

            elif command == "stop_execution":
                if current_execution_task and not current_execution_task.done():
                    logger.info(" Received Stop Command. Cancelling task...")
                    current_execution_task.cancel()
                else:
                    await websocket.send_json({"type": "log", "message": "No active execution to stop."})

            elif command == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {client_ip}")
        if current_execution_task and not current_execution_task.done():
            current_execution_task.cancel()

    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)


if __name__ == "__main__":
    import uvicorn

    # 生产环境建议关掉 reload，开发环境开启
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
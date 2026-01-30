import logging
import os
import sys
import importlib
import asyncio as _real_asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from dask_manager import start_dask_cluster, stop_dask_cluster, get_client
from registry import get_node_info
from executor import execute_graph
# 引入状态管理器
from state_manager import state_manager

asyncio = _real_asyncio
os.environ["MALLOC_TRIM_THRESHOLD_"] = "0"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Flow")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 启动时记录日志到状态管理器
    state_manager.add_log(">>> Backend Starting...", "info")

    client = start_dask_cluster()
    if client:
        # 同时打印到控制台(通过logger)和记录到buffer(通过add_log)
        logger.info(f"Dask Dashboard: {client.dashboard_link}")
        state_manager.add_log(f"Dask Cluster Ready. Dashboard: {client.dashboard_link}", "success")
    else:
        state_manager.add_log("Dask Cluster failed. Running in local mode.", "warning")

    yield

    # 2. 关闭
    logger.info("<<< Backend Shutting down...")
    stop_dask_cluster()


app = FastAPI(title="Flow Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 加载插件
def load_all_plugins():
    logger.info("Loading plugins...")
    nodes_dir = os.path.join(os.path.dirname(__file__), "nodes")
    if os.path.exists(nodes_dir):
        if nodes_dir not in sys.path: sys.path.append(nodes_dir)
        for filename in os.listdir(nodes_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                try:
                    module_name = filename[:-3]
                    if os.path.exists(os.path.join(nodes_dir, "__init__.py")):
                        importlib.import_module(f"nodes.{module_name}")
                    else:
                        importlib.import_module(module_name)
                    logger.info(f"Extension Loaded: {module_name}")
                except Exception as e:
                    logger.error(f"Failed to load: {e}")
    else:
        logger.info("No 'nodes/' directory found.")


load_all_plugins()


@app.get("/object_info")
async def get_node_definitions():
    return get_node_info()


@app.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_ip = websocket.client.host
    logger.info(f"Client connected: {client_ip}")

    # [核心变更] 注册客户端 -> 自动发送历史日志和进度
    await state_manager.register_client(websocket)

    try:
        while True:
            data = await websocket.receive_json()
            command = data.get("command")

            if command == "execute_graph":
                # 如果有旧任务，先取消 (或者你可以选择排队，这里简化为替换)
                if state_manager.current_task and not state_manager.current_task.done():
                    state_manager.add_log("Cancelling previous task for new request...", "warning")
                    state_manager.current_task.cancel()
                    try:
                        await state_manager.current_task
                    except asyncio.CancelledError:
                        pass

                graph = data.get("graph")
                if graph:
                    logger.info(f"Starting execution for {client_ip}")
                    # 任务存入全局状态
                    state_manager.current_task = asyncio.create_task(execute_graph(graph))
                else:
                    await websocket.send_json({"type": "error", "message": "Graph data is empty"})

            elif command == "stop_execution":
                # 只有显式发送 stop 指令才取消任务
                if state_manager.current_task and not state_manager.current_task.done():
                    logger.info("Received Stop Command.")
                    state_manager.current_task.cancel()
                    # executor 会捕获 CancelledError 并广播日志
                else:
                    await websocket.send_json({"type": "log", "message": "No active execution."})

            elif command == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {client_ip}")
        # [核心变更] 断开时只注销，任务继续在后台跑！
        state_manager.unregister_client(websocket)

    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
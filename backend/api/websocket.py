import asyncio
import time
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from core.logger import logger
from core.state_manager import state_manager
from services.executor import execute_graph
from services.dask_service import dask_service

router = APIRouter()


@router.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    # 添加连接超时
    try:
        await asyncio.wait_for(websocket.accept(), timeout=60)
    except asyncio.TimeoutError:
        return

    client_ip = websocket.client.host
    connection_time = time.time()
    logger.info(f"Client connected: {client_ip}")

    # 心跳配置
    HEARTBEAT_INTERVAL = 30  # 秒
    IDLE_TIMEOUT = 3600  # 60分钟无活动断开 - 支持长时间运行的 Cellpose 任务
    last_activity = time.time()

    # 心跳任务
    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({"type": "ping"})
            except Exception:
                break

    heartbeat_task = asyncio.create_task(heartbeat())

    # 1. 连接初始化与历史状态同步
    initialized = False
    try:
        await state_manager.register_client(websocket)

        # 发送 Dask 服务状态
        client = dask_service.get_client()
        if client and websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_json({
                "type": "log",
                "message": f"[System] Dask Cluster Connected: {client.dashboard_link}"
            })
        initialized = True
    except Exception as e:
        logger.warning(f"WebSocket initialization failed for {client_ip}: {e}")
        state_manager.unregister_client(websocket)
        heartbeat_task.cancel()
        return

    # 2. 消息监听主循环（添加超时）
    try:
        while initialized:
            try:
                # 检查是否有任务在运行，如果有则使用较短的超时来定期检查任务状态
                has_running_task = (state_manager.current_task is not None and
                                   not state_manager.current_task.done())
                timeout = IDLE_TIMEOUT if not has_running_task else 30

                data = await asyncio.wait_for(websocket.receive_json(), timeout=timeout)
                last_activity = time.time()
                command = data.get("command")

                if command == "execute_graph":
                    # 处理并发任务：取消旧任务
                    if state_manager.current_task and not state_manager.current_task.done():
                        state_manager.add_log("Interrupting previous workflow...", "warning")
                        state_manager.current_task.cancel()
                        try:
                            await state_manager.current_task
                        except asyncio.CancelledError:
                            pass

                    graph = data.get("graph")
                    if graph:
                        logger.info(f"Executing graph for {client_ip}")
                        state_manager.current_task = asyncio.create_task(execute_graph(graph))
                    else:
                        await websocket.send_json({"type": "error", "message": "Received empty graph"})

                elif command == "stop_execution":
                    if state_manager.current_task and not state_manager.current_task.done():
                        state_manager.current_task.cancel()
                        log_entry = state_manager.add_log("Execution terminated by user.", "warning")
                        await state_manager.broadcast(log_entry)

                elif command == "pong":
                    # 心跳响应，更新活动时间
                    last_activity = time.time()
                    continue

                elif command == "ping":
                    if websocket.client_state == WebSocketState.CONNECTED:
                        await websocket.send_json({"type": "pong"})

            except asyncio.TimeoutError:
                # 检查是否有任务在运行，如果有则继续等待，否则断开
                has_running_task = (state_manager.current_task is not None and
                                   not state_manager.current_task.done())
                if has_running_task:
                    # 有任务在运行，继续等待
                    continue
                else:
                    # 没有任务在运行且超时，断开连接
                    logger.warning(f"Client {client_ip} idle timeout, closing")
                    break
            except Exception as e:
                logger.error(f"WebSocket loop error for {client_ip}: {e}", exc_info=True)
                break

    except WebSocketDisconnect:
        logger.info(f"Client disconnected gracefully: {client_ip}")
    except Exception as e:
        logger.error(f"WebSocket error for {client_ip}: {e}", exc_info=True)
    finally:
        heartbeat_task.cancel()
        state_manager.unregister_client(websocket)
        logger.info(f"Client disconnected: {client_ip}")
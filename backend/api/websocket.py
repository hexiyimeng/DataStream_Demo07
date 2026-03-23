import asyncio
import time
import uuid
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from core.logger import logger
from core.state_manager import state_manager, ExecutionStatus
from core.auth import verify_websocket_token, _mask_token, API_KEYS
from services.executor import execute_graph
from services.dask_service import dask_service

router = APIRouter()


@router.websocket("/ws/run")
async def websocket_endpoint(websocket: WebSocket):
    client_ip = websocket.client.host if websocket.client else "unknown"
    current_execution_id = None  # 当前客户端订阅的 execution_id

    # ========== 鉴权：在 accept() 前完成 ==========
    if API_KEYS is not None:  # 已配置 API_KEYS，需要校验
        token = websocket.query_params.get("token") or websocket.query_params.get("api_key")

        if not verify_websocket_token(token):
            # 校验失败，记录审计日志（脱敏 token）
            masked = _mask_token(token) if token else "(empty)"
            logger.warning(f"WebSocket auth failed: ip={client_ip}, token={masked}")
            # 直接拒绝连接，不 accept
            return
        # 校验成功
        logger.info(f"WebSocket auth success: ip={client_ip}")

    # ========== accept 连接 ==========
    try:
        await asyncio.wait_for(websocket.accept(), timeout=60)
    except asyncio.TimeoutError:
        return

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

    # 1. 连接初始化
    initialized = False
    try:
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
        state_manager.unsubscribe_client(websocket)
        heartbeat_task.cancel()
        return

    # 2. 消息监听主循环
    try:
        while initialized:
            try:
                # 检查当前客户端的 execution 是否在运行
                current_execution_id = state_manager.get_client_execution(websocket)
                has_running_task = False
                if current_execution_id:
                    session = state_manager.get_execution(current_execution_id)
                    if session and session.status in (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING):
                        has_running_task = True

                timeout = IDLE_TIMEOUT if not has_running_task else 30
                data = await asyncio.wait_for(websocket.receive_json(), timeout=timeout)
                last_activity = time.time()
                command = data.get("command")

                if command == "execute_graph":
                    graph = data.get("graph")
                    if not graph:
                        await websocket.send_json({"type": "error", "message": "Received empty graph"})
                        continue

                    # 生成 execution_id（支持前端传入或后端生成）
                    execution_id = data.get("executionId") or uuid.uuid4().hex

                    # 创建 execution session 并订阅客户端
                    state_manager.create_execution(execution_id)
                    state_manager.subscribe_client(execution_id, websocket)
                    current_execution_id = execution_id

                    logger.info(f"Executing graph for {client_ip}, execution_id={execution_id}")

                    # 发送 executionId 给前端
                    await websocket.send_json({
                        "type": "execution_started",
                        "executionId": execution_id
                    })

                    # 启动执行任务
                    session = state_manager.get_execution(execution_id)
                    if session:
                        session.task = asyncio.create_task(execute_graph(graph, execution_id))

                elif command == "stop_execution":
                    # 只停止当前客户端的 execution
                    execution_id = state_manager.get_client_execution(websocket)
                    if execution_id:
                        success = state_manager.cancel_execution(execution_id)
                        if success:
                            logger.info(f"Execution {execution_id} cancelled by user {client_ip}")
                            await state_manager.broadcast(execution_id, {
                                "type": "log",
                                "message": "Execution terminated by user.",
                                "executionId": execution_id
                            })
                            state_manager.add_log("Execution terminated by user.", "warning", execution_id=execution_id)
                        else:
                            await websocket.send_json({
                                "type": "error",
                                "message": "Cannot cancel execution (already finished or not found)"
                            })
                    else:
                        await websocket.send_json({
                            "type": "error",
                            "message": "No active execution to stop"
                        })

                elif command == "pong":
                    # 心跳响应，更新活动时间
                    last_activity = time.time()
                    continue

                elif command == "ping":
                    if websocket.client_state == WebSocketState.CONNECTED:
                        await websocket.send_json({"type": "pong"})

                elif command == "subscribe":
                    # 支持客户端订阅特定 execution（用于重连或监听已有 execution）
                    execution_id = data.get("executionId")
                    if execution_id:
                        session = state_manager.get_execution(execution_id)
                        if session:
                            state_manager.subscribe_client(execution_id, websocket)
                            current_execution_id = execution_id
                            # 同步历史状态
                            await state_manager.sync_history_to_client(websocket, execution_id)
                            await websocket.send_json({
                                "type": "subscribed",
                                "executionId": execution_id
                            })
                        else:
                            await websocket.send_json({
                                "type": "error",
                                "message": f"Execution {execution_id} not found"
                            })

            except asyncio.TimeoutError:
                # 检查是否有任务在运行
                current_execution_id = state_manager.get_client_execution(websocket)
                has_running_task = False
                if current_execution_id:
                    session = state_manager.get_execution(current_execution_id)
                    if session and session.status in (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING):
                        has_running_task = True

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
        # 清理心跳任务
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        # 解绑客户端
        state_manager.unsubscribe_client(websocket)
        logger.info(f"Client disconnected: {client_ip}")

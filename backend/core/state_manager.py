import asyncio
import logging
from collections import deque
from typing import Dict, Set, Optional
from starlette.websockets import WebSocketState

logger = logging.getLogger("BrainFlow.State")


class GlobalStateManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalStateManager, cls).__new__(cls)
            cls._instance.init()
        return cls._instance

    def init(self):
        # 核心任务控制
        self.current_task: Optional[asyncio.Task] = None
        self.is_running = False

        # 状态与日志缓存
        self.node_states: Dict[str, Dict] = {}
        self.log_buffer = deque(maxlen=100)

        # 活跃客户端维护
        self.active_websockets: Set = set()

    async def register_client(self, websocket):
        """客户端建立连接时的状态同步逻辑"""
        self.active_websockets.add(websocket)

        try:
            # 1. 批量回放历史日志
            for log_entry in self.log_buffer:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json(log_entry)
                else:
                    return

            # 2. 同步节点进度状态
            for node_id, state in self.node_states.items():
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({
                        "type": "progress",
                        "taskId": node_id,
                        "progress": state['progress'],
                        "message": state['message']
                    })
                else:
                    return

        except Exception as e:
            logger.debug(f"Sync interrupted during registration: {e}")
            self.unregister_client(websocket)

    def unregister_client(self, websocket):
        """移除断开连接的客户端"""
        if websocket in self.active_websockets:
            self.active_websockets.remove(websocket)

    def add_log(self, message, level="info"):
        """记录日志到缓冲区"""
        entry = {"type": "log", "message": message}
        if level in ["error", "success", "warning"]:
            entry["type"] = level

        self.log_buffer.append(entry)
        return entry

    def update_progress(self, node_id, progress, message):
        """更新并缓存节点进度"""
        self.node_states[node_id] = {
            "progress": progress,
            "message": message
        }

    async def broadcast(self, message_dict):
        """向所有活跃客户端发送消息，具备自动清理机制"""
        if not self.active_websockets:
            return

        bad_sockets = []
        for ws in list(self.active_websockets):
            try:
                # 仅在连接处于 CONNECTED 状态时发送数据
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(message_dict)
                else:
                    bad_sockets.append(ws)
            except Exception:
                bad_sockets.append(ws)

        # 清理失效的连接句柄
        for ws in bad_sockets:
            self.unregister_client(ws)

    def clear_state(self):
        """执行新任务前重置运行状态"""
        self.node_states.clear()
        self.log_buffer.clear()
        self.is_running = True


state_manager = GlobalStateManager()
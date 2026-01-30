import asyncio
import logging
import json
from collections import deque
from typing import Dict, Set, Optional

logger = logging.getLogger("BrainFlow.State")


class GlobalStateManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalStateManager, cls).__new__(cls)
            cls._instance.init()
        return cls._instance

    def init(self):
        # 1. 核心任务引用 (防止 disconnect 后丢失)
        self.current_task: Optional[asyncio.Task] = None
        self.is_running = False

        # 2. 状态缓存
        # 记录每个节点的最后已知进度 { "NodeID": {progress: 50, message: "..."} }
        self.node_states: Dict[str, Dict] = {}

        # 3. 日志缓存 (只保留最近 100 条，防止内存无限增长)
        self.log_buffer = deque(maxlen=100)

        # 4. 当前连接的客户端 (用于广播)
        self.active_websockets: Set = set()

    async def register_client(self, websocket):
        """新客户端连接时调用：建立连接并立即回放历史状态"""
        self.active_websockets.add(websocket)
        logger.info(
            f"Client Registered. Syncing {len(self.log_buffer)} logs and {len(self.node_states)} node states...")

        try:
            # === A. 回放历史日志 ===
            for log_entry in self.log_buffer:
                await websocket.send_json(log_entry)

            # === B. 回放节点进度 (让进度条跳变) ===
            for node_id, state in self.node_states.items():
                await websocket.send_json({
                    "type": "progress",
                    "taskId": node_id,
                    "progress": state['progress'],
                    "message": state['message']
                })

            # === C. 同步运行状态 ===
            if self.is_running:
                await websocket.send_json({"type": "log", "message": ">>> Reconnected to active session."})
            elif self.current_task and self.current_task.done():
                await websocket.send_json({"type": "done", "message": "Previous workflow finished."})

        except Exception as e:
            logger.error(f"Error syncing state to new client: {e}")
            self.active_websockets.discard(websocket)

    def unregister_client(self, websocket):
        """客户端断开时调用，只移除列表，不停止任务"""
        if websocket in self.active_websockets:
            self.active_websockets.remove(websocket)

    def add_log(self, message, level="info"):
        """记录日志并存入 buffer，返回消息对象供广播"""
        entry = {"type": "log", "message": message}
        if level == "error":
            entry["type"] = "error"
        elif level == "success":
            entry["type"] = "success"
        elif level == "warning":
            entry["type"] = "warning"

        self.log_buffer.append(entry)
        return entry

    def update_progress(self, node_id, progress, message):
        """更新节点进度缓存"""
        self.node_states[node_id] = {
            "progress": progress,
            "message": message
        }

    async def broadcast(self, message_dict):
        """广播给所有连接的客户端"""
        if not self.active_websockets:
            return

        # 移除已关闭的脏连接
        to_remove = set()
        for ws in self.active_websockets:
            try:
                await ws.send_json(message_dict)
            except Exception:
                to_remove.add(ws)

        self.active_websockets -= to_remove

    def clear_state(self):
        """每次点击 RUN 时清空旧状态，开始新的一轮"""
        self.node_states.clear()
        self.log_buffer.clear()
        # 保留 active_websockets，因为人还在
        self.is_running = True


# 全局单例导出
state_manager = GlobalStateManager()

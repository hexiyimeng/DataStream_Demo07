import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Set, Optional
from starlette.websockets import WebSocketState

logger = logging.getLogger("BrainFlow.State")


# =============================================================================
# Execution 状态枚举
# =============================================================================
class ExecutionStatus:
    RUNNING = "running"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    FAILED = "failed"
    SUCCEEDED = "succeeded"

    # 合法状态转换：{当前状态: [允许转换到的状态]}
    VALID_TRANSITIONS = {
        RUNNING: [CANCELLING, FAILED, SUCCEEDED],
        CANCELLING: [CANCELLED, FAILED],  # CANCELLING 可以直接到 CANCELLED，或异常时到 FAILED
        CANCELLED: [],   # 终态，不可转换
        FAILED: [],      # 终态，不可转换
        SUCCEEDED: [],   # 终态，不可转换
    }

    @classmethod
    def is_finished(cls, status: str) -> bool:
        return status in (cls.CANCELLED, cls.FAILED, cls.SUCCEEDED)

    @classmethod
    def can_transition(cls, from_status: str, to_status: str) -> bool:
        """检查状态转换是否合法"""
        allowed = cls.VALID_TRANSITIONS.get(from_status, [])
        return to_status in allowed


# =============================================================================
# Execution Session：单个执行会话的隔离状态
# =============================================================================
@dataclass
class ExecutionSession:
    execution_id: str
    task: Optional[asyncio.Task] = None
    status: str = ExecutionStatus.RUNNING
    node_states: Dict[str, Dict] = field(default_factory=dict)
    logs: deque = field(default_factory=lambda: deque(maxlen=100))
    subscribers: Set = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None

    def get_log_summary(self) -> dict:
        """获取日志摘要（用于历史记录保留）"""
        return {
            "execution_id": self.execution_id,
            "status": self.status,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "log_count": len(self.logs),
            "has_errors": any(
                entry.get("type") in ("error", "warning")
                for entry in self.logs
            ),
            "node_count": len(self.node_states),
        }


# =============================================================================
# 全局状态管理器：execution_id 维度隔离
# =============================================================================
class GlobalStateManager:
    _instance = None

    # 清理策略配置
    MAX_FINISHED_EXECUTIONS = 10  # 保留最近 N 个已完成
    EXECUTION_TTL_SECONDS = 3600  # TTL 清理（秒）
    MAX_FAILED_EXECUTION_LOGS = 3  # 保留最近 N 个失败 execution 的完整日志

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalStateManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._init_state()

    def _init_state(self):
        # execution_id -> ExecutionSession
        self.executions: Dict[str, ExecutionSession] = {}
        # websocket -> execution_id（客户端当前订阅的 execution）
        self.client_execution_map: Dict = {}
        # 失败 execution 日志保留（用于排障）
        self._failed_execution_logs: deque = deque(maxlen=self.MAX_FAILED_EXECUTION_LOGS)

    # =========================================================================
    # Execution 生命周期管理
    # =========================================================================
    def create_execution(self, execution_id: str) -> ExecutionSession:
        """
        创建 execution session。幂等：已存在且未完成则返回现有 session。
        如果已完成则创建新 session（覆盖旧数据）。
        """
        existing = self.executions.get(execution_id)
        if existing and not ExecutionStatus.is_finished(existing.status):
            logger.debug(f"[StateManager] Execution {execution_id} already running, reusing")
            return existing

        session = ExecutionSession(execution_id=execution_id)
        self.executions[execution_id] = session
        logger.info(f"[StateManager] Created execution session: {execution_id}")
        return session

    def get_execution(self, execution_id: str) -> Optional[ExecutionSession]:
        """获取指定 execution session"""
        return self.executions.get(execution_id)

    def get_client_execution(self, websocket) -> Optional[str]:
        """获取客户端当前订阅的 execution_id"""
        return self.client_execution_map.get(websocket)

    def set_execution_status(self, execution_id: str, status: str) -> bool:
        """
        设置 execution 状态（带状态机校验）。

        Returns:
            是否成功设置（如果状态转换非法则返回 False）
        """
        session = self.executions.get(execution_id)
        if not session:
            logger.warning(f"[StateManager] Cannot set status: execution {execution_id} not found")
            return False

        old_status = session.status

        # 已完成状态不可再变更
        if ExecutionStatus.is_finished(old_status):
            logger.warning(
                f"[StateManager] Execution {execution_id} already finished ({old_status}), "
                f"cannot change to {status}"
            )
            return False

        # 校验状态转换是否合法
        allowed = ExecutionStatus.VALID_TRANSITIONS.get(old_status, [])
        if status not in allowed:
            logger.warning(
                f"[StateManager] Invalid transition: {execution_id} {old_status} -> {status} "
                f"(allowed: {allowed})"
            )
            return False

        # 执行状态转换
        session.status = status
        if ExecutionStatus.is_finished(status):
            session.finished_at = time.time()
        logger.info(f"[StateManager] Execution {execution_id}: {old_status} -> {status}")
        return True

    def cancel_execution(self, execution_id: str) -> bool:
        """
        取消指定 execution。
        先置状态为 cancelling，再 cancel task。
        返回是否成功发起取消。
        """
        session = self.executions.get(execution_id)
        if not session:
            return False
        if ExecutionStatus.is_finished(session.status):
            return False  # 已完成，无法取消

        if session.status == ExecutionStatus.RUNNING:
            session.status = ExecutionStatus.CANCELLING
            logger.info(f"[StateManager] Execution {execution_id} -> cancelling")

            if session.task and not session.task.done():
                session.task.cancel()
            return True
        return False

    # =========================================================================
    # 客户端订阅管理
    # =========================================================================
    def subscribe_client(self, execution_id: str, websocket):
        """
        订阅客户端到指定 execution。
        同时维护 client_execution_map 和 session.subscribers。
        """
        session = self.executions.get(execution_id)
        if not session:
            logger.warning(f"[StateManager] Cannot subscribe: execution {execution_id} not found")
            return False

        # 如果客户端已订阅其他 execution，先解绑
        old_execution_id = self.client_execution_map.get(websocket)
        if old_execution_id and old_execution_id != execution_id:
            self._unsubscribe_from_execution(old_execution_id, websocket)

        # 绑定新 execution
        self.client_execution_map[websocket] = execution_id
        session.subscribers.add(websocket)
        logger.debug(f"[StateManager] Client subscribed to execution {execution_id}")
        return True

    def unsubscribe_client(self, websocket):
        """
        客户端断开时解绑。同时清理 client_execution_map 和 session.subscribers。
        """
        execution_id = self.client_execution_map.pop(websocket, None)
        if execution_id:
            self._unsubscribe_from_execution(execution_id, websocket)

    def _unsubscribe_from_execution(self, execution_id: str, websocket):
        """从指定 execution 的 subscribers 中移除客户端"""
        session = self.executions.get(execution_id)
        if session and websocket in session.subscribers:
            session.subscribers.discard(websocket)
            logger.debug(f"[StateManager] Client unsubscribed from execution {execution_id}")

    # =========================================================================
    # 状态操作（全部按 execution_id 隔离）
    # =========================================================================
    def update_progress(self, node_id: str, progress: int, message: str,
                        execution_id: str = None, progress_type: str = None):
        """更新节点进度"""
        if not execution_id:
            return
        session = self.executions.get(execution_id)
        if session:
            session.node_states[node_id] = {
                "progress": progress,
                "message": message,
                "executionId": execution_id,
                "progressType": progress_type,
            }

    def add_log(self, message: str, level: str = "info", execution_id: str = None) -> dict:
        """添加日志"""
        entry = {"type": "log", "message": message}
        if level in ["error", "success", "warning"]:
            entry["type"] = level
        if execution_id:
            entry["executionId"] = execution_id

        if execution_id:
            session = self.executions.get(execution_id)
            if session:
                session.logs.append(entry)
        return entry

    async def broadcast(self, execution_id: str, message_dict: dict):
        """
        向指定 execution 的订阅者广播消息。
        发送失败的连接自动解绑。
        """
        session = self.executions.get(execution_id)
        if not session or not session.subscribers:
            return

        # 确保 message_dict 包含 executionId
        if "executionId" not in message_dict:
            message_dict["executionId"] = execution_id

        bad_sockets = []
        for ws in list(session.subscribers):
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(message_dict)
                else:
                    bad_sockets.append(ws)
            except Exception:
                bad_sockets.append(ws)

        # 清理失效连接
        for ws in bad_sockets:
            self.unsubscribe_client(ws)

    async def broadcast_to_subscribers(self, websocket, message_dict: dict):
        """向客户端当前订阅的 execution 广播"""
        execution_id = self.client_execution_map.get(websocket)
        if execution_id:
            await self.broadcast(execution_id, message_dict)

    # =========================================================================
    # 历史状态同步（只针对指定 execution）
    # =========================================================================
    async def sync_history_to_client(self, websocket, execution_id: str):
        """向客户端同步指定 execution 的历史日志和状态"""
        session = self.executions.get(execution_id)
        if not session:
            return

        try:
            # 1. 回放历史日志
            for log_entry in session.logs:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json(log_entry)
                else:
                    return

            # 2. 同步节点进度状态
            for node_id, state in session.node_states.items():
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({
                        "type": "progress",
                        "taskId": node_id,
                        "executionId": execution_id,
                        "progressType": state.get('progressType'),
                        "progress": state['progress'],
                        "message": state['message']
                    })
                else:
                    return
        except Exception as e:
            logger.debug(f"[StateManager] Sync interrupted: {e}")
            self.unsubscribe_client(websocket)

    # =========================================================================
    # 清理策略
    # =========================================================================
    def cleanup_old_executions(self):
        """
        清理已完成的旧 execution。
        只清理 finished 状态，不碰 running/cancelling。
        策略：保留最近 N 个 + TTL 过期清理 + 失败日志优先保留。
        """
        now = time.time()
        to_remove = []

        # 收集已完成的 execution
        finished_executions = [
            (eid, session) for eid, session in self.executions.items()
            if ExecutionStatus.is_finished(session.status)
        ]

        # === 失败日志保留：在清理前保存失败 execution 的日志摘要 ===
        for eid, session in finished_executions:
            if session.status == ExecutionStatus.FAILED:
                # 检查是否已在保留列表中
                existing_ids = [s["execution_id"] for s in self._failed_execution_logs]
                if eid not in existing_ids:
                    self._failed_execution_logs.append(session.get_log_summary())
                    logger.info(
                        f"[StateManager] Preserving failed execution log: {eid} "
                        f"(status={session.status}, logs={len(session.logs)})"
                    )

        # TTL 清理
        for eid, session in finished_executions:
            if session.finished_at:
                age = now - session.finished_at
                if age > self.EXECUTION_TTL_SECONDS:
                    to_remove.append(eid)
                    logger.info(f"[StateManager] Cleaning up execution {eid} (TTL expired, age={age:.0f}s)")

        # 数量限制清理（按 finished_at 排序，保留最近的）
        # 但优先保留失败的 execution
        remaining = [s for e, s in finished_executions if e not in to_remove]
        # 排序：失败优先，然后按时间倒序
        remaining.sort(
            key=lambda s: (
                0 if s.status == ExecutionStatus.FAILED else 1,  # 失败优先
                s.finished_at or 0
            ),
            reverse=True
        )

        if len(remaining) > self.MAX_FINISHED_EXECUTIONS:
            for session in remaining[self.MAX_FINISHED_EXECUTIONS:]:
                if session.execution_id not in to_remove:
                    to_remove.append(session.execution_id)
                    logger.info(f"[StateManager] Cleaning up execution {session.execution_id} (exceeds max count)")

        # 执行清理
        cleaned_count = 0
        for eid in to_remove:
            session = self.executions.pop(eid, None)
            if session:
                # 清理 subscribers 的反向映射
                for ws in list(session.subscribers):
                    self.client_execution_map.pop(ws, None)
                # 显式清理 session 内部状态（帮助 GC）
                session.node_states.clear()
                session.logs.clear()
                session.subscribers.clear()
                cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"[StateManager] Cleaned up {cleaned_count} execution(s), {len(self.executions)} remaining")

        # === GPU 显存清理：每次有 execution 结束时都主动清 ===
        # 这会强制释放 Cellpose 模型占用的 VRAM（force_clear 不管 refcount）
        # 如果希望跨 execution 复用模型，应在 main.py 的 startup/shutdown 里手动调用
        try:
            # 延迟导入避免循环依赖
            from nodes.cellpose_node import force_clear_cellpose_model_cache
            cleared = force_clear_cellpose_model_cache()
            if cleared > 0:
                logger.info(f"[StateManager] GPU cache cleared: {cleared} model(s) removed")
        except Exception as e:
            logger.debug(f"[StateManager] GPU cache clear skipped: {e}")

    def get_failed_execution_logs(self) -> list:
        """获取最近失败 execution 的日志摘要（用于排障）"""
        return list(self._failed_execution_logs)

    # =========================================================================
    # 兼容接口（deprecated，逐步迁移）
    # =========================================================================
    @property
    def is_running(self) -> bool:
        """兼容属性：是否有任何 execution 在运行"""
        for session in self.executions.values():
            if session.status in (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING):
                return True
        return False

    @property
    def current_task(self) -> Optional[asyncio.Task]:
        """兼容属性：返回最近一个未完成的 task（不推荐使用）"""
        for session in reversed(list(self.executions.values())):
            if session.task and not session.task.done():
                return session.task
        return None

    @property
    def active_websockets(self) -> Set:
        """兼容属性：所有已绑定 execution 的客户端"""
        return set(self.client_execution_map.keys())

    @property
    def log_buffer(self) -> deque:
        """兼容属性：返回最近一个 execution 的日志（不推荐使用）"""
        for session in reversed(list(self.executions.values())):
            return session.logs
        return deque(maxlen=100)

    @property
    def node_states(self) -> Dict:
        """兼容属性：返回最近一个 execution 的节点状态（不推荐使用）"""
        for session in reversed(list(self.executions.values())):
            return session.node_states
        return {}

    def clear_state(self, execution_id: str = None):
        """清理指定 execution 的运行状态"""
        if execution_id:
            session = self.executions.get(execution_id)
            if session:
                session.node_states.clear()
                session.logs.clear()
        else:
            # 兼容旧调用：清理所有
            self.executions.clear()
            self.client_execution_map.clear()


state_manager = GlobalStateManager()

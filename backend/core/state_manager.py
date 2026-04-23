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
        CANCELLING: [CANCELLED, FAILED],
        CANCELLED: [],
        FAILED: [],
        SUCCEEDED: [],
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

    MAX_FINISHED_EXECUTIONS = 10
    EXECUTION_TTL_SECONDS = 3600
    MAX_FAILED_EXECUTION_LOGS = 3

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
        self.executions: Dict[str, ExecutionSession] = {}
        self.client_execution_map: Dict = {}
        self._failed_execution_logs: deque = deque(maxlen=self.MAX_FAILED_EXECUTION_LOGS)

    # =========================================================================
    # Execution 生命周期管理
    # =========================================================================
    def create_execution(self, execution_id: str) -> ExecutionSession:
        existing = self.executions.get(execution_id)
        if existing and not ExecutionStatus.is_finished(existing.status):
            logger.debug(f"[StateManager] Execution {execution_id} already running, reusing")
            return existing

        session = ExecutionSession(execution_id=execution_id)
        self.executions[execution_id] = session
        logger.info(f"[StateManager] Created execution session: {execution_id}")
        return session

    def get_execution(self, execution_id: str) -> Optional[ExecutionSession]:
        return self.executions.get(execution_id)

    def get_client_execution(self, websocket) -> Optional[str]:
        return self.client_execution_map.get(websocket)

    def set_execution_status(self, execution_id: str, status: str) -> bool:
        session = self.executions.get(execution_id)
        if not session:
            logger.warning(f"[StateManager] Cannot set status: execution {execution_id} not found")
            return False

        old_status = session.status

        if ExecutionStatus.is_finished(old_status):
            logger.warning(
                f"[StateManager] Execution {execution_id} already finished ({old_status}), "
                f"cannot change to {status}"
            )
            return False

        allowed = ExecutionStatus.VALID_TRANSITIONS.get(old_status, [])
        if status not in allowed:
            logger.warning(
                f"[StateManager] Invalid transition: {execution_id} {old_status} -> {status} "
                f"(allowed: {allowed})"
            )
            return False

        session.status = status
        if ExecutionStatus.is_finished(status):
            session.finished_at = time.time()
        logger.info(f"[StateManager] Execution {execution_id}: {old_status} -> {status}")
        return True

    def cancel_execution(self, execution_id: str) -> bool:
        session = self.executions.get(execution_id)
        if not session:
            return False
        if ExecutionStatus.is_finished(session.status):
            return False

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
        session = self.executions.get(execution_id)
        if not session:
            logger.warning(f"[StateManager] Cannot subscribe: execution {execution_id} not found")
            return False

        old_execution_id = self.client_execution_map.get(websocket)
        if old_execution_id and old_execution_id != execution_id:
            self._unsubscribe_from_execution(old_execution_id, websocket)

        self.client_execution_map[websocket] = execution_id
        session.subscribers.add(websocket)
        logger.debug(f"[StateManager] Client subscribed to execution {execution_id}")
        return True

    def unsubscribe_client(self, websocket):
        execution_id = self.client_execution_map.pop(websocket, None)
        if execution_id:
            self._unsubscribe_from_execution(execution_id, websocket)

    def _unsubscribe_from_execution(self, execution_id: str, websocket):
        session = self.executions.get(execution_id)
        if session and websocket in session.subscribers:
            session.subscribers.discard(websocket)
            logger.debug(f"[StateManager] Client unsubscribed from execution {execution_id}")

    # =========================================================================
    # 状态操作（全部按 execution_id 隔离）
    # =========================================================================
    def update_progress(self, node_id: str, progress: int, message: str,
                        execution_id: str = None, progress_type: str = None,
                        run_state: str = None, device: str = None, waiting_for: str = None,
                        progress_role: str = None, extra: dict = None):
        """更新节点进度（支持完整运行态字段）"""
        if not execution_id:
            return
        session = self.executions.get(execution_id)
        if session:
            state = {
                "progress": progress,
                "message": message,
                "executionId": execution_id,
                "progressType": progress_type,
            }
            if run_state is not None:
                state["runState"] = run_state
            if device is not None:
                state["device"] = device
            if waiting_for is not None:
                state["waitingFor"] = waiting_for
            if progress_role is not None:
                state["progressRole"] = progress_role
            # 合并 extra 中的所有额外字段（totalChunks, processedChunks 等）
            if extra:
                state.update(extra)
            session.node_states[node_id] = state

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
        session = self.executions.get(execution_id)
        if not session or not session.subscribers:
            return

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

        for ws in bad_sockets:
            self.unsubscribe_client(ws)

    async def broadcast_to_subscribers(self, websocket, message_dict: dict):
        execution_id = self.client_execution_map.get(websocket)
        if execution_id:
            await self.broadcast(execution_id, message_dict)

    # =========================================================================
    # 历史状态同步（只针对指定 execution）
    # =========================================================================
    def get_execution_snapshot(self, execution_id: str) -> dict | None:
        """
        获取指定 execution 的全量状态快照，用于重连恢复。
        聚合所有节点的 chunk 统计，优先使用 sink 节点（chunk_sink/stage_sink）的数据。
        """
        session = self.executions.get(execution_id)
        if not session:
            return None

        # 聚合 chunk 统计：从 node_states 中收集
        total_chunks = 0
        processed_chunks = 0
        completed_inference_chunks = 0
        skipped_chunks = 0
        failed_chunks = 0
        has_sink_data = False

        # 优先收集 sink 节点的统计（chunk_sink / stage_sink）
        for node_id, state in session.node_states.items():
            tc = state.get('totalChunks', 0) or 0
            pc = state.get('processedChunks', 0) or 0
            inf = state.get('completedInferenceChunks', 0) or 0
            skip = state.get('skippedChunks', 0) or 0
            fail = state.get('failedChunks', 0) or 0
            role = state.get('progressRole', '')

            if role in ('chunk_sink', 'stage_sink') and tc > 0:
                if tc > total_chunks:
                    total_chunks = tc
                if pc > processed_chunks:
                    processed_chunks = pc
                if inf > completed_inference_chunks:
                    completed_inference_chunks = inf
                if skip > skipped_chunks:
                    skipped_chunks = skip
                if fail > failed_chunks:
                    failed_chunks = fail
                has_sink_data = True
            elif not has_sink_data:
                if tc > 0:
                    total_chunks = max(total_chunks, tc)
                if pc > 0:
                    processed_chunks = max(processed_chunks, pc)
                if inf > 0:
                    completed_inference_chunks = max(completed_inference_chunks, inf)
                if skip > 0:
                    skipped_chunks = max(skipped_chunks, skip)
                if fail > 0:
                    failed_chunks = max(failed_chunks, fail)

        return {
            "type": "execution_snapshot",
            "executionId": execution_id,
            "status": session.status,
            "createdAt": session.created_at,
            "finishedAt": session.finished_at,
            "nodeCount": len(session.node_states),
            "logCount": len(session.logs),
            "totalChunks": total_chunks,
            "processedChunks": processed_chunks,
            "completedInferenceChunks": completed_inference_chunks,
            "skippedChunks": skipped_chunks,
            "failedChunks": failed_chunks,
        }

    async def sync_history_to_client(self, websocket, execution_id: str):
        """
        向客户端同步指定 execution 的历史日志和状态。
        顺序：1. execution_snapshot  2. 历史 logs  3. node progress states
        """
        session = self.executions.get(execution_id)
        if not session:
            return

        try:
            # 0. 先发送 execution_snapshot
            snapshot = self.get_execution_snapshot(execution_id)
            if snapshot and websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json(snapshot)
            elif not snapshot:
                return

            # 1. 回放历史日志
            for log_entry in session.logs:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json(log_entry)
                else:
                    return

            # 2. 同步节点进度状态（包含完整运行态字段）
            for node_id, state in session.node_states.items():
                if websocket.client_state == WebSocketState.CONNECTED:
                    sync_msg = {
                        "type": "progress",
                        "taskId": node_id,
                        "executionId": execution_id,
                        "progressType": state.get('progressType'),
                        "progress": state['progress'],
                        "message": state['message'],
                    }
                    if 'runState' in state:
                        sync_msg['runState'] = state['runState']
                    if 'device' in state:
                        sync_msg['device'] = state['device']
                    if 'waitingFor' in state:
                        sync_msg['waitingFor'] = state['waitingFor']
                    if 'progressRole' in state:
                        sync_msg['progressRole'] = state['progressRole']
                    if 'totalChunks' in state:
                        sync_msg['totalChunks'] = state['totalChunks']
                    if 'processedChunks' in state:
                        sync_msg['processedChunks'] = state['processedChunks']
                    if 'completedInferenceChunks' in state:
                        sync_msg['completedInferenceChunks'] = state['completedInferenceChunks']
                    if 'skippedChunks' in state:
                        sync_msg['skippedChunks'] = state['skippedChunks']
                    if 'failedChunks' in state:
                        sync_msg['failedChunks'] = state['failedChunks']
                    await websocket.send_json(sync_msg)
                else:
                    return
        except Exception as e:
            logger.debug(f"[StateManager] Sync interrupted: {e}")
            self.unsubscribe_client(websocket)

    # =========================================================================
    # 清理策略
    # =========================================================================
    def cleanup_old_executions(self):
        now = time.time()
        to_remove = []

        finished_executions = [
            (eid, session) for eid, session in self.executions.items()
            if ExecutionStatus.is_finished(session.status)
        ]

        for eid, session in finished_executions:
            if session.status == ExecutionStatus.FAILED:
                existing_ids = [s["execution_id"] for s in self._failed_execution_logs]
                if eid not in existing_ids:
                    self._failed_execution_logs.append(session.get_log_summary())
                    logger.info(
                        f"[StateManager] Preserving failed execution log: {eid} "
                        f"(status={session.status}, logs={len(session.logs)})"
                    )

        for eid, session in finished_executions:
            if session.finished_at:
                age = now - session.finished_at
                if age > self.EXECUTION_TTL_SECONDS:
                    to_remove.append(eid)
                    logger.info(f"[StateManager] Cleaning up execution {eid} (TTL expired, age={age:.0f}s)")

        remaining = [s for e, s in finished_executions if e not in to_remove]
        remaining.sort(
            key=lambda s: (
                0 if s.status == ExecutionStatus.FAILED else 1,
                s.finished_at or 0
            ),
            reverse=True
        )

        if len(remaining) > self.MAX_FINISHED_EXECUTIONS:
            for session in remaining[self.MAX_FINISHED_EXECUTIONS:]:
                if session.execution_id not in to_remove:
                    to_remove.append(session.execution_id)
                    logger.info(f"[StateManager] Cleaning up execution {session.execution_id} (exceeds max count)")

        cleaned_count = 0
        for eid in to_remove:
            session = self.executions.pop(eid, None)
            if session:
                for ws in list(session.subscribers):
                    self.client_execution_map.pop(ws, None)
                session.node_states.clear()
                session.logs.clear()
                session.subscribers.clear()
                cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"[StateManager] Cleaned up {cleaned_count} execution(s), {len(self.executions)} remaining")

        try:
            from nodes.cellpose_node import clear_cellpose_model_cache
            cleared = clear_cellpose_model_cache()
            if cleared:
                logger.info(f"[StateManager] GPU cache cleared via clear_if_safe(): {cleared}")
        except Exception as e:
            logger.debug(f"[StateManager] GPU cache clear_if_safe() skipped: {e}")

    def get_failed_execution_logs(self) -> list:
        return list(self._failed_execution_logs)

    # =========================================================================
    # 兼容接口（deprecated，逐步迁移）
    # =========================================================================
    @property
    def is_running(self) -> bool:
        for session in self.executions.values():
            if session.status in (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING):
                return True
        return False

    @property
    def current_task(self) -> Optional[asyncio.Task]:
        for session in reversed(list(self.executions.values())):
            if session.task and not session.task.done():
                return session.task
        return None

    @property
    def active_websockets(self) -> Set:
        return set(self.client_execution_map.keys())

    @property
    def log_buffer(self) -> deque:
        for session in reversed(list(self.executions.values())):
            return session.logs
        return deque(maxlen=100)

    @property
    def node_states(self) -> Dict:
        for session in reversed(list(self.executions.values())):
            return session.node_states
        return {}

    def clear_state(self, execution_id: str = None):
        if execution_id:
            session = self.executions.get(execution_id)
            if session:
                session.node_states.clear()
                session.logs.clear()
        else:
            self.executions.clear()
            self.client_execution_map.clear()


state_manager = GlobalStateManager()

import { useState, useEffect, useRef, useCallback, useReducer } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, WSMessage, NodeSpec, LogEntry, ExecutionRuntimeState, ExecutionPhase, WebSocketStatus, RunState } from '../types';

// === Progress buffer entry (full runtime state) ===
interface ProgressBufferEntry {
  progress: number | null;
  progressType: 'chunk_count' | 'state_only' | 'stage_based';
  message: string;
  runState?: RunState;
  progressRole?: string;
  waitingFor?: string[];
  device?: string;
  totalChunks?: number;
  processedChunks?: number;
  completedInferenceChunks?: number;
  skippedChunks?: number;
  failedChunks?: number;
}

// === Execution state reducer ===
type ExecutionAction =
  | { type: 'START'; executionId: string }
  | { type: 'SET_PHASE'; phase: ExecutionPhase }
  | { type: 'SET_SNAPSHOT'; snapshot: Partial<ExecutionRuntimeState> }
  | { type: 'FINISH'; status: 'succeeded' | 'failed' | 'cancelled'; message?: string }
  | { type: 'RESET' };

const initialExecutionState: ExecutionRuntimeState = {
  phase: 'idle',
  executionId: null,
  startedAt: null,
  finishedAt: null,
  totalNodes: 0,
  lastError: null,
  totalChunks: 0,
  processedChunks: 0,
  completedInferenceChunks: 0,
  skippedChunks: 0,
  failedChunks: 0,
};

function executionReducer(state: ExecutionRuntimeState, action: ExecutionAction): ExecutionRuntimeState {
  switch (action.type) {
    case 'START':
      return {
        ...state,
        phase: 'graph_building',
        executionId: action.executionId,
        startedAt: Date.now(),
        finishedAt: null,
        lastError: null,
      };
    case 'SET_PHASE':
      return { ...state, phase: action.phase };
    case 'SET_SNAPSHOT':
      return { ...state, ...action.snapshot };
    case 'FINISH':
      return {
        ...state,
        phase: action.status,
        finishedAt: Date.now(),
      };
    case 'RESET':
      return { ...initialExecutionState };
    default:
      return state;
  }
}

// ============================================================
// useFlowEngine — 单连接模型
//
// WebSocket 生命周期完全由 useEffect (deps=[]) 控制：
//   - 挂载 → 建连
//   - 卸载 → 清理
//   - 网络断 → onclose 安排重连
//   - 普通 state 更新（executionState / nodes / logs）→ 绝不触发重连
//
// 所有消息处理函数通过 ref 访问最新状态，不通过闭包捕获变化的值。
// ============================================================
export const useFlowEngine = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  addLog: (msg: string, type?: LogEntry['type']) => void
) => {
  // --- Connection state ---
  const [websocketStatus, setWebsocketStatus] = useState<WebSocketStatus>('disconnected');
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});

  // --- Execution state (reducer) ---
  const [executionState, dispatchExecution] = useReducer(executionReducer, initialExecutionState);

  // --- Refs ---
  const wsRef = useRef<WebSocket | null>(null);
  const progressBufferRef = useRef<Map<string, ProgressBufferEntry>>(new Map());
  const tickIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const mountedRef = useRef(true);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const finishedRef = useRef(false);
  const restoredExecutionRef = useRef(false); // tracks if current session was restored via subscribe

  // ============================================================
  // 1. 获取节点定义（只在 mount 时）
  // ============================================================
  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'}/object_info`)
      .then(res => res.json())
      .then(setNodeDefs)
      .catch(err => addLog(`API Error: ${err}`, 'error'));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // intentionally empty — addLog is only called on error path, stable identity

  // ============================================================
  // 2. 断线清理：停止连线动画
  // ============================================================
  useEffect(() => {
    if (websocketStatus === 'disconnected') {
      setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
    }
  }, [websocketStatus, setEdges]);

  // ============================================================
  // 3. Tick Loop：批量将 progress buffer 写回节点
  // ============================================================
  useEffect(() => {
    const tick = setInterval(() => {
      if (progressBufferRef.current.size === 0) return;

      const updates = new Map(progressBufferRef.current);
      progressBufferRef.current.clear();

      setNodes(nds => nds.map(n => {
        if (!updates.has(n.id)) return n;
        const data = updates.get(n.id)!;

        const runStateChanged = n.data.runState !== data.runState;
        const progressChanged = n.data.progress !== data.progress;
        const messageChanged = n.data.message !== data.message;
        if (!runStateChanged && !progressChanged && !messageChanged) return n;

        return {
          ...n,
          className: data.progressType === 'chunk_count'
            && data.progress !== null
            && data.progress > 0
            && data.progress < 100
            ? 'node-running-pulse'
            : '',
          data: {
            ...n.data,
            progress: data.progress ?? 0,
            message: data.message ?? '',
            runState: data.runState,
            progressRole: data.progressRole as NodeData['progressRole'],
            waitingFor: data.waitingFor,
            device: data.device,
            totalChunks: data.totalChunks,
            processedChunks: data.processedChunks,
            completedInferenceChunks: data.completedInferenceChunks,
            skippedChunks: data.skippedChunks,
            failedChunks: data.failedChunks,
          }
        };
      }));
    }, 100);
    tickIntervalRef.current = tick;
    return () => {
      if (tickIntervalRef.current) clearInterval(tickIntervalRef.current);
    };
    // setNodes has stable identity — never causes re-run
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [setNodes]);

  // ============================================================
  // 4. WebSocket 单连接生命周期（deps=[]，真正的单例）
  //
  // 所有消息处理逻辑内联在 onmessage handler 中，
  // 通过 dispatchExecution / progressBufferRef / addLog ref 操作状态，
  // 不依赖外部闭包中的可变值。
  // ============================================================
  useEffect(() => {
    console.log('[useFlowEngine] connect effect run');
    mountedRef.current = true;

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const connectWs = () => {
      if (!mountedRef.current) {
        console.log('[useFlowEngine] connectWs: mountedRef=false, skip');
        return;
      }

      // 防止并发：已有 open/connecting 的 socket 不重复建连
      const existing = wsRef.current;
      if (existing && existing.readyState === WebSocket.OPEN || existing?.readyState === WebSocket.CONNECTING) {
        console.log('[useFlowEngine] connectWs: already connecting/open, skip');
        return;
      }

      clearReconnectTimer();
      setWebsocketStatus('reconnecting');

      const wsUrl = `${import.meta.env.VITE_WS_URL || 'ws://localhost:8000'}/ws/run`;
      console.log('[useFlowEngine] connecting to', wsUrl);
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!mountedRef.current) { ws.close(); return; }
        console.log('[useFlowEngine] ws open');
        setWebsocketStatus('connected');
        addLog('System Connected', 'success');

        // 重连恢复
        const storedExecutionId = sessionStorage.getItem('brainflow_execution_id');
        if (storedExecutionId) {
          restoredExecutionRef.current = true;
          ws.send(JSON.stringify({ command: 'subscribe', executionId: storedExecutionId }));
          addLog(`Attempting to restore execution ${storedExecutionId}...`, 'info');
        }
      };

      ws.onclose = (event) => {
        console.log('[useFlowEngine] ws close', event.code, event.reason);
        if (!mountedRef.current) return;
        setWebsocketStatus('disconnected');
        wsRef.current = null;

        // 网络断开时标记 execution 为 failed（重连成功后可恢复）
        const reconnecting = event.code !== 1000; // 1000 = intentional close
        if (reconnecting) {
          dispatchExecution({ type: 'FINISH', status: 'failed', message: 'Connection lost. Reconnecting...' });
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          console.log('[useFlowEngine] scheduling reconnect in 1s');
          reconnectTimerRef.current = setTimeout(connectWs, 1000);
        }
      };

      ws.onerror = (event) => {
        console.log('[useFlowEngine] ws error', event);
      };

      // ========================================================
      // 消息处理（全部内联，通过 ref 访问最新状态）
      // 不依赖外部闭包中的 executionState / nodes 等可变值
      // ========================================================
      ws.onmessage = (e) => {
        let msg: WSMessage;
        try {
          msg = JSON.parse(e.data);
        } catch {
          console.error('[useFlowEngine] parse error', e.data);
          return;
        }

        const msgType = msg.type;

        // 日志类消息
        if (msgType === 'log') { addLog(msg.message || '', 'info'); return; }
        if (msgType === 'success') { addLog(msg.message || '', 'success'); return; }
        if (msgType === 'warning') { addLog(msg.message || '', 'warning'); return; }

        // error 类型（非 execution_finished）
        if (msgType === 'error') {
          const msgText = msg.message || '';
          if (msgText.includes('not found')) {
            sessionStorage.removeItem('brainflow_execution_id');
            addLog('Old execution expired (project restarted). Starting fresh.', 'warning');
          }
          progressBufferRef.current.clear();
          addLog(msgText || 'Unknown Error', 'error');
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          return;
        }

        // execution_started
        if (msgType === 'execution_started') {
          if (!msg.executionId) return;
          finishedRef.current = false;
          sessionStorage.setItem('brainflow_execution_id', msg.executionId);
          dispatchExecution({ type: 'START', executionId: msg.executionId });
          dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          return;
        }

        // execution_snapshot（重连恢复）
        if (msgType === 'execution_snapshot') {
          dispatchExecution({
            type: 'SET_SNAPSHOT',
            snapshot: {
              phase: msg.status === 'running' ? 'running'
                : msg.status === 'cancelling' ? 'cancelling'
                : msg.status === 'succeeded' ? 'succeeded'
                : msg.status === 'failed' ? 'failed'
                : msg.status === 'cancelled' ? 'cancelled'
                : 'idle',
              executionId: msg.executionId ?? null,
              startedAt: msg.createdAt ?? null,
              finishedAt: msg.finishedAt ?? null,
              totalNodes: msg.nodeCount ?? 0,
              totalChunks: msg.totalChunks ?? 0,
              processedChunks: msg.processedChunks ?? 0,
              completedInferenceChunks: msg.completedInferenceChunks ?? 0,
              skippedChunks: msg.skippedChunks ?? 0,
              failedChunks: msg.failedChunks ?? 0,
            }
          });

          // 重连恢复时：如果 execution 还在 running，立即恢复连线动画
          if (msg.status === 'running') {
            setEdges(eds => eds.map(e => ({ ...e, animated: true })));
          }
          return;
        }

        // progress
        if (msgType === 'progress') {
          if (!msg.taskId) return;
          progressBufferRef.current.set(msg.taskId, {
            progress: msg.progress ?? null,
            progressType: msg.progressType ?? 'state_only',
            message: msg.message ?? '',
            runState: msg.runState,
            progressRole: msg.progressRole,
            waitingFor: msg.waitingFor,
            device: msg.device,
            totalChunks: msg.totalChunks,
            processedChunks: msg.processedChunks,
            completedInferenceChunks: msg.completedInferenceChunks,
            skippedChunks: msg.skippedChunks,
            failedChunks: msg.failedChunks,
          });

          // 更新全局 phase
          if (msg.runState === 'submitted') {
            dispatchExecution({ type: 'SET_PHASE', phase: 'submitted' });
          } else if (msg.runState === 'running') {
            dispatchExecution({ type: 'SET_PHASE', phase: 'running' });
          }

          // 实时聚合 chunk 统计
          if (msg.totalChunks !== undefined && msg.totalChunks > 0) {
            const isSink = msg.progressRole === 'chunk_sink' || msg.progressRole === 'stage_sink';
            if (isSink) {
              dispatchExecution({
                type: 'SET_SNAPSHOT',
                snapshot: {
                  totalChunks: msg.totalChunks,
                  processedChunks: msg.processedChunks ?? 0,
                  completedInferenceChunks: msg.completedInferenceChunks ?? 0,
                  skippedChunks: msg.skippedChunks ?? 0,
                  failedChunks: msg.failedChunks ?? 0,
                }
              });
            }
          }

          if (msg.message?.toLowerCase().includes('error')) {
            addLog(`[${msg.taskId}] ${msg.message}`, 'error');
          }
          return;
        }

        // execution_finished（唯一权威终态）
        if (msgType === 'execution_finished') {
          if (finishedRef.current) return;
          finishedRef.current = true;

          // cancelling 不是终态，只作为中间状态，不 dispatch FINISH
          if (msg.status === 'cancelling') {
            return;
          }

          const status = (msg.status === 'running' ? 'succeeded' : msg.status) ?? 'succeeded';
          sessionStorage.removeItem('brainflow_execution_id');
          dispatchExecution({ type: 'FINISH', status, message: msg.message });

          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));

          const logType: LogEntry['type'] = status === 'failed' || status === 'cancelled' ? 'error' : 'success';
          addLog(msg.message || `Execution ${status}`, logType);
          progressBufferRef.current.clear();
          return;
        }

        // LEGACY: done（succeeded 兼容）
        if (msgType === 'done') {
          if (finishedRef.current) return;
          finishedRef.current = true;
          sessionStorage.removeItem('brainflow_execution_id');
          dispatchExecution({ type: 'FINISH', status: 'succeeded', message: msg.message });
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          addLog(msg.message || 'Execution succeeded', 'success');
          progressBufferRef.current.clear();
          return;
        }

        // LEGACY: error（非 execution_finished 的 error）
        // @ts-expect-error - legacy compatibility path, msgType 'error' is a valid type but shouldn't reach here in normal flow
        if (msgType === 'error' && !finishedRef.current) {
          // 这个分支已经在上面处理了，保留给 legacy 兼容
          const status = msg.status === 'cancelled' ? 'cancelled' : 'failed';
          finishedRef.current = true;
          sessionStorage.removeItem('brainflow_execution_id');
          dispatchExecution({ type: 'FINISH', status, message: msg.message });
          setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
          addLog(msg.message || `Execution ${status}`, 'error');
          progressBufferRef.current.clear();
          return;
        }

        // subscribed
        if (msgType === 'subscribed') {
          if (msg.executionId) {
            addLog(`Restored execution ${msg.executionId}`, 'success');
            // 如果是重连恢复的 execution，立即恢复连线动画
            if (restoredExecutionRef.current) {
              restoredExecutionRef.current = false;
              setEdges(eds => eds.map(e => ({ ...e, animated: true })));
            }
          }
          return;
        }

        // execution_control_ack（stop 响应）
        if (msgType === 'execution_control_ack') {
          if (msg.message) {
            addLog(msg.message, 'warning');
          }
          return;
        }

        // ping / pong
        if (msgType === 'ping' || msgType === 'pong') return;
      };
    };

    connectWs();

    return () => {
      console.log('[useFlowEngine] connect effect cleanup (unmount)');
      mountedRef.current = false;
      clearReconnectTimer();
      if (wsRef.current) {
        wsRef.current.close(1000, 'component unmount');
        wsRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // <-- 关键：只有挂载/卸载触发，无其他依赖

  // =========================================================
  // 执行 (Run)
  // =========================================================
  const runFlow = useCallback(() => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      addLog('Server not connected!', 'error');
      return;
    }
    if (websocketStatus !== 'connected') {
      addLog('Server not connected!', 'error');
      return;
    }

    const executionId = crypto.randomUUID();

    const graph: Record<string, { type: string; inputs: Record<string, unknown> }> = {};
    nodes.forEach(node => {
      const inputs = { ...node.data.values };
      edges.forEach(edge => {
        if (edge.target === node.id && edge.targetHandle) {
          inputs[edge.targetHandle] = [edge.source, parseInt(edge.sourceHandle || '0')];
        }
      });
      graph[node.id] = { type: node.data.opType, inputs };
    });

    setEdges(eds => eds.map(e => ({ ...e, animated: true })));

    setNodes(nds => nds.map(n => ({
      ...n,
      data: {
        ...n.data,
        progress: 0,
        message: 'Pending...',
        runState: 'ready' as RunState,
        progressRole: undefined,
        waitingFor: undefined,
        device: undefined,
        totalChunks: undefined,
        processedChunks: undefined,
        completedInferenceChunks: undefined,
        skippedChunks: undefined,
        failedChunks: undefined,
        executionId,
      }
    })));

    wsRef.current.send(JSON.stringify({ command: 'execute_graph', graph, executionId }));
    addLog('Executing Workflow...', 'info');
  }, [nodes, edges, addLog, setEdges, setNodes, websocketStatus]);

  // =========================================================
  // 停止 (Stop)
  // =========================================================
  const stopFlow = useCallback(() => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;
    dispatchExecution({ type: 'SET_PHASE', phase: 'cancelling' });
    wsRef.current.send(JSON.stringify({ command: 'stop_execution' }));
    setEdges(eds => eds.map(e => e.animated ? { ...e, animated: false } : e));
    addLog('Requesting Stop...', 'warning');
  }, [addLog, setEdges]);

  return {
    websocketStatus,
    nodeDefs,
    executionState,
    runFlow,
    stopFlow,
  };
};

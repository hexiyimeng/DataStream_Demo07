import { useState, useEffect, useRef, useCallback } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, WSMessage, NodeSpec, LogEntry } from '../types';

// WebSocket 通信、缓冲队列 (Tick Loop) 和执行逻辑
// 进度缓冲类型定义
interface ProgressBufferEntry {
  progress: number | null;
  progressType: 'chunk_count' | 'state_only' | 'stage_based';
  message: string;
}

export const useFlowEngine = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  addLog: (msg: string, type?: LogEntry['type']) => void
) => {
  const [isConnected, setIsConnected] = useState(false);
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});

  const wsRef = useRef<WebSocket | null>(null);
  const progressBufferRef = useRef<Map<string, ProgressBufferEntry>>(new Map());
  const tickIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null); // Store interval ID for cleanup
  const manualCloseRef = useRef(false); // 防止 StrictMode 双挂载导致的竞态：cleanup 关闭 WS 后 old onclose 仍会 fire
  const mountedRef = useRef(true); // 防止组件卸载后触发 reconnect
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null); // 统一管理重连定时器

  // 1. 获取节点定义 (Metadata)
  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'}/object_info`)
      .then(res => res.json())
      .then(setNodeDefs)
      .catch(err => addLog(`API Error: ${err}`, 'error'));
  }, [addLog]);

  // =========================================================
  // 断线熔断机制 (Kill Switch) - 仅在真正断开时清理
  // 注意：WebSocket 重连期间保持进度显示，不清零
  // =========================================================
  useEffect(() => {
    if (!isConnected) {
        // 只有在确认连接真正断开（而非重连中）时才清理
        // 停止连线动画
        setEdges(eds => eds.map(e => {
            if (!e.animated) return e;
            return { ...e, animated: false };
        }));
        // 不再清零节点进度！保留最后的执行状态，重连后可继续显示
    }
  }, [isConnected, setEdges, setNodes]);

  // 2. WebSocket 连接管理
  useEffect(() => {
    // 组件卸载时设为 false，阻止所有异步回调
    mountedRef.current = true;

    const clearReconnectTimer = () => {
        if (reconnectTimerRef.current) {
            clearTimeout(reconnectTimerRef.current);
            reconnectTimerRef.current = null;
        }
    };

    const connectWs = () => {
        // 组件已卸载则不再建新连接
        if (!mountedRef.current) return;

        clearReconnectTimer();
        const ws = new WebSocket(`${import.meta.env.VITE_WS_URL || 'ws://localhost:8000'}/ws/run`);

        ws.onopen = () => {
            if (!mountedRef.current) { ws.close(); return; }
            setIsConnected(true);
            addLog("System Connected", 'success');
            manualCloseRef.current = false;

            // 重连恢复：如果 sessionStorage 中有历史 executionId，尝试恢复订阅
            const storedExecutionId = sessionStorage.getItem('brainflow_execution_id');
            if (storedExecutionId) {
                ws.send(JSON.stringify({ command: 'subscribe', executionId: storedExecutionId }));
                addLog(`Attempting to restore execution ${storedExecutionId}...`, 'info');
            }
        };

        ws.onclose = (e) => {
            if (!mountedRef.current) return; // 组件卸载后的关闭，不处理
            setIsConnected(false);
            // manualClose=true 是主动 close，不重连
            // code=1001 是客户端断开（刷新），需要重连
            if (!manualCloseRef.current) {
                addLog("System Disconnected, Reconnecting...", 'warning');
                reconnectTimerRef.current = setTimeout(connectWs, 1000);
            }
        };

        ws.onmessage = (e) => {
          try {
            const msg: WSMessage = JSON.parse(e.data);

            // 日志处理
            if (msg.type === 'log') addLog(msg.message || '', 'info');
            if (msg.type === 'success') addLog(msg.message || '', 'success');
            if (msg.type === 'warning') addLog(msg.message || '', 'warning');

            if (msg.type === 'error') {
                // 如果是 "Execution not found"（项目重启后旧缓存），清除 sessionStorage
                const msgText = msg.message || "";
                if (msgText.includes("not found")) {
                    sessionStorage.removeItem('brainflow_execution_id');
                    addLog(`Old execution expired (project restarted). Starting fresh.`, 'warning');
                }
                progressBufferRef.current.clear();
                addLog(msg.message || "Unknown Error", 'error');
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
            }

            // 进度缓冲 (Buffer) - 区分状态型和百分比型进度
            if (msg.taskId) {
               progressBufferRef.current.set(msg.taskId, {
                 progress: msg.progress ?? null,
                 progressType: msg.progressType ?? 'state_only',
                 message: msg.message ?? ""
               });
               if (msg.message && msg.message.toLowerCase().includes("error")) addLog(`[${msg.taskId}] ${msg.message}`, 'error');
            }

            // execution_started：保存 executionId 到 sessionStorage（刷新页面后可用于恢复）
            if (msg.type === 'execution_started' && msg.executionId) {
                sessionStorage.setItem('brainflow_execution_id', msg.executionId);
            }

            // subscribed：恢复订阅成功
            if (msg.type === 'subscribed' && msg.executionId) {
                addLog(`Restored execution ${msg.executionId}`, 'success');
            }

            // 完成信号（兼容旧前端和新的 execution_finished）
            if (msg.type === 'done' || msg.type === 'execution_finished') {
                sessionStorage.removeItem('brainflow_execution_id');
                progressBufferRef.current.clear();
                const finishMsg = msg.message || (msg.type === 'execution_finished' ? "Workflow Finished" : "Workflow Finished");
                const msgType: LogEntry['type'] = (msg as any).status === 'failed' || (msg as any).status === 'cancelled' ? 'error' : 'success';
                addLog(finishMsg, msgType);
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
                // 只更新正在运行的节点为完成状态，不是所有节点
                setNodes((nds) => nds.map(n => {
                    const wasRunning = (n.data.progress !== undefined && n.data.progress > 0 && n.data.progress < 100);
                    const finalMessage = (msg as any).status === 'failed' ? 'Error' : (msg as any).status === 'cancelled' ? 'Cancelled' : 'Done';
                    return wasRunning
                    ? { ...n, className: '', data: { ...n.data, progress: 100, message: finalMessage } }
                    : n;
                }));
            }
          } catch (err) { console.error('WS Error', err); }
        };
        wsRef.current = ws;
    };
    connectWs();

    // 3. Tick Loop (React 性能优化的关键)
    // 使用 ref 存储间隔 ID 以便在回调中清理
    const tick = setInterval(() => {
      if (progressBufferRef.current.size > 0) {
        const updates = new Map(progressBufferRef.current);
        progressBufferRef.current.clear(); // 清空缓冲

        // 添加日志输出到控制台（调试用）
        updates.forEach((data, nodeId) => {
          const progressStr = data.progress !== null ? `${data.progress}%` : 'N/A (state only)';
          console.log(`[Progress Update] Node ${nodeId}: ${progressStr} (${data.progressType}) - ${data.message}`);
        });

        try {
          setNodes((nds) => nds.map((n) => {
            if (updates.has(n.id)) {
              const updateData = updates.get(n.id);
              // 比较逻辑：处理 progress 可能为 null 的情况
              const oldProgress = n.data.progress;
              const newProgress = updateData?.progress;
              const oldMessage = n.data.message;
              const newMessage = updateData?.message;

              // 检查是否有实际变化
              const progressChanged = oldProgress !== newProgress;
              const messageChanged = oldMessage !== newMessage;

              if (!progressChanged && !messageChanged) return n;

              // 添加调试日志
              console.log(`[useFlowEngine] Updating node ${n.id}:`, {
                'oldProgress': oldProgress,
                'newProgress': newProgress,
                'oldMessage': oldMessage,
                'newMessage': newMessage,
                'hasNodeSpec': !!n.data.nodeSpec,
                'progressType': updateData?.progressType
              });

              return {
                  ...n,
                  // 只有 chunk_count 类型且进度在 0-100 之间时才显示呼吸灯
                  // state_only 类型 progress 为 null，不显示呼吸灯
                  className: updateData?.progressType === 'chunk_count' &&
                             updateData.progress !== null &&
                             updateData.progress > 0 &&
                             updateData.progress < 100
                      ? 'node-running-pulse'
                      : '',
                  // 保持原有 nodeSpec，更新 progress（可能为 null）和 message
                  data: {
                    ...n.data,
                    progress: newProgress ?? 0, // null 时使用 0
                    message: newMessage ?? ""
                  }
              };
            }
            return n;
          }));
        } catch (error) {
          console.error('[Tick Loop Error]', error);
        }
      }
    }, 100); // 100ms 刷新率

    tickIntervalRef.current = tick;

    return () => {
      mountedRef.current = false;
      manualCloseRef.current = true;
      clearReconnectTimer();
      wsRef.current?.close();
      if (tickIntervalRef.current) clearInterval(tickIntervalRef.current);
    };
  }, [setNodes, setEdges, addLog]);

  // 4. 执行 (Run)
  const runFlow = useCallback(() => {
    if (!wsRef.current || !isConnected) { addLog("Server not connected!", "error"); return; }

    // 构建计算图
    const graph: Record<string, { type: string; inputs: Record<string, unknown> }> = {};
    nodes.forEach((node) => {
      const inputs = { ...node.data.values };
      edges.forEach((edge) => {
        if (edge.target === node.id && edge.targetHandle) {
             inputs[edge.targetHandle] = [edge.source, parseInt(edge.sourceHandle || "0")];
        }
      });
      graph[node.id] = { type: node.data.opType, inputs };
    });

    setEdges((eds) => eds.map(e => ({ ...e, animated: true })));
    wsRef.current.send(JSON.stringify({ command: 'execute_graph', graph }));
    addLog("Executing Workflow...", 'info');

    // 重置进度
    setNodes((nds) => nds.map(n => ({ ...n, data: { ...n.data, progress: 0, message: 'Pending...' } })));
  }, [nodes, edges, addLog, setEdges, setNodes, isConnected]);

  // 5. 停止 (Stop)
  const stopFlow = useCallback(() => {
    if (!wsRef.current) return;
    wsRef.current.send(JSON.stringify({ command: 'stop_execution' }));
    setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
    addLog("Requesting Stop...", 'warning');
  }, [addLog, setEdges]);

  return { isConnected, nodeDefs, runFlow, stopFlow };
};

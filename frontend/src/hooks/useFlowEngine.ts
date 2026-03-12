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

  // 1. 获取节点定义 (Metadata)
  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'}/object_info`)
      .then(res => res.json())
      .then(setNodeDefs)
      .catch(err => addLog(`API Error: ${err}`, 'error'));
  }, [addLog]);

  // =========================================================
  // 核心修复：断线熔断机制 (Kill Switch)
  // =========================================================
  useEffect(() => {
    if (!isConnected) {
        // 1. 停止连线动画
        setEdges(eds => eds.map(e => {
            if (!e.animated) return e; // 性能优化：本来没动的就别改了
            return { ...e, animated: false };
        }));

        // 2. 强制重置节点状态 (消除僵尸进度条)
        setNodes(nds => nds.map(n => {
            // 只有那些看起来"正在运行"的节点才需要重置
            const isRunningState = (n.data.progress !== undefined && n.data.progress > 0 && n.data.progress < 100) || n.data.message;

            if (isRunningState) {
                return {
                    ...n,
                    className: n.className ? n.className.replace('node-running-pulse', '').trim() : '',
                    data: {
                        ...n.data,
                        progress: 0,            // 进度归零，绿色进度条消失
                        message: 'Disconnected' // 或者设为空字符串 "" 以彻底隐藏
                    }
                };
            }
            return n;
        }));
    }
  }, [isConnected, setEdges, setNodes]);

  // 2. WebSocket 连接管理
  useEffect(() => {
    const connectWs = () => {
        // 只有后端开启时才能连上
        const ws = new WebSocket(`${import.meta.env.VITE_WS_URL || 'ws://localhost:8000'}/ws/run`);

        ws.onopen = () => {
            setIsConnected(true);
            addLog("System Connected", 'success');
        };

        ws.onclose = () => {
            setIsConnected(false);
            addLog("System Disconnected", 'warning');
            setTimeout(connectWs, 3000);
        };

        ws.onmessage = (e) => {
          try {
            const msg: WSMessage = JSON.parse(e.data);

            // 日志处理
            if (msg.type === 'log') addLog(msg.message || '', 'info');
            if (msg.type === 'success') addLog(msg.message || '', 'success');
            if (msg.type === 'warning') addLog(msg.message || '', 'warning');

            if (msg.type === 'error') {
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

            // 完成信号
            if (msg.type === 'done') {
                progressBufferRef.current.clear();
                addLog(msg.message || "Workflow Finished", 'success');
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
                // 只更新正在运行的节点为完成状态，不是所有节点
                setNodes((nds) => nds.map(n => {
                    const wasRunning = n.data.progress > 0 && n.data.progress < 100;
                    return wasRunning
                    ? { ...n, className: '', data: { ...n.data, progress: 100, message: 'Done' } }
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

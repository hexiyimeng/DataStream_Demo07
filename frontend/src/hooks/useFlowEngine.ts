import { useState, useEffect, useRef, useCallback } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, WSMessage, NodeSpec, LogEntry } from '../types';

// WebSocket 通信、缓冲队列 (Tick Loop) 和执行逻辑
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
  const progressBufferRef = useRef<Map<string, { progress: number; message?: string }>>(new Map());

  // 1. 获取节点定义 (Metadata)
  useEffect(() => {
    fetch('http://localhost:8000/object_info')
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
            // 只有那些看起来“正在运行”的节点才需要重置
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
        const ws = new WebSocket('ws://localhost:8000/ws/run');

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

            // 进度缓冲 (Buffer)
            if (msg.taskId) {
               progressBufferRef.current.set(msg.taskId, { progress: msg.progress ?? 0, message: msg.message });
               if (msg.message?.toLowerCase().includes("error")) addLog(`[${msg.taskId}] ${msg.message}`, 'error');
            }

            // 完成信号
            if (msg.type === 'done') {
                progressBufferRef.current.clear();
                addLog(msg.message || "Workflow Finished", 'success');
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
                // 强制刷新所有节点为完成状态
                setNodes((nds) => nds.map(n => ({ ...n, className: '', data: { ...n.data, progress: 100, message: 'Done' } })));
            }
          } catch (err) { console.error('WS Error', err); }
        };
        wsRef.current = ws;
    };
    connectWs();

    // 3. Tick Loop (React 性能优化的关键)
    const tick = setInterval(() => {
      if (progressBufferRef.current.size > 0) {
        const updates = new Map(progressBufferRef.current);
        progressBufferRef.current.clear(); // 清空缓冲

        setNodes((nds) => nds.map((n) => {
          if (updates.has(n.id)) {
            const updateData = updates.get(n.id);
            // 只有数据真的变了才更新，防止 React 渲染抖动
            if (n.data.progress === updateData?.progress && n.data.message === updateData?.message) return n;

            return {
                ...n,
                // 只有进度 < 100 且 > 0 时才显示呼吸灯
                className: updateData && updateData.progress < 100 && updateData.progress > 0
                    ? 'node-running-pulse'
                    : '',
                data: { ...n.data, ...updateData }
            };
          }
          return n;
        }));
      }
    }, 100); // 100ms 刷新率

    return () => { wsRef.current?.close(); clearInterval(tick); };
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
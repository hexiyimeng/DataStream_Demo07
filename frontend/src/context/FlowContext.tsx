import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useNodesState, useEdgesState, addEdge, type Connection, type Node, type Edge } from '@xyflow/react';
import type { NodeSpec, WSMessage, Workflow, LogEntry, NodeData } from '../types';
import { FlowContext } from './FlowContextDef';
import { useUndoRedo } from '../hooks/useUndoRedo'; // ⚠️ 确保已创建此 Hook

export const FlowProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // === 1. 核心状态管理 ===
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<NodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);

  // 工作流状态
  const [workflows, setWorkflows] = useState<Workflow[]>(() => [
    { id: '1', name: 'Workflow 1', nodes: [], edges: [], timestamp: Date.now() }
  ]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>('1');
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});
  const [isConnected, setIsConnected] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);

  // === 2. 引用与缓冲池 (性能优化核心) ===
  const wsRef = useRef<WebSocket | null>(null);

  // 🔥 [优化] 缓冲池：避免 WebSocket 高频消息导致 React 频繁重绘
  // key: nodeId, value: { progress, message }
  const progressBufferRef = useRef<Map<string, { progress: number; message?: string }>>(new Map());
  const logBufferRef = useRef<LogEntry[]>([]);

  // === 3. Undo/Redo 系统初始化 ===
  // 这里传入 setNodes/setEdges 的包装器，确保类型安全
  const { undo, redo, takeSnapshot, syncCurrentState } = useUndoRedo<BrainFlowNodeData>(
    [], [],
    (nds: Node<BrainFlowNodeData>[]) => setNodes(nds),
    (eds: Edge[]) => setEdges(eds)
  );

  // === 4. 辅助功能 ===
  const toggleConsole = () => setIsConsoleOpen(prev => !prev);
  const toggleTheme = () => setTheme(prev => prev === 'light' ? 'dark' : 'light');

  // 日志添加 (带缓冲)
  const addLog = useCallback((message: string, type: 'info' | 'success' | 'error' = 'info') => {
    logBufferRef.current.push({
      id: Date.now().toString() + Math.random(),
      timestamp: new Date().toLocaleTimeString(),
      type,
      message
    });
  }, []);

  // === 5. WebSocket 连接与心跳循环 ===
  useEffect(() => {
    // 获取节点定义
    fetch('http://localhost:8000/object_info')
      .then(res => res.json())
      .then(setNodeDefs)
      .catch(err => addLog(`API Error: ${err}`, 'error'));

    const connectWs = () => {
        const ws = new WebSocket('ws://localhost:8000/ws/run');

        ws.onopen = () => {
          setIsConnected(true);
          addLog("Server Connected", 'success');
        };

        ws.onclose = () => {
          setIsConnected(false);
          setTimeout(connectWs, 3000);
        };

        ws.onmessage = (e) => {
          try {
            const msg: WSMessage = JSON.parse(e.data);

            if (msg.type === 'log') {
              addLog(msg.message || '', 'info');
            }

            if (msg.type === 'progress' && msg.taskId) {
               // 仅更新缓冲区，不触发重绘
               progressBufferRef.current.set(msg.taskId, {
                 progress: msg.progress ?? 0,
                 message: msg.message
               });

               // 关键状态日志直接输出
               if (msg.message && msg.message !== "Done" && !msg.message.startsWith("Start")) {
                   addLog(`[${msg.taskId.split('_')[0]}] ${msg.message}`, 'info');
               }
            }

            if (msg.type === 'done') addLog("Workflow Finished", 'success');

          } catch (err) {
            console.error('WebSocket message parsing error:', err);
          }
        };
        wsRef.current = ws;
    };

    connectWs();

    // 🔥 [性能核心] 统一刷新循环 (Game Loop 模式)
    // 每 100ms 检查一次缓冲区，如果有数据变化才 setNodes
    const tick = setInterval(() => {
      // 1. 处理进度更新
      if (progressBufferRef.current.size > 0) {
        const updates = new Map(progressBufferRef.current);
        progressBufferRef.current.clear();

        setNodes((nds) => nds.map((n) => {
          if (updates.has(n.id)) {
            const updateData = updates.get(n.id);
            // 浅比较，如果没变化就不返回新对象 (React 优化)
            if (n.data.progress === updateData?.progress && n.data.message === updateData?.message) {
              return n;
            }
            return { ...n, data: { ...n.data, ...updateData } };
          }
          return n;
        }));
      }

      // 2. 处理日志更新
      if (logBufferRef.current.length > 0) {
        const newLogs = [...logBufferRef.current];
        logBufferRef.current = [];

        setLogs(prev => {
          const lastMsg = prev.length > 0 ? prev[prev.length - 1].message : '';
          // 简单去重：如果连续两条日志完全一样，丢弃
          const filtered = newLogs.filter((l, i) =>
             i === 0 ? l.message !== lastMsg : l.message !== newLogs[i-1].message
          );
          if (filtered.length === 0) return prev;
          return [...prev, ...filtered].slice(-100);
        });
      }
    }, 100);

    return () => {
      wsRef.current?.close();
      clearInterval(tick);
    };
  }, [setNodes, addLog]);

  // === 6. 历史记录快照逻辑 (Snapshot) ===
  useEffect(() => {
      // 1. 总是保持当前状态同步给 Hook 内部的 Ref
      syncCurrentState(nodes, edges);

      // 2. 智能快照：过滤掉系统自动产生的更新 (如进度条)
      // 逻辑：如果当前有任何节点处于 "运行中" (0 < progress < 100)，即使 nodes 变了也不记入历史
      const isSystemUpdate = nodes.some(n => n.data.progress !== undefined && n.data.progress > 0 && n.data.progress < 100);

      if (!isSystemUpdate) {
         takeSnapshot();
      }

  }, [nodes, edges, syncCurrentState, takeSnapshot]);

  // === 7. 快捷键监听 (Ctrl+Z / Ctrl+Y) ===
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // 如果焦点在输入框，不触发撤销
      if (['INPUT', 'TEXTAREA'].includes((e.target as HTMLElement).tagName)) return;

      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'z') {
        e.preventDefault();
        if (e.shiftKey) redo(); else undo();
      }
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'y') {
        e.preventDefault();
        redo();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo]);

  // === 8. 主题同步 ===
  useEffect(() => {
    if (theme === 'dark') document.documentElement.classList.add('dark');
    else document.documentElement.classList.remove('dark');
  }, [theme]);

  // === 9. 核心类型校验逻辑 ===
  const isValidConnection = useCallback((connection: Connection | Edge) => {
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);

    if (!sourceNode || !targetNode) return false;

    const sourceSpec = sourceNode.data.nodeSpec;
    const sourceHandleIndex = parseInt(connection.sourceHandle || "0");
    if (!sourceSpec?.output || !sourceSpec.output[sourceHandleIndex]) return false;
    const outputType = sourceSpec.output[sourceHandleIndex];

    const targetSpec = targetNode.data.nodeSpec;
    const targetHandleName = connection.targetHandle;
    if (!targetSpec || !targetHandleName) return false;

    const inputConfig = targetSpec.input?.required?.[targetHandleName] || targetSpec.input?.optional?.[targetHandleName];
    if (!inputConfig) return false;

    const inputType = Array.isArray(inputConfig) ? inputConfig[0] : inputConfig;

    if (inputType === "*" || outputType === "*") return true;
    return outputType === inputType;
  }, [nodes]);

  // === 10. 工作流管理 ===
  const saveCurrentWorkflow = useCallback(() => {
    setWorkflows(prev => prev.map(w => w.id === activeWorkflowId ? { ...w, nodes, edges } : w));
  }, [nodes, edges, activeWorkflowId]);

  useEffect(() => {
    const timer = setTimeout(() => saveCurrentWorkflow(), 500);
    return () => clearTimeout(timer);
  }, [nodes, edges, activeWorkflowId, saveCurrentWorkflow]);

  const switchWorkflow = (id: string) => {
    saveCurrentWorkflow();
    const target = workflows.find(w => w.id === id);
    if (target) { setActiveWorkflowId(id); setNodes(target.nodes || []); setEdges(target.edges || []); }
  };

  const createWorkflow = () => {
    saveCurrentWorkflow();
    const newId = Date.now().toString();
    setWorkflows(prev => [...prev, { id: newId, name: `Workflow ${workflows.length + 1}`, nodes: [], edges: [], timestamp: Date.now() }]);
    setActiveWorkflowId(newId); setNodes([]); setEdges([]);
  };

  const deleteWorkflow = (id: string) => {
    if (workflows.length <= 1) return;
    const newWfs = workflows.filter(w => w.id !== id);
    setWorkflows(newWfs);
    if (activeWorkflowId === id) switchWorkflow(newWfs[0].id);
  };

  const renameWorkflow = (id: string, name: string) => setWorkflows(prev => prev.map(w => w.id === id ? { ...w, name } : w));

  // === 11. 节点操作 ===
  const addNode = useCallback((type: string) => {
    const spec = nodeDefs[type]; if (!spec) return;
    setNodes((nds) => nds.concat({
      id: `${type}_${Date.now()}`,
      type: 'dynamic',
      position: { x: Math.random() * 400 + 200, y: Math.random() * 300 + 100 },
      data: { opType: type, nodeSpec: spec, values: {}, progress: 0, message: "" },
    }));
  }, [nodeDefs, setNodes]);

  const addNodeAt = useCallback((type: string, position: {x: number, y: number}) => {
    const spec = nodeDefs[type]; if (!spec) return;
    setNodes((nds) => nds.concat({
      id: `${type}_${Date.now()}`,
      type: 'dynamic',
      position: position,
      data: { opType: type, nodeSpec: spec, values: {}, progress: 0, message: "" },
    }));
  }, [nodeDefs, setNodes]);

  const updateNodeData = useCallback((id: string, newData: Partial<NodeData>) => {
    setNodes((nds) => nds.map((n) => n.id === id ? { ...n, data: { ...n.data, ...newData } } : n));
  }, [setNodes]);

  const onConnect = useCallback((params: Connection) => {
      // 增强: 连接时校验 + 自动添加
      if (isValidConnection(params)) {
          setEdges((eds) => addEdge({ ...params, animated: true, style: { stroke: '#94a3b8', strokeWidth: 2 } }, eds));
      } else {
          addLog("Invalid Connection: Type Mismatch", "error");
      }
  }, [setEdges, isValidConnection, addLog]);

  const runFlow = useCallback(() => {
    if (!wsRef.current) return;
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

    wsRef.current.send(JSON.stringify({ command: 'execute_graph', graph }));
    addLog("Executing Workflow...", 'info');
  }, [nodes, edges, addLog]);

  const clearLogs = () => setLogs([]);

  return (
    <FlowContext.Provider value={{
      nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
      setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
      addNode, addNodeAt, updateNodeData, runFlow, clearLogs,
      createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
      theme, toggleTheme, isConsoleOpen, toggleConsole, isValidConnection,
      undo, redo // 🔥 暴露撤销重做方法
    }}>
      {children}
    </FlowContext.Provider>
  );
};
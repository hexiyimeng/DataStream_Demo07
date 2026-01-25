import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  useNodesState,
  useEdgesState,
  addEdge,
  type Connection,
  type Node,
  type Edge,
  type OnConnectStart,
  type OnConnectEnd
} from '@xyflow/react';
import type { NodeSpec, WSMessage, Workflow, LogEntry, NodeData } from '../types';
import { FlowContext } from './FlowContextDef';
import { useUndoRedo } from '../hooks/useUndoRedo';

export const FlowProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // === 1. 核心状态管理 ===
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<NodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);

  // 连线交互状态
  const [connectingType, setConnectingType] = useState<string | null>(null);


  useEffect(() => {
    const root = document.documentElement; // 获取 <html> 标签
    if (theme === 'dark') {
      root.classList.add('dark');
    } else {
      root.classList.remove('dark');
    }
  }, [theme]);

  // 剪贴板状态
  const clipboardRef = useRef<{ nodes: Node<NodeData>[], edges: Edge[] } | null>(null);

  // 工作流状态
  const [workflows, setWorkflows] = useState<Workflow[]>(() => [
    { id: '1', name: 'Workflow 1', nodes: [], edges: [], timestamp: Date.now() }
  ]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>('1');
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});
  const [isConnected, setIsConnected] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);

  // === 2. 引用与缓冲池 ===
  const wsRef = useRef<WebSocket | null>(null);
  const progressBufferRef = useRef<Map<string, { progress: number; message?: string }>>(new Map());
  const logBufferRef = useRef<LogEntry[]>([]);

  // === 3. Undo/Redo 系统 ===
  const { undo, redo, takeSnapshot, syncCurrentState } = useUndoRedo<NodeData>(
    [], [],
    (nds: Node<NodeData>[]) => setNodes(nds),
    (eds: Edge[]) => setEdges(eds)
  );

  // === 4. 辅助功能 ===
  const toggleConsole = () => setIsConsoleOpen(prev => !prev);
  const toggleTheme = () => setTheme(prev => prev === 'light' ? 'dark' : 'light');

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

            // 1. 处理系统级日志 (如 "Engine Started")
            if (msg.type === 'log') {
              addLog(msg.message || '', 'info');
            }

            // 2. 处理节点进度/状态 -> [关键：废话过滤]
            if (msg.taskId) {
               // A. 灵动岛状态 (始终更新，保证 UI 上的进度条和文字是活的)
               progressBufferRef.current.set(msg.taskId, {
                 progress: msg.progress ?? 0,
                 message: msg.message
               });

               // B. 日志控制台 (严格过滤)
               // 只有真正的报错、完成或者有意义的信息才进控制台
               const text = msg.message || "";

               // 定义“废话”关键词
               const isSpam =
                  text.includes("Running") ||     // 过滤 "Dask Running..."
                  text.includes("Processing") ||  // 过滤 "Processing..."
                  text.includes("Active") ||      // 过滤 "Active Tasks..."
                  text.includes("[Sys]") ||       // 过滤系统监控
                  text.includes("RAM:");          // 过滤内存信息

               if (text.toLowerCase().includes("error")) {
                   addLog(`[Node Error] ${text}`, 'error');
               }
               // 只有非废话才显示。这样你在 Python 里的 print("读取文件成功") 就能显示出来
               else if (!isSpam && text !== "Done" && text !== "") {
                   addLog(`[${msg.taskId.split('_')[0]}] ${text}`, 'info');
               }
            }

            // 3. 处理全局错误
            if (msg.type === 'error') {
                progressBufferRef.current.clear(); // 🛑 出错时清空缓冲，停止所有节点动画
                addLog(msg.message || "Unknown Error", 'error');
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
            }

            // 4. 完成信号 -> [关键修复：解决线一直在转]
            if (msg.type === 'done') {
                progressBufferRef.current.clear(); // 🛑 核心操作：立即清空进度缓冲
                addLog("Workflow Finished", 'success');
                // 强制关闭所有连线动画
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
            }

          } catch (err) {
            console.error('WebSocket message parsing error:', err);
          }
        };
        wsRef.current = ws;
    };

    connectWs();

    // 定时更新器 (100ms)
    // 这里的逻辑是从缓冲池里取数据更新 React 状态，避免高频渲染
    const tick = setInterval(() => {
      if (progressBufferRef.current.size > 0) {
        const updates = new Map(progressBufferRef.current);
        progressBufferRef.current.clear();

        setNodes((nds) => nds.map((n) => {
          if (updates.has(n.id)) {
            const updateData = updates.get(n.id);
            // 性能优化：只有数据真的变了才触发 React 更新
            if (n.data.progress === updateData?.progress && n.data.message === updateData?.message) {
              return n;
            }
            return { ...n, data: { ...n.data, ...updateData } };
          }
          return n;
        }));
      }

      if (logBufferRef.current.length > 0) {
        const newLogs = [...logBufferRef.current];
        logBufferRef.current = [];

        setLogs(prev => {
          const lastMsg = prev.length > 0 ? prev[prev.length - 1].message : '';
          // 简单的去重逻辑
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

  // === 6. 历史记录快照逻辑 (保持不变) ===
  useEffect(() => {
      syncCurrentState(nodes, edges);
      const isSystemUpdate = nodes.some(n => n.data.progress !== undefined && n.data.progress > 0 && n.data.progress < 100);
      if (!isSystemUpdate) {
         takeSnapshot();
      }
  }, [nodes, edges, syncCurrentState, takeSnapshot]);

  // === 7. 连线感知逻辑 (保持不变) ===
  const onConnectStart: OnConnectStart = useCallback((_, { nodeId, handleId, handleType }) => {
    if (!nodeId || !handleId || handleType !== 'source') return;
    const sourceNode = nodes.find(n => n.id === nodeId);
    if (!sourceNode || !sourceNode.data.nodeSpec) return;
    const typeIndex = parseInt(handleId);
    const outType = sourceNode.data.nodeSpec.output?.[typeIndex];
    if (outType) setConnectingType(outType);
  }, [nodes]);

  const onConnectEnd: OnConnectEnd = useCallback(() => {
    setConnectingType(null);
  }, []);

  // === 8. 剪贴板与删除逻辑 (保持不变) ===
  const handleCopy = useCallback(() => {
    const selectedNodes = nodes.filter(n => n.selected);
    if (selectedNodes.length === 0) return;
    const selectedIds = new Set(selectedNodes.map(n => n.id));
    const selectedEdges = edges.filter(e => selectedIds.has(e.source) && selectedIds.has(e.target));
    clipboardRef.current = { nodes: selectedNodes, edges: selectedEdges };
    addLog(`Copied ${selectedNodes.length} nodes`, 'info');
  }, [nodes, edges, addLog]);

  const handlePaste = useCallback(() => {
    if (!clipboardRef.current) return;
    const { nodes: cpNodes, edges: cpEdges } = clipboardRef.current;
    const idMap = new Map<string, string>();
    const newNodes: Node<NodeData>[] = [];
    const offset = 20 + Math.random() * 30;

    cpNodes.forEach(n => {
       const newId = `${n.data.opType}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
       idMap.set(n.id, newId);
       newNodes.push({
         ...n,
         id: newId,
         position: { x: n.position.x + offset, y: n.position.y + offset },
         selected: true,
         data: { ...n.data, progress: 0, message: '' }
       });
    });

    const newEdges = cpEdges.map(e => ({
      ...e,
      id: `e_${idMap.get(e.source)}-${idMap.get(e.target)}_${Math.random()}`,
      source: idMap.get(e.source)!,
      target: idMap.get(e.target)!,
      selected: true
    }));

    setNodes(nds => nds.map(n => ({ ...n, selected: false } as Node<NodeData>)).concat(newNodes));
    setEdges(eds => eds.map(e => ({ ...e, selected: false } as Edge)).concat(newEdges));
    addLog(`Pasted ${newNodes.length} nodes`, 'info');
  }, [setNodes, setEdges, addLog]);

  const handleDelete = useCallback(() => {
      const selectedNodes = nodes.filter(n => n.selected);
      const selectedEdges = edges.filter(e => e.selected);
      if (selectedNodes.length === 0 && selectedEdges.length === 0) return;
      setNodes(nds => nds.filter(n => !n.selected));
      setEdges(eds => eds.filter(e => !e.selected));
  }, [nodes, edges, setNodes, setEdges]);

  // === 9. 键盘事件监听 (保持不变) ===
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName) || target.isContentEditable) return;
      const isCtrl = e.metaKey || e.ctrlKey;
      if (isCtrl && e.key.toLowerCase() === 'z') { e.preventDefault(); if (e.shiftKey) redo(); else undo(); }
      else if (isCtrl && e.key.toLowerCase() === 'y') { e.preventDefault(); redo(); }
      else if (isCtrl && e.key.toLowerCase() === 'c') { e.preventDefault(); handleCopy(); }
      else if (isCtrl && e.key.toLowerCase() === 'v') { e.preventDefault(); handlePaste(); }
      else if (e.key === 'Delete' || e.key === 'Backspace') { handleDelete(); }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo, handleCopy, handlePaste, handleDelete]);

  // === 10. 类型校验 (保持不变) ===
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

  // === 11. 工作流管理 (保持不变) ===
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

  // === 12. 节点操作 (保持不变) ===
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
      if (isValidConnection(params)) {
          setEdges((eds) => addEdge({ ...params, animated: false, style: { stroke: '#94a3b8', strokeWidth: 2 } }, eds));
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
    setEdges((eds) => eds.map(e => ({ ...e, animated: true })));
    wsRef.current.send(JSON.stringify({ command: 'execute_graph', graph }));
    addLog("Executing Workflow...", 'info');
  }, [nodes, edges, addLog, setEdges]);

  const stopFlow = useCallback(() => {
    if (!wsRef.current) return;
    wsRef.current.send(JSON.stringify({ command: 'stop_execution' }));
    // 立即停止动画，不等后端回传，提升响应速度
    setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
    addLog("Requesting Stop...", 'info');
  }, [addLog, setEdges]);

  const clearLogs = () => setLogs([]);

  return (
    <FlowContext.Provider value={{
      nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
      setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
      addNode, addNodeAt, updateNodeData, runFlow, stopFlow ,clearLogs,
      createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
      theme, toggleTheme, isConsoleOpen, toggleConsole, isValidConnection,
      undo, redo,
      onConnectStart, onConnectEnd, connectingType,
      handleCopy, handlePaste, handleDelete
    }}>
      {children}
    </FlowContext.Provider>
  );
};
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
  // ========================================================================
  // 1. 核心状态管理 (Core State)
  // ========================================================================
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<NodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);

  // 连线交互状态
  const [connectingType, setConnectingType] = useState<string | null>(null);

  // 主题切换副作用
  useEffect(() => {
    const root = document.documentElement;
    if (theme === 'dark') {
      root.classList.add('dark');
    } else {
      root.classList.remove('dark');
    }
  }, [theme]);

  // 剪贴板引用
  const clipboardRef = useRef<{ nodes: Node<NodeData>[], edges: Edge[] } | null>(null);

  // 工作流 (Tabs) 状态
  const [workflows, setWorkflows] = useState<Workflow[]>(() => [
    { id: '1', name: 'Workflow 1', nodes: [], edges: [], timestamp: Date.now() }
  ]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>('1');

  // 节点定义与连接状态
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});
  const [isConnected, setIsConnected] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);

  // ========================================================================
  // 2. 引用与缓冲池 (Refs & Buffers)
  // ========================================================================
  const wsRef = useRef<WebSocket | null>(null);
  // 进度缓冲：避免 WebSocket 高频消息直接触发 React 渲染
  const progressBufferRef = useRef<Map<string, { progress: number; message?: string }>>(new Map());
  // 日志缓冲
  const logBufferRef = useRef<LogEntry[]>([]);

  // ========================================================================
  // 3. Undo/Redo 系统
  // ========================================================================
  const { undo, redo, takeSnapshot, syncCurrentState } = useUndoRedo<NodeData>(
    [], [],
    (nds: Node<NodeData>[]) => setNodes(nds),
    (eds: Edge[]) => setEdges(eds)
  );

  // ========================================================================
  // 4. 辅助功能函数
  // ========================================================================
  const toggleConsole = () => setIsConsoleOpen(prev => !prev);
  const toggleTheme = () => setTheme(prev => prev === 'light' ? 'dark' : 'light');

  const addLog = useCallback((message: string, type: 'info' | 'success' | 'error' | 'warning' = 'info') => {
    logBufferRef.current.push({
      id: Date.now().toString() + Math.random(),
      timestamp: new Date().toLocaleTimeString(),
      type,
      message
    });
  }, []);

  const clearLogs = () => setLogs([]);

  // ========================================================================
  // [新增] 自动保存与恢复 (AutoSave & Restore)
  // ========================================================================

  // A. 初始化恢复 (Mount 时执行一次)
  useEffect(() => {
      const savedData = localStorage.getItem('BRAINFLOW_AUTOSAVE');
      if (savedData) {
          try {
              const parsed = JSON.parse(savedData);
              if (parsed.nodes && Array.isArray(parsed.nodes)) {
                  setNodes(parsed.nodes);
              }
              if (parsed.edges && Array.isArray(parsed.edges)) {
                  setEdges(parsed.edges);
              }
              // 如果你需要恢复多工作流，可以在这里扩展 parsed.workflows

              // 模拟一条日志，告诉用户恢复成功
              // 注意：此时 WS 还没连上，只能手动加到 buffer
              logBufferRef.current.push({
                  id: "sys_restore", timestamp: new Date().toLocaleTimeString(),
                  type: 'success', message: "Session restored from local storage."
              });
          } catch (e) {
              console.error("AutoSave load failed:", e);
          }
      }
  }, [setNodes, setEdges]);

  // B. 自动保存 (Debounce 1s)
  useEffect(() => {
      const timer = setTimeout(() => {
          // 只保存当前画布，或者你可以选择保存整个 workflows 数组
          const dataToSave = {
              nodes,
              edges,
              timestamp: Date.now()
          };
          localStorage.setItem('BRAINFLOW_AUTOSAVE', JSON.stringify(dataToSave));
      }, 1000);
      return () => clearTimeout(timer);
  }, [nodes, edges]);

  // ========================================================================
  // 5. WebSocket 连接与消息处理
  // ========================================================================
  useEffect(() => {
    // 获取节点元数据
    fetch('http://localhost:8000/object_info')
      .then(res => res.json())
      .then(setNodeDefs)
      .catch(err => addLog(`API Error: ${err}`, 'error'));

    const connectWs = () => {
        const ws = new WebSocket('ws://localhost:8000/ws/run');

        ws.onopen = () => {
          setIsConnected(true);
          // 连接成功日志由后端 state_manager 发送，前端不再重复发 "Connected"
        };

        ws.onclose = () => {
          setIsConnected(false);
          // 3秒后尝试重连
          setTimeout(connectWs, 3000);
        };

        ws.onmessage = (e) => {
          try {
            const msg: WSMessage = JSON.parse(e.data);

            // --- A. 系统级日志 ---
            if (msg.type === 'log') addLog(msg.message || '', 'info');
            if (msg.type === 'success') addLog(msg.message || '', 'success');
            if (msg.type === 'warning') addLog(msg.message || '', 'warning');
            if (msg.type === 'error') {
                progressBufferRef.current.clear(); // 出错清空进度缓冲
                addLog(msg.message || "Unknown Error", 'error');
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
            }

            // --- B. 节点进度/状态 ---
            if (msg.taskId) {
               // 1. 更新缓冲池 (这部分始终执行，保证 UI 进度条是活的)
               progressBufferRef.current.set(msg.taskId, {
                 progress: msg.progress ?? 0,
                 message: msg.message
               });

               const text = msg.message || "";
               // 只有真正的报错才标红
               if (text.toLowerCase().includes("error")) {
                   addLog(`[${msg.taskId.split('_')[0]}] ${text}`, 'error');
               }
               // 过滤掉频繁的 log，只显示重要状态变更，这里你可以根据喜好调整
               // else if (text !== "Done" && text !== "" && !text.includes("Processing")) {
               //    const typeName = msg.taskId.split('_')[0];
               //    addLog(`[${typeName}] ${text}`, 'info');
               // }
            }

            // --- D. 工作流完成 ---
            if (msg.type === 'done') {
                progressBufferRef.current.clear(); // 立即清空进度缓冲
                addLog(msg.message || "Workflow Finished Successfully", 'success');
                // 停止连线动画
                setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
                // 强制所有节点显示完成 (防止有些节点卡在 99%)
                setNodes((nds) => nds.map(n => ({
                    ...n,
                    className: '',
                    data: { ...n.data, progress: 100, message: 'Done' }
                })));
            }

          } catch (err) {
            console.error('WebSocket message parsing error:', err);
          }
        };
        wsRef.current = ws;
    };

    connectWs();

    // --- 定时更新器 (Tick Loop) ---
    const tick = setInterval(() => {
      // 1. 处理进度更新
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
            // 只要进度不为 100，就加上呼吸灯样式
            const isRunning = updateData?.progress !== undefined && updateData.progress < 100;
            return {
                ...n,
                className: isRunning ? 'node-running-pulse' : '',
                data: { ...n.data, ...updateData }
            };
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
          // 简单的去重逻辑
          const filtered = newLogs.filter((l, i) =>
             i === 0 ? l.message !== lastMsg : l.message !== newLogs[i-1].message
          );
          if (filtered.length === 0) return prev;
          // 只保留最近 100 条
          return [...prev, ...filtered].slice(-100);
        });
      }
    }, 100); // 100ms 刷新一次 UI

    return () => {
      wsRef.current?.close();
      clearInterval(tick);
    };
  }, [setNodes, addLog, setEdges]);

  // ========================================================================
  // 6. 历史记录快照逻辑
  // ========================================================================
  useEffect(() => {
      syncCurrentState(nodes, edges);
      // 只有当不是系统自动更新进度引起的变动时，才记录快照
      const isSystemUpdate = nodes.some(n => n.data.progress !== undefined && n.data.progress > 0 && n.data.progress < 100);
      if (!isSystemUpdate) {
         takeSnapshot();
      }
  }, [nodes, edges, syncCurrentState, takeSnapshot]);

  // ========================================================================
  // 7. 连线感知逻辑 (Smart Connect)
  // ========================================================================
  const onConnectStart: OnConnectStart = useCallback((_, { nodeId, handleId, handleType }) => {
    if (!nodeId || !handleId || handleType !== 'source') return;
    const sourceNode = nodes.find(n => n.id === nodeId);
    if (!sourceNode || !sourceNode.data.nodeSpec) return;

    // 解析 Handle ID (通常是 "0", "1" 等索引)
    const typeIndex = parseInt(handleId);
    const outType = sourceNode.data.nodeSpec.output?.[typeIndex];
    if (outType) setConnectingType(outType);
  }, [nodes]);

  const onConnectEnd: OnConnectEnd = useCallback(() => {
    setConnectingType(null);
  }, []);

  // ========================================================================
  // 8. 剪贴板与删除逻辑
  // ========================================================================
  const handleCopy = useCallback(() => {
    const selectedNodes = nodes.filter(n => n.selected);
    if (selectedNodes.length === 0) return;
    const selectedIds = new Set(selectedNodes.map(n => n.id));
    // 只有两端都被选中的线才复制
    const selectedEdges = edges.filter(e => selectedIds.has(e.source) && selectedIds.has(e.target));

    clipboardRef.current = { nodes: selectedNodes, edges: selectedEdges };
    addLog(`Copied ${selectedNodes.length} nodes to clipboard`, 'info');
  }, [nodes, edges, addLog]);

  const handlePaste = useCallback(() => {
    if (!clipboardRef.current) return;
    const { nodes: cpNodes, edges: cpEdges } = clipboardRef.current;

    const idMap = new Map<string, string>();
    const newNodes: Node<NodeData>[] = [];
    // 粘贴偏移量，防止重叠
    const offset = 20 + Math.random() * 30;

    // 1. 复制节点并生成新 ID
    cpNodes.forEach(n => {
       // 生成唯一 ID
       const newId = `${n.data.opType}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
       idMap.set(n.id, newId);

       newNodes.push({
         ...n,
         id: newId,
         position: { x: n.position.x + offset, y: n.position.y + offset },
         selected: true,
         // 重置状态
         data: { ...n.data, progress: 0, message: '' }
       });
    });

    // 2. 复制连线并重定向 ID
    const newEdges = cpEdges.map(e => ({
      ...e,
      id: `e_${idMap.get(e.source)}-${idMap.get(e.target)}_${Math.random()}`,
      source: idMap.get(e.source)!,
      target: idMap.get(e.target)!,
      selected: true
    }));

    // 取消原有选中，选中新粘贴的
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

  // ========================================================================
  // 9. 键盘快捷键监听
  // ========================================================================
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // 忽略输入框中的按键
      const target = e.target as HTMLElement;
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName) || target.isContentEditable) return;

      const isCtrl = e.metaKey || e.ctrlKey;

      if (isCtrl && e.key.toLowerCase() === 'z') {
          e.preventDefault();
          if (e.shiftKey) redo(); else undo();
      }
      else if (isCtrl && e.key.toLowerCase() === 'y') { e.preventDefault(); redo(); }
      else if (isCtrl && e.key.toLowerCase() === 'c') { e.preventDefault(); handleCopy(); }
      else if (isCtrl && e.key.toLowerCase() === 'v') { e.preventDefault(); handlePaste(); }
      else if (e.key === 'Delete' || e.key === 'Backspace') { handleDelete(); }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo, handleCopy, handlePaste, handleDelete]);

  // ========================================================================
  // 10. 类型校验与连接逻辑
  // ========================================================================
  const isValidConnection = useCallback((connection: Connection | Edge) => {
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);
    if (!sourceNode || !targetNode) return false;

    // 获取源节点输出类型
    const sourceSpec = sourceNode.data.nodeSpec;
    const sourceHandleIndex = parseInt(connection.sourceHandle || "0");
    if (!sourceSpec?.output || !sourceSpec.output[sourceHandleIndex]) return false;
    const outputType = sourceSpec.output[sourceHandleIndex];

    // 获取目标节点输入类型
    const targetSpec = targetNode.data.nodeSpec;
    const targetHandleName = connection.targetHandle;
    if (!targetSpec || !targetHandleName) return false;

    // 检查 required 和 optional
    const inputConfig = targetSpec.input?.required?.[targetHandleName] || targetSpec.input?.optional?.[targetHandleName];
    if (!inputConfig) return false;

    const inputType = Array.isArray(inputConfig) ? inputConfig[0] : inputConfig;

    // 通配符支持
    if (inputType === "*" || outputType === "*") return true;

    // 精确匹配
    return outputType === inputType;
  }, [nodes]);

  const onConnect = useCallback((params: Connection) => {
      if (isValidConnection(params)) {
          setEdges((eds) => addEdge({
              ...params,
              animated: false,
              style: { stroke: '#94a3b8', strokeWidth: 2 }
          }, eds));
      } else {
          addLog("Invalid Connection: Data Type Mismatch", "error");
      }
  }, [setEdges, isValidConnection, addLog]);

  // ========================================================================
  // 11. 工作流 (Tab) 管理
  // ========================================================================
  const saveCurrentWorkflow = useCallback(() => {
    setWorkflows(prev => prev.map(w => w.id === activeWorkflowId ? { ...w, nodes, edges } : w));
  }, [nodes, edges, activeWorkflowId]);

  // 自动保存 (Debounce 500ms)
  useEffect(() => {
    const timer = setTimeout(() => saveCurrentWorkflow(), 500);
    return () => clearTimeout(timer);
  }, [nodes, edges, activeWorkflowId, saveCurrentWorkflow]);

  const switchWorkflow = (id: string) => {
    saveCurrentWorkflow(); // 切换前保存
    const target = workflows.find(w => w.id === id);
    if (target) {
        setActiveWorkflowId(id);
        setNodes(target.nodes || []);
        setEdges(target.edges || []);
    }
  };

  const createWorkflow = () => {
    saveCurrentWorkflow();
    const newId = Date.now().toString();
    setWorkflows(prev => [...prev, {
        id: newId,
        name: `Workflow ${workflows.length + 1}`,
        nodes: [],
        edges: [],
        timestamp: Date.now()
    }]);
    setActiveWorkflowId(newId);
    setNodes([]);
    setEdges([]);
  };

  const deleteWorkflow = (id: string) => {
    if (workflows.length <= 1) {
        addLog("Cannot delete the last workflow", "warning");
        return;
    }
    const newWfs = workflows.filter(w => w.id !== id);
    setWorkflows(newWfs);
    // 如果删除的是当前激活的，切换到第一个
    if (activeWorkflowId === id) switchWorkflow(newWfs[0].id);
  };

  const renameWorkflow = (id: string, name: string) => {
      setWorkflows(prev => prev.map(w => w.id === id ? { ...w, name } : w));
  };

  // ========================================================================
  // 12. 节点添加与更新
  // ========================================================================
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

  // ========================================================================
  // 13. 执行与停止
  // ========================================================================
  const runFlow = useCallback(() => {
    if (!wsRef.current) {
        addLog("Server not connected!", "error");
        return;
    }

    // 构建计算图 JSON
    const graph: Record<string, { type: string; inputs: Record<string, unknown> }> = {};

    nodes.forEach((node) => {
      // 1. 获取常规参数值
      const inputs = { ...node.data.values };

      // 2. 获取连线输入
      edges.forEach((edge) => {
        if (edge.target === node.id && edge.targetHandle) {
             // 格式: [sourceNodeId, sourceOutputIndex]
             inputs[edge.targetHandle] = [edge.source, parseInt(edge.sourceHandle || "0")];
        }
      });

      graph[node.id] = { type: node.data.opType, inputs };
    });

    // 激活连线动画
    setEdges((eds) => eds.map(e => ({ ...e, animated: true })));

    // 发送
    wsRef.current.send(JSON.stringify({ command: 'execute_graph', graph }));
    addLog("Executing Workflow...", 'info');

    // 重置所有节点的进度
    setNodes((nds) => nds.map(n => ({
        ...n,
        data: { ...n.data, progress: 0, message: 'Pending...' }
    })));

  }, [nodes, edges, addLog, setEdges, setNodes]);

  const stopFlow = useCallback(() => {
    if (!wsRef.current) return;
    wsRef.current.send(JSON.stringify({ command: 'stop_execution' }));

    // 立即停止动画，不等后端回传，提升响应速度
    setEdges((eds) => eds.map(e => ({ ...e, animated: false })));
    addLog("Requesting Stop...", 'warning');
  }, [addLog, setEdges]);

  // ========================================================================
  // 14. Render Provider
  // ========================================================================
  return (
    <FlowContext.Provider value={{
      // State
      nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
      // Node/Edge Actions
      setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
      addNode, addNodeAt, updateNodeData,
      // Flow Control
      runFlow, stopFlow ,clearLogs,
      // Workflow Management
      createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
      // UI State
      theme, toggleTheme, isConsoleOpen, toggleConsole,
      // Helpers
      isValidConnection, undo, redo,
      // Interaction Hooks
      onConnectStart, onConnectEnd, connectingType,
      // Clipboard
      handleCopy, handlePaste, handleDelete
    }}>
      {children}
    </FlowContext.Provider>
  );
};
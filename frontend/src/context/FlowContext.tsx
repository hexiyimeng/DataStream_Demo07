// src/context/FlowContext.tsx
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useNodesState, useEdgesState, addEdge, type Connection, type Node, type Edge, type OnConnectStart, type OnConnectEnd } from '@xyflow/react';
import type { LogEntry, NodeData } from '../types';
import { FlowContext } from './FlowContextDef';

// 引入所有拆分的 Hooks
import { useUndoRedo } from '../hooks/useUndoRedo';
import { useAutoSave } from '../hooks/useAutoSave';
import { useFlowOperations } from '../hooks/useFlowOperations';
import { useFlowEngine } from '../hooks/useFlowEngine';
import { useWorkflows } from '../hooks/useWorkflows';

export const FlowProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // ===========================================
  // 1. 基础状态 (Base State)
  // ===========================================
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<NodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);
  const [connectingType, setConnectingType] = useState<string | null>(null);

  // 日志系统
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const logBufferRef = useRef<LogEntry[]>([]);

  // 使用 useCallback 保证引用稳定
  const addLog = useCallback((message: string, type: 'info' | 'success' | 'error' | 'warning' = 'info') => {
    logBufferRef.current.push({ id: Date.now().toString() + Math.random(), timestamp: new Date().toLocaleTimeString(), type, message });
  }, []);

  const clearLogs = useCallback(() => setLogs([]), []);

  // 日志 Tick Loop (避免高频 setState)
  useEffect(() => {
    const tick = setInterval(() => {
      if (logBufferRef.current.length > 0) {
        const newLogs = [...logBufferRef.current];
        logBufferRef.current = [];
        setLogs(prev => [...prev, ...newLogs].slice(-100));
      }
    }, 100);
    return () => clearInterval(tick);
  }, []);

  // 主题副作用
  useEffect(() => { document.documentElement.classList.toggle('dark', theme === 'dark'); }, [theme]);

  const toggleTheme = useCallback(() => setTheme(t => t === 'light' ? 'dark' : 'light'), []);
  const toggleConsole = useCallback(() => setIsConsoleOpen(p => !p), []);

  // ===========================================
  // 2. 核心 Hook 组装 (Composition)
  // ===========================================

  // A. 撤销/重做
  const { undo, redo, takeSnapshot, syncCurrentState } = useUndoRedo<NodeData>(
    [], [], (nds) => setNodes(nds), (eds) => setEdges(eds)
  );

  // B. 历史快照触发器
  useEffect(() => {
      syncCurrentState(nodes, edges);
      // 只有非系统更新（不是进度条更新）才记录快照
      const isSystemUpdate = nodes.some(n => n.data.progress !== undefined && n.data.progress > 0 && n.data.progress < 100);
      if (!isSystemUpdate) takeSnapshot();
  }, [nodes, edges, syncCurrentState, takeSnapshot]);

  // C. 自动保存 (LocalStorage)
  useAutoSave(nodes, edges, setNodes, setEdges);

  // D. 交互操作 (复制/粘贴/快捷键)
  const { handleCopy, handlePaste, handleDelete } = useFlowOperations(nodes, edges, setNodes, setEdges, undo, redo, addLog);

  // E. 工作流管理 (多 Tab)
  const {
    workflows, activeWorkflowId, createWorkflow, switchWorkflow,
    deleteWorkflow, renameWorkflow, saveCurrentWorkflow
  } = useWorkflows(nodes, edges, setNodes, setEdges, addLog);

  // F. 引擎核心 (WebSocket/Run)
  const { isConnected, nodeDefs, runFlow, stopFlow } = useFlowEngine(nodes, edges, setNodes, setEdges, addLog);

  // ===========================================
  // 3. 连接校验与辅助 (Helpers)
  // ===========================================
  const isValidConnection = useCallback((connection: Connection | Edge) => {
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);
    if (!sourceNode || !targetNode) return false;

    const sourceSpec = sourceNode.data.nodeSpec;
    const targetSpec = targetNode.data.nodeSpec;
    const sourceHandleIndex = parseInt(connection.sourceHandle || "0");
    const targetHandleName = connection.targetHandle;

    if (!sourceSpec?.output?.[sourceHandleIndex] || !targetSpec || !targetHandleName) return false;

    const outputType = sourceSpec.output[sourceHandleIndex];
    const inputConfig = targetSpec.input?.required?.[targetHandleName] || targetSpec.input?.optional?.[targetHandleName];
    if (!inputConfig) return false;

    const inputType = Array.isArray(inputConfig) ? inputConfig[0] : inputConfig;
    // 简单的类型兼容性检查 (* 为通配符)
    return (inputType === "*" || outputType === "*" || outputType === inputType);
  }, [nodes]);

  const onConnect = useCallback((params: Connection) => {
      if (isValidConnection(params)) {
          setEdges((eds) => addEdge({ ...params, animated: false, style: { stroke: '#94a3b8', strokeWidth: 2 } }, eds));
      } else { addLog("Invalid Connection", "error"); }
  }, [setEdges, isValidConnection, addLog]);

  const onConnectStart: OnConnectStart = useCallback((_, { nodeId, handleId, handleType }) => {
    if (handleType !== 'source') return;
    const node = nodes.find(n => n.id === nodeId);
    if (node) setConnectingType(node.data.nodeSpec?.output?.[parseInt(handleId || "0")] || null);
  }, [nodes]);

  const onConnectEnd: OnConnectEnd = useCallback(() => setConnectingType(null), []);

  const addNodeAt = useCallback((type: string, position: {x: number, y: number}) => {
    const spec = nodeDefs[type]; if (!spec) return;
    setNodes((nds) => nds.concat({ id: `${type}_${Date.now()}`, type: 'dynamic', position, data: { opType: type, nodeSpec: spec, values: {}, progress: 0, message: "" } }));
  }, [nodeDefs, setNodes]);

  const addNode = useCallback((type: string) => addNodeAt(type, { x: Math.random() * 400 + 200, y: Math.random() * 300 + 100 }), [addNodeAt]);

  const updateNodeData = useCallback((id: string, newData: Partial<NodeData>) => {
    setNodes((nds) => nds.map((n) => n.id === id ? { ...n, data: { ...n.data, ...newData } } : n));
  }, [setNodes]);

  // ===========================================
  // 4. 性能优化 (Context Memoization)
  // ===========================================
  // 将 Context 的值进行 memoize，防止每次 render 都生成新对象导致消费组件无效渲染
  const contextValue = useMemo(() => ({
      // State
      nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
      // Actions
      setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
      addNode, addNodeAt, updateNodeData,
      runFlow, stopFlow, clearLogs,
      // Workflow Actions
      createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
      // UI
      theme, toggleTheme, isConsoleOpen, toggleConsole,
      isValidConnection, undo, redo,
      onConnectStart, onConnectEnd, connectingType,
      handleCopy, handlePaste, handleDelete
  }), [
      // 依赖列表：只有这些变化时，Context 才会更新
      nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
      theme, isConsoleOpen, connectingType,
      // 下面这些通常是稳定的函数引用，但为了安全也加上
      setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
      addNode, addNodeAt, updateNodeData, runFlow, stopFlow, clearLogs,
      createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
      toggleTheme, toggleConsole, isValidConnection, undo, redo,
      onConnectStart, onConnectEnd, handleCopy, handlePaste, handleDelete
  ]);

  return (
    <FlowContext.Provider value={contextValue}>
      {children}
    </FlowContext.Provider>
  );
};
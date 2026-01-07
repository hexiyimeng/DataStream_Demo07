import React, { useState, useEffect, useRef, useCallback } from 'react';
import {useNodesState, useEdgesState, addEdge, type Connection, type Node, type Edge} from '@xyflow/react';
import type { NodeSpec, WSMessage, Workflow, LogEntry, BrainFlowNodeData } from '../types';
import { FlowContext } from './FlowContextDef';

export const FlowProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<BrainFlowNodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [theme, setTheme] = useState<'light' | 'dark'>('dark');
  const [isConsoleOpen, setIsConsoleOpen] = useState(true);
  const toggleConsole = () => setIsConsoleOpen(prev => !prev);
  const toggleTheme = () => setTheme(prev => prev === 'light' ? 'dark' : 'light');

  const [workflows, setWorkflows] = useState<Workflow[]>(() => [{ id: '1', name: 'Workflow 1', nodes: [], edges: [], timestamp: Date.now() }]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>('1');
  const [nodeDefs, setNodeDefs] = useState<Record<string, NodeSpec>>({});
  const [isConnected, setIsConnected] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  const addLog = (message: string, type: 'info' | 'success' | 'error' = 'info') => {
    setLogs(prev => {
        // 简单防抖：如果最后一条日志跟这一条一样，就不重复添加（针对进度刷屏）
        if (prev.length > 0 && prev[prev.length - 1].message === message) return prev;
        return [...prev, { id: Date.now().toString() + Math.random(), timestamp: new Date().toLocaleTimeString(), type, message }].slice(-100);
    });
  };

  useEffect(() => {
    fetch('http://localhost:8000/object_info').then(res => res.json()).then(setNodeDefs).catch(err => addLog(`API Error: ${err}`, 'error'));
    const connectWs = () => {
        const ws = new WebSocket('ws://localhost:8000/ws/run');
        ws.onopen = () => { setIsConnected(true); addLog("Server Connected", 'success'); };
        ws.onclose = () => { setIsConnected(false); setTimeout(connectWs, 3000); };

        ws.onmessage = (e) => {
          try {
            const msg: WSMessage = JSON.parse(e.data);

            // 1. 普通日志
            if (msg.type === 'log') addLog(msg.message || '', 'info');

            // 2. 进度更新
            if (msg.type === 'progress' && msg.taskId) {
               setNodes(nds => nds.map(n => n.id === msg.taskId ? {
                 ...n,
                 data: {
                    ...n.data,
                    progress: msg.progress ?? 0,
                    message: msg.message // 【关键】把消息存进去，DynamicNode 才能显示
                 }
               } : n));

               // 【关键】如果后端发来了描述性文字 (例如 "Writing... 10%")，也打印到控制台
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
    return () => wsRef.current?.close();
  }, [setNodes]);

  useEffect(() => {
    if (theme === 'dark') document.documentElement.classList.add('dark');
    else document.documentElement.classList.remove('dark');
  }, [theme]);

  // 【新增】核心类型校验逻辑
  // =========================================================
  const isValidConnection = useCallback((connection: Connection | Edge) => {
    // 1. 查找源节点和目标节点
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);

    if (!sourceNode || !targetNode) return false;

    // 2. 获取源节点的输出类型
    // DynamicNode 中输出 Handle 的 ID 是数字索引 ("0", "1"...)
    const sourceSpec = sourceNode.data.nodeSpec;
    const sourceHandleIndex = parseInt(connection.sourceHandle || "0");

    // 安全检查：防止索引越界
    if (!sourceSpec?.output || !sourceSpec.output[sourceHandleIndex]) return false;
    const outputType = sourceSpec.output[sourceHandleIndex];

    // 3. 获取目标节点的输入类型
    // DynamicNode 中输入 Handle 的 ID 是参数名 (例如 "stream", "model")
    const targetSpec = targetNode.data.nodeSpec;
    const targetHandleName = connection.targetHandle;

    if (!targetSpec || !targetHandleName) return false;

    // 在 required 中查找该输入的定义
    // ComfyUI 协议: required 的值是 ["TYPE", {config}] 或 ["TYPE"]
    const inputConfig = targetSpec.input?.required?.[targetHandleName];

    // 如果没找到配置，说明这个 Handle 不应该存在，禁止连接
    if (!inputConfig) return false;

    // 提取类型字符串 (数组的第一个元素)
    const inputType = Array.isArray(inputConfig) ? inputConfig[0] : inputConfig;

    // 4. 类型比对
    // 支持通配符 "*" (ComfyUI 习惯)
    if (inputType === "*" || outputType === "*") return true;

    // 严格相等校验
    const isValid = outputType === inputType;

    // (可选) 调试日志
    // if (!isValid) console.warn(`[TypeCheck] Blocked: ${outputType} -> ${inputType}`);

    return isValid;
  }, [nodes]); // 依赖 nodes 数据

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

  const addNode = useCallback((type: string) => {
    const spec = nodeDefs[type]; if (!spec) return;
    setNodes((nds) => nds.concat({
      id: `${type}_${Date.now()}`, type: 'dynamic', position: { x: Math.random() * 400 + 200, y: Math.random() * 300 + 100 },
      data: { opType: type, nodeSpec: spec, values: {}, progress: 0, message: "" },
    }));
  }, [nodeDefs, setNodes]);


  const addNodeAt = useCallback((type: string, position: {x: number, y: number}) => {
    const spec = nodeDefs[type]; if (!spec) return;
    setNodes((nds) => nds.concat({
      id: `${type}_${Date.now()}`,
      type: 'dynamic',
      position: position, // 使用传入的坐标
      data: { opType: type, nodeSpec: spec, values: {}, progress: 0, message: "" },
    }));
  }, [nodeDefs, setNodes]);

  const updateNodeData = useCallback((id: string, newData: Partial<BrainFlowNodeData>) => {
    setNodes((nds) => nds.map((n) => n.id === id ? { ...n, data: { ...n.data, ...newData } } : n));
  }, [setNodes]);

  const onConnect = useCallback((params: Connection) => setEdges((eds) => addEdge({ ...params, animated: true, style: { stroke: '#94a3b8', strokeWidth: 2 } }, eds)), [setEdges]);

  const runFlow = useCallback(() => {
    if (!wsRef.current) return;
    const graph: Record<string, { type: string; inputs: Record<string, unknown> }> = {};
    nodes.forEach((node) => {
      const inputs = { ...node.data.values };
      edges.forEach((edge) => {
        if (edge.target === node.id && edge.targetHandle) inputs[edge.targetHandle] = [edge.source, parseInt(edge.sourceHandle || "0")];
      });
      graph[node.id] = { type: node.data.opType, inputs };
    });
    wsRef.current.send(JSON.stringify({ command: 'execute_graph', graph }));
    addLog(" Request Sent", 'info');
  }, [nodes, edges]);

  const clearLogs = () => setLogs([]);

  return (
    <FlowContext.Provider value={{
      nodes, edges, nodeDefs, isConnected, logs, workflows, activeWorkflowId,
      setNodes, setEdges, onNodesChange, onEdgesChange, onConnect,
      addNode, addNodeAt, updateNodeData, runFlow, clearLogs,
      createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, saveCurrentWorkflow,
      theme, toggleTheme, isConsoleOpen, toggleConsole, isValidConnection
    }}>
      {children}
    </FlowContext.Provider>
  );
};
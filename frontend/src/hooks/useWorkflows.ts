import { useState, useCallback, useEffect } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { Workflow, NodeData } from '../types';

export const useWorkflows = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: (nodes: Node<NodeData>[]) => void,
  setEdges: (edges: Edge[]) => void,
  addLog: (msg: string, type?: 'info' | 'success' | 'error' | 'warning') => void
) => {
  // 初始状态：默认有一个工作流
  const [workflows, setWorkflows] = useState<Workflow[]>(() => [
    { id: '1', name: 'Workflow 1', nodes: [], edges: [], timestamp: Date.now() }
  ]);
  const [activeWorkflowId, setActiveWorkflowId] = useState<string>('1');

  // === 核心：保存当前画布到 activeWorkflow ===
  // 注意：这个函数会更新 workflows 数组中的数据
  const saveCurrentWorkflow = useCallback(() => {
    setWorkflows(prev => prev.map(w =>
      w.id === activeWorkflowId ? { ...w, nodes, edges } : w
    ));
  }, [nodes, edges, activeWorkflowId]);

  // === 自动同步 (Debounce 500ms) ===
  // 当画布 (nodes/edges) 变化时，自动更新 workflows 数组里的数据
  // 这样你切换 Tab 时，数据才是最新的
  useEffect(() => {
    const timer = setTimeout(() => saveCurrentWorkflow(), 500);
    return () => clearTimeout(timer);
  }, [nodes, edges, activeWorkflowId, saveCurrentWorkflow]);

  // === 1. 切换工作流 ===
  const switchWorkflow = useCallback((id: string) => {
    // 切换前强制保存一次当前状态 (以防 Debounce 还没触发)
    saveCurrentWorkflow();

    const target = workflows.find(w => w.id === id);
    if (target) {
      setActiveWorkflowId(id);
      // 恢复画布
      setNodes(target.nodes || []);
      setEdges(target.edges || []);
      addLog(`Switched to ${target.name}`, 'info');
    }
  }, [workflows, saveCurrentWorkflow, setNodes, setEdges, addLog]);

  // === 2. 新建工作流 ===
  const createWorkflow = useCallback(() => {
    saveCurrentWorkflow(); // 保存旧的

    const newId = Date.now().toString();
    const newName = `Workflow ${workflows.length + 1}`;

    setWorkflows(prev => [...prev, {
        id: newId,
        name: newName,
        nodes: [],
        edges: [],
        timestamp: Date.now()
    }]);

    // 切换到新的 (空的)
    setActiveWorkflowId(newId);
    setNodes([]);
    setEdges([]);
    addLog(`Created ${newName}`, 'success');
  }, [workflows.length, saveCurrentWorkflow, setNodes, setEdges, addLog]);

  // === 3. 删除工作流 ===
  const deleteWorkflow = useCallback((id: string) => {
    if (workflows.length <= 1) {
        addLog("Cannot delete the last workflow", "warning");
        return;
    }

    const targetName = workflows.find(w => w.id === id)?.name;
    const newWfs = workflows.filter(w => w.id !== id);
    setWorkflows(newWfs);

    // 如果删除的是当前激活的，切换到第一个
    if (activeWorkflowId === id) {
       const nextWf = newWfs[0];
       setActiveWorkflowId(nextWf.id);
       setNodes(nextWf.nodes || []);
       setEdges(nextWf.edges || []);
    }

    addLog(`Deleted ${targetName}`, 'info');
  }, [workflows, activeWorkflowId, setNodes, setEdges, addLog]);

  // === 4. 重命名 ===
  const renameWorkflow = useCallback((id: string, name: string) => {
      setWorkflows(prev => prev.map(w => w.id === id ? { ...w, name } : w));
  }, []);

  return {
    workflows,
    activeWorkflowId,
    createWorkflow,
    switchWorkflow,
    deleteWorkflow,
    renameWorkflow,
    saveCurrentWorkflow
  };
};
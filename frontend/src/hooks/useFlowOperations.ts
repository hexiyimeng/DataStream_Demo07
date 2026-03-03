import { useRef, useCallback, useEffect } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData, LogEntry } from '../types';
//负责用户交互：复制、粘贴、删除、键盘快捷键。
export const useFlowOperations = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node<NodeData>[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  undo: () => void,
  redo: () => void,
  addLog: (msg: string, type?: LogEntry['type']) => void
) => {
  const clipboardRef = useRef<{ nodes: Node<NodeData>[], edges: Edge[] } | null>(null);

  // === 复制 ===
  const handleCopy = useCallback(() => {
    const selectedNodes = nodes.filter(n => n.selected);
    if (selectedNodes.length === 0) return;
    const selectedIds = new Set(selectedNodes.map(n => n.id));
    // 仅复制两端都被选中的连线
    const selectedEdges = edges.filter(e => selectedIds.has(e.source) && selectedIds.has(e.target));

    clipboardRef.current = { nodes: selectedNodes, edges: selectedEdges };
    addLog(`Copied ${selectedNodes.length} nodes`, 'info');
  }, [nodes, edges, addLog]);

  // === 粘贴 ===
  const handlePaste = useCallback(() => {
    if (!clipboardRef.current) return;
    const { nodes: cpNodes, edges: cpEdges } = clipboardRef.current;

    const idMap = new Map<string, string>();
    const newNodes: Node<NodeData>[] = [];
    const offset = 20 + Math.random() * 30; // 随机偏移防止重叠

    // 1. 复制节点并重生成 ID
    cpNodes.forEach(n => {
       const newId = `${n.data.opType}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
       idMap.set(n.id, newId);
       newNodes.push({
         ...n, id: newId,
         position: { x: n.position.x + offset, y: n.position.y + offset },
         selected: true,
         data: { ...n.data, progress: 0, message: '' } // 重置运行状态
       });
    });

    // 2. 复制连线并重定向 ID
    const newEdges = cpEdges.map(e => ({
      ...e, id: `e_${idMap.get(e.source)}-${idMap.get(e.target)}_${Math.random()}`,
      source: idMap.get(e.source)!, target: idMap.get(e.target)!, selected: true
    }));

    // 取消原有选中，选中新粘贴的
    setNodes(nds => nds.map(n => ({ ...n, selected: false } as Node<NodeData>)).concat(newNodes));
    setEdges(eds => eds.map(e => ({ ...e, selected: false } as Edge)).concat(newEdges));
    addLog(`Pasted ${newNodes.length} nodes`, 'info');
  }, [setNodes, setEdges, addLog]);

  // === 删除 ===
  const handleDelete = useCallback(() => {
      const selectedNodes = nodes.filter(n => n.selected);
      const selectedEdges = edges.filter(e => e.selected);
      if (selectedNodes.length === 0 && selectedEdges.length === 0) return;

      setNodes(nds => nds.filter(n => !n.selected));
      setEdges(eds => eds.filter(e => !e.selected));
  }, [nodes, edges, setNodes, setEdges]);

  // === 快捷键监听 ===
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      // 避免在输入框打字时触发快捷键
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

  return { handleCopy, handlePaste, handleDelete };
};
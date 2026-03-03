// src/hooks/useAutoSave.ts
import { useEffect } from 'react';
import type { Node, Edge } from '@xyflow/react';
import type { NodeData } from '../types';

export const useAutoSave = (
  nodes: Node<NodeData>[],
  edges: Edge[],
  setNodes: (n: Node<NodeData>[]) => void,
  setEdges: (e: Edge[]) => void
) => {
  // 1. 初始化恢复 (关键修改：数据清洗)
  useEffect(() => {
    const savedData = localStorage.getItem('BRAINFLOW_AUTOSAVE');
    if (savedData) {
      try {
        const parsed = JSON.parse(savedData);

        // A. 恢复节点，但强制重置运行状态
        if (parsed.nodes && Array.isArray(parsed.nodes)) {
          const cleanNodes = parsed.nodes.map((n: Node<NodeData>) => ({
            ...n,
            // 强制覆盖 data 中的运行时状态
            data: {
              ...n.data,
              progress: 0,       // 进度归零
              message: '',       // 消息清空
            },
            // 移除可能残留的呼吸灯样式类名
            className: n.className ? n.className.replace('node-running-pulse', '').trim() : ''
          }));
          setNodes(cleanNodes);
        }

        // B. 恢复连线，但强制停止动画
        if (parsed.edges && Array.isArray(parsed.edges)) {
          const cleanEdges = parsed.edges.map((e: Edge) => ({
            ...e,
            animated: false, // 强制停止蚂蚁线动画
            style: { ...e.style, stroke: '#94a3b8', strokeWidth: 2 } // 恢复默认颜色
          }));
          setEdges(cleanEdges);
        }

        console.log("Session restored from local storage (Runtime state cleansed).");
      } catch (e) {
        console.error("AutoSave load failed:", e);
      }
    }
  }, []); // 空依赖，只在组件挂载时执行一次

  // 2. 自动保存 (保持不变，依然保存当前的结构)
  useEffect(() => {
    const timer = setTimeout(() => {
      // 保存时其实可以把 dirty state 存进去，但加载时清洗掉即可，这样逻辑最简单
      const dataToSave = { nodes, edges, timestamp: Date.now() };
      localStorage.setItem('BRAINFLOW_AUTOSAVE', JSON.stringify(dataToSave));
    }, 1000);
    return () => clearTimeout(timer);
  }, [nodes, edges]);
};
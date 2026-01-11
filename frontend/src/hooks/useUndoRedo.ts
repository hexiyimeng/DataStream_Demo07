import { useState, useCallback, useRef } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { debounce } from 'lodash-es'; // 需要安装: npm i lodash-es @types/lodash-es

interface HistoryState {
  nodes: Node[];
  edges: Edge[];
}

export const useUndoRedo = (
  initialNodes: Node[],
  initialEdges: Edge[],
  setNodes: (nodes: Node[]) => void,
  setEdges: (edges: Edge[]) => void
) => {
  // 历史栈：过去和未来
  const [past, setPast] = useState<HistoryState[]>([]);
  const [future, setFuture] = useState<HistoryState[]>([]);

  // 当前状态的引用，用于快照
  // 我们使用 ref 来避免闭包陷阱，保证拿到的总是最新的
  const currentState = useRef<HistoryState>({ nodes: initialNodes, edges: initialEdges });

  // 同步外部状态到内部 Ref (每次外部 nodes/edges 变了都要调这个)
  const syncCurrentState = useCallback((nodes: Node[], edges: Edge[]) => {
    currentState.current = { nodes, edges };
  }, []);

  // 核心：拍摄快照 (存入历史)
  // 使用 debounce 防抖，防止拖拽节点时存入几百条记录
  const takeSnapshot = useCallback(
    debounce(() => {
      setPast((prev) => {
        // 限制历史记录最多 50 步，节省内存
        const newPast = [...prev, currentState.current];
        return newPast.length > 50 ? newPast.slice(newPast.length - 50) : newPast;
      });
      setFuture([]); // 一旦有新操作，未来的记录就失效了
    }, 500), // 500ms 内没有新操作才存盘
    []
  );

  const undo = useCallback(() => {
    setPast((prev) => {
      if (prev.length === 0) return prev;

      const newPast = [...prev];
      const previousState = newPast.pop(); // 拿到上一步

      if (previousState) {
        // 把现在的状态推入 Future
        setFuture((f) => [currentState.current, ...f]);

        // 恢复状态
        setNodes(previousState.nodes);
        setEdges(previousState.edges);
        // 更新 Ref
        currentState.current = previousState;
      }
      return newPast;
    });
  }, [setNodes, setEdges]);

  const redo = useCallback(() => {
    setFuture((prev) => {
      if (prev.length === 0) return prev;

      const newFuture = [...prev];
      const nextState = newFuture.shift(); // 拿到下一步

      if (nextState) {
        // 把现在的状态推入 Past
        setPast((p) => [...p, currentState.current]);

        // 恢复状态
        setNodes(nextState.nodes);
        setEdges(nextState.edges);
        // 更新 Ref
        currentState.current = nextState;
      }
      return newFuture;
    });
  }, [setNodes, setEdges]);

  return { undo, redo, takeSnapshot, syncCurrentState, canUndo: past.length > 0, canRedo: future.length > 0 };
};
import { useState, useCallback, useRef, useEffect } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { debounce } from 'lodash-es'; // 需要安装: npm i lodash-es @types/lodash-es

interface HistoryState<T extends Record<string, unknown> = Record<string, unknown>> {
  nodes: Node<T>[];
  edges: Edge[];
}

export const useUndoRedo = <T extends Record<string, unknown> = Record<string, unknown>>(
  initialNodes: Node<T>[],
  initialEdges: Edge[],
  setNodes: (nodes: Node<T>[]) => void,
  setEdges: (edges: Edge[]) => void
) => {
  // 历史栈：过去和未来
  const [past, setPast] = useState<HistoryState<T>[]>([]);
  const [future, setFuture] = useState<HistoryState<T>[]>([]);

  // 当前状态的引用，用于快照
  // 我们使用 ref 来避免闭包陷阱，保证拿到的总是最新的
  const currentState = useRef<HistoryState<T>>({ nodes: initialNodes, edges: initialEdges });

  // 同步外部状态到内部 Ref (每次外部 nodes/edges 变了都要调这个)
  const syncCurrentState = useCallback((nodes: Node<T>[], edges: Edge[]) => {
    currentState.current = { nodes, edges };
  }, []);

  // 核心：拍摄快照 (存入历史)
  // 使用 debounce 防抖，防止拖拽节点时存入几百条记录
  const saveToHistory = useCallback(() => {
    setPast((prev) => {
      // 限制历史记录最多 30 步，节省内存
      const newPast = [...prev, currentState.current];
      return newPast.length > 30 ? newPast.slice(newPast.length - 30) : newPast;
    });
    setFuture([]); // 一旦有新操作，未来的记录就失效了
  }, [currentState]);

  const debouncedSaveHistoryRef = useRef<ReturnType<typeof debounce> | null>(null);
  
  useEffect(() => {
    // 清除之前的防抖函数
    if (debouncedSaveHistoryRef.current) {
      debouncedSaveHistoryRef.current.cancel();
    }
    
    // 创建新的防抖函数
    debouncedSaveHistoryRef.current = debounce(saveToHistory, 500);
    
    // 清理函数
    return () => {
      if (debouncedSaveHistoryRef.current) {
        debouncedSaveHistoryRef.current.cancel();
      }
    };
  }, [saveToHistory]);
  
  const takeSnapshot = useCallback(() => {
    if (debouncedSaveHistoryRef.current) {
      debouncedSaveHistoryRef.current();
    }
  }, []);

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
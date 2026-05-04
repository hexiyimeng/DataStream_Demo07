// src/hooks/useUndoRedo.ts
import { useState, useCallback, useEffect, useRef } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { debounce } from 'lodash-es';
import type { NodeData, NodeSpec } from '../types';
import { serializeNodesForStorage, hydrateNodesWithLatestSpecs, validateEdgesWithLatestSpecs } from '../utils/workflowPersistence';
import type { SerializedNode } from '../utils/workflowPersistence';

interface HistoryState {
  nodes: SerializedNode[];
  edges: Edge[];
}

// NOTE: useUndoRedo always operates on Node<NodeData> internally.
// The caller (FlowContext) passes T=NodeData so the hook composes with the rest
// of the context, but internally we cast to Node<NodeData> for hydration compatibility.
export const useUndoRedo = (
  _initialNodes: Node<NodeData>[],
  _initialEdges: Edge[],
  setNodes: (nodes: Node<NodeData>[]) => void,
  setEdges: (edges: Edge[]) => void,
  nodeDefs: Record<string, NodeSpec> = {},
) => {
  const [past, setPast] = useState<HistoryState[]>([]);
  const [future, setFuture] = useState<HistoryState[]>([]);

  const currentState = useRef<{ nodes: Node<NodeData>[]; edges: Edge[] }>({
    nodes: [] as Node<NodeData>[],
    edges: [] as Edge[],
  });

  const syncCurrentState = useCallback((nodes: Node<NodeData>[], edges: Edge[]) => {
    currentState.current = { nodes, edges };
  }, []);

  // Snapshot: store stripped (no runtime data) nodes
  const saveToHistory = useCallback(() => {
    const stripped = serializeNodesForStorage(currentState.current.nodes);
    const newEntry: HistoryState = {
      nodes: stripped,
      edges: currentState.current.edges,
    };

    setPast(prev => {
      const newPast = [...prev, newEntry];
      return newPast.length > 30 ? newPast.slice(newPast.length - 30) : newPast;
    });
    setFuture([]);
  }, []);

  const debouncedSaveHistoryRef = useRef<ReturnType<typeof debounce> | null>(null);

  useEffect(() => {
    if (debouncedSaveHistoryRef.current) {
      debouncedSaveHistoryRef.current.cancel();
    }
    debouncedSaveHistoryRef.current = debounce(saveToHistory, 500);
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
    setPast(prev => {
      if (prev.length === 0) return prev;
      const newPast = [...prev];
      const previousEntry = newPast.pop();
      if (!previousEntry) return prev;

      setFuture(f => [{
        nodes: serializeNodesForStorage(currentState.current.nodes),
        edges: currentState.current.edges,
      }, ...f]);

      if (Object.keys(nodeDefs).length > 0) {
        const hydratedNodes = hydrateNodesWithLatestSpecs(previousEntry.nodes, nodeDefs);
        const { validEdges } = validateEdgesWithLatestSpecs(previousEntry.edges, hydratedNodes);
        const castNodes = hydratedNodes as unknown as Node<NodeData>[];
        setNodes(castNodes);
        setEdges(validEdges);
        currentState.current = { nodes: castNodes, edges: validEdges };
      } else {
        setNodes(currentState.current.nodes);
        setEdges(currentState.current.edges);
      }

      return newPast;
    });
  }, [setNodes, setEdges, nodeDefs]);

  const redo = useCallback(() => {
    setFuture(prev => {
      if (prev.length === 0) return prev;
      const newFuture = [...prev];
      const nextEntry = newFuture.shift();
      if (!nextEntry) return prev;

      setPast(p => [
        ...p,
        {
          nodes: serializeNodesForStorage(currentState.current.nodes),
          edges: currentState.current.edges,
        },
      ]);

      if (Object.keys(nodeDefs).length > 0) {
        const hydratedNodes = hydrateNodesWithLatestSpecs(nextEntry.nodes, nodeDefs);
        const { validEdges } = validateEdgesWithLatestSpecs(nextEntry.edges, hydratedNodes);
        const castNodes = hydratedNodes as unknown as Node<NodeData>[];
        setNodes(castNodes);
        setEdges(validEdges);
        currentState.current = { nodes: castNodes, edges: validEdges };
      } else {
        setNodes(currentState.current.nodes);
        setEdges(currentState.current.edges);
      }

      return newFuture;
    });
  }, [setNodes, setEdges, nodeDefs]);

  return {
    undo,
    redo,
    takeSnapshot,
    syncCurrentState,
    canUndo: past.length > 0,
    canRedo: future.length > 0,
  };
};
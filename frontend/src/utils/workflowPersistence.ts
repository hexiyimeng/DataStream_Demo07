/**
 * workflowPersistence.ts
 *
 * Centralized hydration and serialization for BrainFlow workflow data.
 *
 * Design principles:
 * - `nodeSpec` is runtime-derived from backend `/object_info`. It must NOT be
 *   stored as persistent workflow schema — backend node definitions evolve and
 *   old stored specs become stale, causing false type mismatches.
 * - When loading/restoring nodes, always rehydrate `nodeSpec` from the latest
 *   `nodeDefs` using `node.data.opType` as the lookup key.
 * - Serialize only the user graph: node id, type, position, opType, values,
 *   dimensions, and edges.
 * - Strip all runtime state: nodeSpec, progress, runState, chunk counters,
 *   executionId, animated edge state, etc.
 */

import type { Node, Edge } from '@xyflow/react';
import type { NodeData, NodeSpec } from '../types';
import { canConnectPorts, resolveNodeOutputTypes } from './portTypes';

// ============================================================
// Types
// ============================================================

export interface SerializedFlow {
  nodes: SerializedNode[];
  edges: Edge[];
}

export interface SerializedNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: {
    opType: string;
    values?: Record<string, unknown>;
    dimensions?: Record<string, number>;
    selected?: boolean;
  };
}

export interface HydrationResult {
  nodes: Node<NodeData>[];
  edges: Edge[];
  invalidNodeTypes: string[];
  removedEdges: number;
  warnings: string[];
}

export interface SerializedNodeForStorage {
  id: string;
  type?: string;
  position: { x: number; y: number };
  data: {
    opType: string;
    values?: Record<string, unknown>;
    dimensions?: Record<string, number>;
    selected?: boolean;
  };
}

// ============================================================
// Fallback node spec for invalid/missing node types
// ============================================================

const FALLBACK_NODE_SPEC: NodeSpec = {
  type: '__fallback__',
  display_name: 'Unknown Node',
  category: 'Unknown',
  input: { required: {} },
  output: [],
};

// ============================================================
// 1. Strip runtime data from a single node (returns plain object)
// ============================================================

/**
 * Strip all runtime-derived fields from a node, returning a clean
 * serialized representation that contains only the user graph data.
 * Returns a plain object (not a ReactFlow Node) for storage.
 */
export function stripRuntimeNodeData(node: Node<NodeData>): SerializedNode {
  return {
    id: node.id,
    type: node.type ?? 'dynamic',
    position: { x: node.position.x, y: node.position.y },
    data: {
      opType: node.data.opType,
      values: node.data.values,
      dimensions: node.data.dimensions as Record<string, number> | undefined,
      selected: node.selected,
    },
  };
}

// ============================================================
// 2. Serialize a node array for storage
// ============================================================

/**
 * Convert a node array to a storage-safe format, stripping all runtime fields.
 * Does NOT include edges — those are serialized separately by serializeFlowForStorage.
 */
export function serializeNodesForStorage(nodes: Node<NodeData>[]): SerializedNode[] {
  return nodes.map(n => stripRuntimeNodeData(n));
}

// ============================================================
// 3. Serialize the full flow (nodes + edges) for storage
// ============================================================

/**
 * Serialize both nodes and edges into a storage-safe format.
 * Nodes are stripped of runtime data; edges keep their structural fields.
 */
export function serializeFlowForStorage(
  nodes: Node<NodeData>[],
  edges: Edge[]
): SerializedFlow {
  return {
    nodes: serializeNodesForStorage(nodes),
    edges: edges.map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle,
      targetHandle: e.targetHandle,
      type: e.type,
    })),
  };
}

// ============================================================
// 4. Hydrate a single node with latest nodeDefs
// ============================================================

/**
 * Restore runtime fields (nodeSpec, runtime state) on a deserialized node
 * using the latest nodeDefs. Returns a fully-hydrated ReactFlow Node.
 *
 * Missing node types are marked _invalid instead of crashing the canvas.
 */
export function hydrateNodeFromSpec(
  serialized: SerializedNode,
  nodeDefs: Record<string, NodeSpec>,
  positionOverride?: { x: number; y: number }
): Node<NodeData> {
  const opType = serialized.data?.opType;
  const latestSpec = opType ? nodeDefs[opType] : undefined;

  const baseData: NodeData = {
    opType: opType ?? '',
    values: serialized.data?.values ?? {},
    progress: 0,
    message: '',
    runState: undefined,
    progressRole: undefined,
    waitingFor: undefined,
    device: undefined,
    totalChunks: undefined,
    processedChunks: undefined,
    completedInferenceChunks: undefined,
    skippedChunks: undefined,
    failedChunks: undefined,
    executionId: undefined,
    _invalid: false,
    _warning: undefined,
    // Always set nodeSpec — from latest defs, or fallback for invalid types
    nodeSpec: latestSpec ?? {
      ...FALLBACK_NODE_SPEC,
      type: opType ?? '__unknown__',
      display_name: opType ?? 'Unknown Node',
    },
  };

  // If spec not in latest defs, mark node invalid
  if (!latestSpec && opType) {
    baseData._invalid = true;
    baseData._warning = `Node '${opType}' is no longer available`;
  }

  return {
    id: serialized.id,
    type: serialized.type ?? 'dynamic',
    position: positionOverride ?? serialized.position,
    data: baseData,
    selected: serialized.data?.selected ?? false,
  } as Node<NodeData>;
}

// ============================================================
// 5. Hydrate all nodes in a flow
// ============================================================

/**
 * Convert deserialized nodes (with no runtime data) into fully-hydrated
 * BrainFlow nodes using the latest nodeDefs.
 */
export function hydrateNodesWithLatestSpecs(
  serializedNodes: SerializedNode[],
  nodeDefs: Record<string, NodeSpec>
): Node<NodeData>[] {
  return serializedNodes.map(n => hydrateNodeFromSpec(n, nodeDefs));
}

// ============================================================
// 6. Validate edges against hydrated nodes
// ============================================================

/**
 * After hydrating nodes with latest specs, validate all edges and remove
 * those that are type-incompatible under the current frontend type system.
 *
 * Returns the filtered edges plus a report of what was removed and why.
 *
 * DASK_ARRAY[uint16] -> DASK_ARRAY[any] is always valid (any accepts all).
 * DaskTypeCast dynamic output resolves from values.target_dtype.
 */
export function validateEdgesWithLatestSpecs(
  edges: Edge[],
  nodes: Node<NodeData>[]
): { validEdges: Edge[]; removedCount: number; warnings: string[] } {
  const nodeMap = new Map(nodes.map(n => [n.id, n]));
  const warnings: string[] = [];

  const validEdges = edges.filter(edge => {
    const sourceNode = nodeMap.get(edge.source);
    const targetNode = nodeMap.get(edge.target);

    if (!sourceNode || !targetNode) {
      warnings.push(`Removed orphaned edge ${edge.id} (node not found)`);
      return false;
    }

    const sourceHandleIndex = parseInt(edge.sourceHandle || '0');
    const targetHandleName = edge.targetHandle;

    if (!targetHandleName) return true;

    const outputTypes = resolveNodeOutputTypes(sourceNode.data.nodeSpec, sourceNode.data.values);
    const outputType = outputTypes[sourceHandleIndex] || 'unknown';

    const targetSpec = targetNode.data.nodeSpec;
    const inputConfig = targetSpec?.input?.required?.[targetHandleName]
      || targetSpec?.input?.optional?.[targetHandleName];
    const inputType = Array.isArray(inputConfig) && typeof inputConfig[0] === 'string'
      ? inputConfig[0]
      : 'unknown';

    const result = canConnectPorts(outputType, inputType);
    if (!result.ok) {
      warnings.push(
        `Removed invalid connection ${edge.source}:${edge.sourceHandle} → ${edge.target}:${edge.targetHandle}: ${result.reason}`
      );
      return false;
    }
    return true;
  });

  return {
    validEdges,
    removedCount: edges.length - validEdges.length,
    warnings,
  };
}

// ============================================================
// 7. Full flow hydration — main entry point for load paths
// ============================================================

/**
 * Primary load path: takes raw stored/serialized flow data plus the latest
 * nodeDefs and returns fully-hydrated nodes and validated edges.
 *
 * This is the single entry point for:
 * - autosave restore
 * - workflow switch
 * - undo/redo restore
 * - import JSON
 * - clipboard paste (partial, see #9)
 */
export function hydrateFlowWithLatestSpecs(
  flow: { nodes: SerializedNode[]; edges: Edge[] },
  nodeDefs: Record<string, NodeSpec>
): HydrationResult {
  const warnings: string[] = [];

  const hydratedNodes = hydrateNodesWithLatestSpecs(flow.nodes, nodeDefs);

  const invalidNodeTypes = hydratedNodes
    .filter(n => n.data._invalid)
    .map(n => n.data.opType);

  const { validEdges, removedCount, warnings: edgeWarnings } = validateEdgesWithLatestSpecs(
    flow.edges,
    hydratedNodes
  );
  warnings.push(...edgeWarnings);

  return {
    nodes: hydratedNodes,
    edges: validEdges,
    invalidNodeTypes,
    removedEdges: removedCount,
    warnings,
  };
}

// ============================================================
// 8. Partial hydration for paste (only nodes, new IDs)
// ============================================================

/**
 * Hydrate pasted nodes (which already have new IDs) with fresh specs.
 * Unlike full flow hydration, this handles ID remapping and does not
 * validate edges — edges are mapped separately by the caller.
 */
export function hydratePastedNodes(
  serializedNodes: SerializedNode[],
  nodeDefs: Record<string, NodeSpec>
): Node<NodeData>[] {
  return serializedNodes.map(n => {
    const newPosition = {
      x: n.position.x + 30 + Math.random() * 20,
      y: n.position.y + 30 + Math.random() * 20,
    };
    return hydrateNodeFromSpec(n, nodeDefs, newPosition);
  });
}

// ============================================================
// 9. Parse any stored flow format (handles legacy with nodeSpec)
// ============================================================

/**
 * Parse stored JSON into a normalized SerializedFlow regardless of
 * whether it contains legacy `nodeSpec` fields or the new stripped format.
 *
 * Returns null if the format is completely unrecognizable.
 */
export function parseStoredFlow(raw: unknown): SerializedFlow | null {
  if (!raw || typeof raw !== 'object') return null;

  const obj = raw as Record<string, unknown>;
  const rawNodes = obj.nodes;
  if (!Array.isArray(rawNodes)) return null;

  const serializedNodes: SerializedNode[] = rawNodes.map((n: unknown) => {
    if (!n || typeof n !== 'object') return null;
    const node = n as Record<string, unknown>;
    const nodeData = node.data as Record<string, unknown> | undefined;
    return {
      id: String(node.id ?? ''),
      type: String(node.type ?? 'dynamic'),
      position: {
        x: Number((node.position as Record<string, unknown>)?.x ?? 0),
        y: Number((node.position as Record<string, unknown>)?.y ?? 0),
      },
      data: {
        opType: String(nodeData?.opType ?? ''),
        values: nodeData?.values as Record<string, unknown> | undefined,
        dimensions: nodeData?.dimensions as Record<string, number> | undefined,
        selected: Boolean(nodeData?.selected),
      },
    };
  }).filter(Boolean) as SerializedNode[];

  const rawEdges = obj.edges;
  const edges: Edge[] = Array.isArray(rawEdges) ? rawEdges.map((e: unknown) => {
    if (!e || typeof e !== 'object') return null;
    const edge = e as Record<string, unknown>;
    return {
      id: String(edge.id ?? ''),
      source: String(edge.source ?? ''),
      target: String(edge.target ?? ''),
      sourceHandle: edge.sourceHandle as string | null,
      targetHandle: edge.targetHandle as string | null,
      type: edge.type as string | undefined,
    };
  }).filter(Boolean) as Edge[] : [];

  return { nodes: serializedNodes, edges };
}
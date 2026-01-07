import React from 'react';
import { type Node, type Edge, type OnNodesChange, type OnEdgesChange, type Connection } from '@xyflow/react';
import type { NodeSpec, Workflow, LogEntry, BrainFlowNodeData } from '../types';

export interface FlowContextType {
  nodes: Node<BrainFlowNodeData>[];
  edges: Edge[];
  onNodesChange: OnNodesChange<Node<BrainFlowNodeData>>;
  onEdgesChange: OnEdgesChange<Edge>;
  setNodes: React.Dispatch<React.SetStateAction<Node<BrainFlowNodeData>[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  theme: 'light' | 'dark';
  toggleTheme: () => void;
  isConsoleOpen: boolean;
  toggleConsole: () => void;
  workflows: Workflow[];
  activeWorkflowId: string;
  createWorkflow: () => void;
  switchWorkflow: (id: string) => void;
  deleteWorkflow: (id: string) => void;
  renameWorkflow: (id: string, name: string) => void;
  saveCurrentWorkflow: () => void;
  nodeDefs: Record<string, NodeSpec>;
  isConnected: boolean;
  logs: LogEntry[];
  onConnect: (connection: Connection) => void;
  addNode: (type: string) => void;
  addNodeAt: (type: string, position: {x: number, y: number}) => void;
  isValidConnection: (connection: Connection | Edge) => boolean;
  updateNodeData: (id: string, data: Partial<BrainFlowNodeData>) => void;
  runFlow: () => void;
  clearLogs: () => void;
}

export const FlowContext = React.createContext<FlowContextType | null>(null);
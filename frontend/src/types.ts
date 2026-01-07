import type { Node, Edge } from '@xyflow/react';

// 1. 基础配置
export interface NodeInputConfig {
  [key: string]: [string, Record<string, unknown>?];
}

export interface NodeSpec {
  type: string;
  display_name: string;
  category: string;
  input: { required: NodeInputConfig; optional?: NodeInputConfig };
  output: string[];
}

// 2. 节点数据
export interface BrainFlowNodeData extends Record<string, unknown> {
  opType: string;
  nodeSpec: NodeSpec;
  values: Record<string, unknown>;
  progress: number;
  message?: string;
  updateValue?: (id: string, key: string, val: unknown) => void;
}

// 3. WebSocket 消息
export interface WSMessage {
  type: 'log' | 'progress' | 'error' | 'done';
  message?: string;
  taskId?: string;
  progress?: number;
}

// 4. 新增：工作流（用于多标签页）
export interface Workflow {
  id: string;
  name: string;
  nodes: Node<BrainFlowNodeData>[];
  edges: Edge[];
  timestamp: number;
}

// 5. 新增：日志条目
export interface LogEntry {
  id: string;
  timestamp: string;
  type: 'info' | 'success' | 'error' | 'warning';
  message: string;
}
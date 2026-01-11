import type { Node, Edge } from '@xyflow/react';

// === 基础配置 ===
export interface NodeInputConfig {
  [key: string]: [string, Record<string, unknown>?];
}

// === 节点的规格 (Node Specification) ===
export interface NodeSpec {
  type: string;  // 节点类型标识 (如 "LoadImage")
  display_name: string; // 显示名称 (如 "Load Image")
  category: string; // 分类目录 (如 "image/loaders")
  input: { required: NodeInputConfig; optional?: NodeInputConfig }; // 输入定义
  output: string[]; // 输出类型列表 (如 ["IMAGE", "MASK"])
}

// === 节点数据 (Node Data) ===
export interface NodeData extends Record<string, unknown> {
  opType: string;    // 操作类型 (对应 NodeSpec.type)
  nodeSpec: NodeSpec; // 完整的规格定义
  values: Record<string, unknown>; // 用户填写的参数值
  progress: number;  // 运行进度 (0-100)
  message?: string;  // 运行状态消息 (如 "Loading...")
  // 可选：用于直接更新值的辅助函数
  updateValue?: (id: string, key: string, val: unknown) => void;
}

// === WebSocket 消息结构 ===
export interface WSMessage {
  type: 'log' | 'progress' | 'error' | 'done' | 'executed';
  message?: string;
  taskId?: string; // 对应节点的 ID
  progress?: number;
}

// === 工作流 (Workflow) ===
export interface Workflow {
  id: string;
  name: string;
  nodes: Node<NodeData>[];
  edges: Edge[];
  timestamp: number;
}

// === 日志条目 ===
export interface LogEntry {
  id: string;
  timestamp: string;
  type: 'info' | 'success' | 'error' | 'warning';
  message: string;
}
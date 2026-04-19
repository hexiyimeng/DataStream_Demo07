import type { Node, Edge } from '@xyflow/react';

// === 进度类型 ===
export type ProgressType = 'chunk_count' | 'state_only' | 'stage_based';

// === 基础配置 ===
export interface NodeInputConfig {
  [key: string]: [string, Record<string, unknown>?];
}

// === 节点的规格 (Node Specification) ===
export interface NodeSpec {
  type: string;  // 节点类型标识 (如 "LoadImage") - 后端返回 "name" 和 "type" 都有
  name?: string;  // 后端也返回此字段，与 type 相同
  display_name: string; // 显示名称 (如 "Load Image")
  category: string; // 分类目录 (如 "image/loaders")
  description?: string; // 节点描述
  input: { required: NodeInputConfig; optional?: NodeInputConfig }; // 输入定义
  output: string[]; // 输出类型列表 (如 ["IMAGE", "MASK"])
  output_name?: string[]; // 输出插槽名称 (如 ["Image", "Alpha"])
  output_node?: boolean; // 是否为输出节点
  progress_type?: ProgressType; // 进度报告类型：chunk_count / state_only / stage_based
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
  // 可选：节点失效标记
  _invalid?: boolean; // 节点是否失效（后端已删除）
  _warning?: string;  // 失效警告消息
}

// === WebSocket 消息结构 ===
export interface WSMessage {
  type: 'log' | 'success' | 'warning' | 'progress' | 'error' | 'done' | 'executed' | 'execution_started' | 'subscribed';
  message?: string;
  taskId?: string; // 对应节点的 ID
  progress?: number | null; // 百分比进度 (0-100)，state_only 时为 null
  progressType?: ProgressType; // 进度类型：chunk_count / state_only / stage_based
  executionId?: string;
  status?: 'running' | 'failed' | 'cancelled'; // execution_finished 的状态
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
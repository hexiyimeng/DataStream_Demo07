import type { Node, Edge } from '@xyflow/react';

// === 进度类型 ===
export type ProgressType = 'chunk_count' | 'state_only' | 'stage_based';

// === 执行时状态 (runState) ===
export type RunState = 'ready' | 'submitted' | 'running' | 'done' | 'failed' | 'cancelled';

// === 节点进度角色 (progressRole) ===
export type ProgressRole = 'chunk_sink' | 'chunk_intermediate' | 'stage_sink' | 'state_only';

// === Execution 全局阶段 ===
export type ExecutionPhase =
  | 'idle'
  | 'graph_building'
  | 'submitted'
  | 'running'
  | 'cancelling'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

// === WebSocket 连接状态 ===
export type WebSocketStatus = 'connected' | 'reconnecting' | 'disconnected';

// === 节点运行时数据 (NodeData 的运行时扩展) ===
export interface NodeRuntimeData {
  runState?: RunState;
  progressRole?: ProgressRole;
  waitingFor?: string[];
  device?: string;
  totalChunks?: number;
  processedChunks?: number;
  completedInferenceChunks?: number;
  skippedChunks?: number;
  failedChunks?: number;
  executionId?: string | null;
}

// === Execution 全局运行时状态 ===
export interface ExecutionRuntimeState {
  phase: ExecutionPhase;
  executionId: string | null;
  startedAt: number | null;
  finishedAt: number | null;
  totalNodes: number;
  lastError: string | null;
  // 汇总的 chunk 统计（来自所有 CHUNK_COUNT 节点）
  totalChunks: number;
  processedChunks: number;
  completedInferenceChunks: number;
  skippedChunks: number;
  failedChunks: number;
}

// === 基础配置 ===
export interface NodeInputConfig {
  [key: string]: [string, Record<string, unknown>?];
}

// === 节点的规格 (Node Specification) ===
export interface NodeSpec {
  type: string;  // 节点类型标识 (如 "LoadImage")
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
  // --- 运行时状态字段 ---
  runState?: RunState;
  progressRole?: ProgressRole;
  waitingFor?: string[];
  device?: string;
  totalChunks?: number;
  processedChunks?: number;
  completedInferenceChunks?: number;
  skippedChunks?: number;
  failedChunks?: number;
  executionId?: string | null;
  // 可选：用于直接更新值的辅助函数
  updateValue?: (id: string, key: string, val: unknown) => void;
  // 可选：节点失效标记
  _invalid?: boolean;
  _warning?: string;
}

// === WebSocket 消息结构 ===
// execution_finished 是唯一权威终态事件
// done / error 仅作为 legacy compatibility，不再作为终态收口依据
export type WSMessageType =
  | 'log'
  | 'success'
  | 'warning'
  | 'progress'
  | 'error'           // legacy compatibility
  | 'done'            // legacy compatibility
  | 'executed'
  | 'execution_started'
  | 'execution_finished'   // 唯一权威终态事件
  | 'subscribed'
  | 'ping'
  | 'pong'
  | 'execution_snapshot';   // 重连恢复时后端发送的全量状态快照

export interface WSMessage {
  type: WSMessageType;
  message?: string;
  taskId?: string;
  progress?: number | null;
  progressType?: ProgressType;
  executionId?: string;

  // --- progress 消息的完整字段 ---
  runState?: RunState;
  progressRole?: ProgressRole;
  waitingFor?: string[];
  device?: string;
  totalChunks?: number;
  processedChunks?: number;
  completedInferenceChunks?: number;
  skippedChunks?: number;
  failedChunks?: number;

  // --- execution_finished / execution_snapshot ---
  status?: 'running' | 'succeeded' | 'failed' | 'cancelled';
  createdAt?: number;
  finishedAt?: number;
  nodeCount?: number;
  logCount?: number;
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

# BrainFlow Architecture

BrainFlow is a distributed, graph-based, large-scale data processing application focused on block-wise image and array processing.

The project is designed for datasets that are too large to load into memory, with a long-term target of PB-scale workflows. It uses Dask as the lazy distributed execution layer and provides a visual workflow editor so algorithm engineers can compose processing pipelines without manually managing scheduling, chunk progress, GPU model lifetime, WebSocket state, or OME-Zarr I/O.

The most important design goal is:

> Algorithm engineers should be able to write block-level algorithms and let BrainFlow handle distributed execution, progress tracking, large-data I/O, and runtime orchestration.

---

## 1. System Goals

### 1.1 Primary goals

BrainFlow should support:

- Very large 2D / 3D / 4D / 5D image and array datasets.
- Lazy Dask execution, avoiding full materialization in memory.
- OME-Zarr input and output.
- Block-wise custom algorithms written as simple NumPy functions.
- GPU-aware segmentation and inference nodes, including Cellpose.
- Visual DAG construction in the frontend.
- Real-time execution progress over WebSocket.
- Execution cancellation, reconnect, and history recovery.
- Plugin-based node discovery.
- Typed ports so invalid workflow connections are rejected early.

### 1.2 Non-goals

BrainFlow should not become:

- A framework where normal nodes eagerly call `.compute()` on large arrays.
- A framework where every algorithm engineer must understand Dask internals.
- A single-purpose Cellpose wrapper.
- A frontend-only DAG editor without backend execution guarantees.
- A system that silently downcasts, rechunks, or materializes data without explicit reason.

---

## 2. High-Level Architecture

BrainFlow consists of four major layers:

```text
Frontend React App
  - ReactFlow visual editor
  - Dynamic node UI generated from backend node specs
  - WebSocket execution control and progress display
  - Workflow save/load/autosave

FastAPI Backend
  - HTTP node-definition and dashboard endpoints
  - WebSocket execution endpoint
  - Execution session state and history

Execution / Dask Layer
  - DAG validation
  - Lazy graph construction
  - Unified sink submission
  - Dask queue-based progress monitoring
  - Cancellation and cleanup

Node / Algorithm Layer
  - OME-Zarr reader/writer
  - BlockMap abstraction
  - Cellpose segmentation
  - ROI / type cast / scale-shift / debug nodes
  - Instance post-processing and statistics
```

End-to-end flow:

```text
User builds graph in ReactFlow
        ↓
Frontend serializes nodes + edges into backend DAG JSON
        ↓
WebSocket sends execute_graph command
        ↓
Backend validates graph topology and types
        ↓
Backend executes node methods only to build lazy Dask graph
        ↓
Output nodes return delayed sink tasks
        ↓
Executor submits delayed sinks to Dask
        ↓
Dask workers execute chunk/block tasks
        ↓
Workers report progress through execution-scoped Dask queues
        ↓
Backend broadcasts progress over WebSocket
        ↓
Frontend updates node state, logs, global execution state
```

---

## 3. Repository Layout

The current backend is organized around these directories and modules:

```text
api/
  http_routes.py        # REST endpoints: object_info, dashboard_url
  websocket.py          # WebSocket execution endpoint

core/
  auth.py               # HTTP and WebSocket API key verification
  chunk_policy.py       # Dask chunk recommendation and safe rechunk helpers
  config.py             # Hardware-aware app configuration
  logger.py             # Global logging setup
  registry.py           # Node registration and frontend node spec generation
  state_manager.py      # Execution session state, logs, subscribers, cleanup
  type_system.py        # Port type parsing and compatibility rules

services/
  dask_service.py       # LocalCluster lifecycle, memory limits, GPU worker binding
  executor.py           # DAG validation, lazy graph build, submit, progress, cleanup
  plugin_loader.py      # Recursive node plugin import

nodes/
  block_map.py          # BaseBlockMapNode and SegmentationBlockMapNode
  cellpose_node.py      # Cellpose model cache and DaskCellpose node
  debug_nodes.py        # Debug chunk marker node
  instance_nodes.py     # Per-chunk instance table extraction
  ome_zarr_flow.py      # OME-Zarr reader and writer
  post_processing_nodes.py # DaskStats and deprecated label stitcher
  roi_node.py           # ROI crop node
  scale_shift_node.py   # Debug scale/shift node
  type_cast_node.py     # Dask dtype conversion node

utils/
  chunk_helpers.py      # Chunk origin and adjacency helpers
  memory_monitor.py     # Process / Dask worker / GPU memory snapshots
  progress_helper.py    # Execution-scoped Dask queue progress reporting
  union_find.py         # Union-Find for instance reconciliation

main.py                 # FastAPI app, plugin startup, Dask startup, static frontend
```

The current frontend is organized around these files:

```text
App.tsx                 # Main shell layout: Header, Sidebar, FlowEditor, BottomPanel
main.tsx                # React root, ReactFlowProvider, FlowProvider

types.ts                # Shared frontend protocol types and runtime states

context/
  FlowContext.tsx       # Global graph/UI/execution state composition
  FlowContextDef.tsx    # Context interface definition

components/
  flow/
    FlowEditor.tsx      # ReactFlow canvas, drop handling, context menu
    DynamicNode.tsx     # Dynamic node rendering from NodeSpec
    ContextMenu.tsx     # Right-click node creation menu

  layout/
    Header.tsx          # Workflow tabs, run/stop, save/load, dashboard link
    Sidebar.tsx         # Node library tree/search and drag source
    BottomPanel.tsx     # Logs, execution status, chunk statistics

hooks/
  useFlowEngine.ts      # WebSocket, run/stop, node definitions, execution reducer
  useFlowOperations.ts  # Copy/paste/delete and keyboard shortcuts
  useWorkflows.ts      # Multi-workflow tab management
  useAutoSave.ts       # LocalStorage graph persistence
  useUndoRedo.ts       # Debounced undo/redo stack
  useFlowContext.ts    # Context accessor hook

utils/
  portTypes.ts          # Frontend port type parsing and connection validation

ui/
  SectionHeader.tsx
  TextField.tsx
```

---

## 4. Backend Architecture

### 4.1 FastAPI startup lifecycle

`main.py` creates the FastAPI application and uses a lifespan context manager.

Startup order is intentionally important:

1. Load backend node plugins.
2. Start the Dask LocalCluster.
3. Register routes and serve frontend static files.

This order avoids some GPU initialization conflicts by importing torch/cellpose-related plugins before workers begin executing heavy workloads.

Shutdown order:

1. Record shutdown log.
2. Stop Dask cluster.
3. Clear model cache where possible.

### 4.2 Plugin loading

`services/plugin_loader.py` recursively imports Python files under `nodes/`.

Critical plugins currently include:

- `nodes.cellpose_node`
- `nodes.ome_zarr_flow`
- `nodes.post_processing_nodes`
- `nodes.instance_nodes`

If a critical plugin fails, startup fails fast. Non-critical plugin failures are logged as warnings, allowing degraded startup.

This protects the system from half-working states where the UI loads but core nodes are missing.

### 4.3 Node registry

`core/registry.py` provides:

- `@register_node(name)` decorator.
- Global `NODE_CLASS_MAPPINGS`.
- Global `NODE_DISPLAY_NAME_MAPPINGS`.
- `get_node_info()` endpoint payload generator.
- `ProgressType` enum:
  - `CHUNK_COUNT`
  - `STATE_ONLY`
  - `STAGE_BASED`

The frontend calls `/object_info`, receives node metadata, and renders the visual node library and dynamic node UI.

A user-facing node should define:

```python
CATEGORY = "BrainFlow/..."
DISPLAY_NAME = "..."
DESCRIPTION = "..."
PROGRESS_TYPE = ProgressType.STATE_ONLY | ProgressType.CHUNK_COUNT | ProgressType.STAGE_BASED
INPUT_TYPES = classmethod(...)
RETURN_TYPES = (...,)
RETURN_NAMES = (...,)
FUNCTION = "execute"
OUTPUT_NODE = True | False
```

---

## 5. Execution Model

### 5.1 Execution session isolation

Every run has an `execution_id`.

The execution ID is generated by the frontend with `crypto.randomUUID()` and sent to the backend in the WebSocket `execute_graph` command. The backend can also generate one if not provided.

Execution state is stored in `core/state_manager.py` as `ExecutionSession`:

```text
execution_id
status
asyncio task
node_states
logs
subscribers
created_at
finished_at
```

The state manager isolates:

- progress state
- logs
- WebSocket subscribers
- cancellation state
- reconnect snapshots

No progress queue or WebSocket message should be shared across executions unless explicitly intended.

### 5.2 Backend WebSocket protocol

The main WebSocket endpoint is:

```text
/ws/run
```

Supported client commands:

```json
{ "command": "execute_graph", "graph": { ... }, "executionId": "..." }
{ "command": "stop_execution" }
{ "command": "subscribe", "executionId": "..." }
{ "command": "ping" }
{ "command": "pong" }
```

Important server messages:

```text
execution_started       # Backend accepted a run and assigned executionId
progress                # Node-level progress update
execution_snapshot      # Reconnect state recovery
execution_control_ack   # Stop/cancel acknowledgement
execution_finished      # Authoritative terminal event
log / success / warning / error
ping / pong
```

`execution_finished` is the authoritative final event. Legacy `done` and `error` events may exist for compatibility, but frontend state should converge on `execution_finished`.

### 5.3 Executor phases

`services/executor.py` is the backend execution core.

It has two conceptual phases:

#### Phase 1 — GraphBuilding

The executor:

1. Validates the graph is acyclic.
2. Validates port type compatibility.
3. Recursively schedules required upstream nodes.
4. Calls node methods to build lazy objects.
5. Collects Dask arrays and delayed sink outputs.

Critical rule:

> Phase 1 must not compute large data.

Node methods should return lazy Dask arrays, delayed objects, or small metadata. They should not call `.compute()` on full arrays.

#### Phase 2 — Submit / Running

The executor:

1. Finds output nodes.
2. Collects delayed sink tasks.
3. Submits all sink tasks to Dask together.
4. Starts progress queue monitors.
5. Waits for Dask futures.
6. Collects final results.
7. Broadcasts terminal state.
8. Cleans resources.

This design allows multiple output nodes to share one lazy graph and reduces duplicate upstream computation.

---

## 6. Graph Validation

### 6.1 Acyclic validation

`validate_graph_acyclic()` performs DFS over node dependencies and rejects cycles.

A dependency is represented by an input value shaped like:

```json
["source_node_id", output_index]
```

### 6.2 Type validation

`validate_graph_types()` checks each edge against declared node port types.

Backend type compatibility is implemented in `core/type_system.py`.

Frontend type compatibility is mirrored in `utils/portTypes.ts`.

The current Dask array type format is:

```text
DASK_ARRAY[uint8]
DASK_ARRAY[uint16]
DASK_ARRAY[float32]
DASK_ARRAY[any]
DASK_ARRAY[same]
```

The frontend and backend should remain consistent. If the backend type system changes, `portTypes.ts` should be updated in the same change.

---

## 7. Node Model

### 7.1 Standard node

A standard node is a registered class with a method such as `execute()`.

It may return:

- Small Python values.
- Metadata dictionaries.
- Dask arrays.
- Dask delayed tasks.
- Output progress metadata.

### 7.2 Output node

An output node has:

```python
OUTPUT_NODE = True
```

Output nodes should normally return a Dask delayed sink task. The executor submits these delayed sink tasks in Phase 2.

Examples:

- `OMEZarrWriter`
- `DaskStats`

### 7.3 BlockMap node

`nodes/block_map.py` provides the most important abstraction for algorithm engineers.

A block-wise node should usually subclass `BaseBlockMapNode` and define:

```python
PROCESS_BLOCK = my_block_function
RETURN_TYPES = (...,)
RETURN_NAMES = (...,)
INPUT_TYPES = classmethod(...)
```

The block function receives one NumPy block and returns one NumPy block.

Example mental model:

```python
def my_algorithm_block(block: np.ndarray, param_a: float, ctx) -> np.ndarray:
    # Operate on one block only.
    # Do not assume global dataset is available.
    return result
```

The framework handles:

- `map_blocks`
- dtype resolution
- input parameter coercion
- skip policy
- failure policy
- block context
- progress reporting
- device hint

### 7.4 Block context

`BlockContext` provides runtime metadata:

```text
node_id
execution_id
device
block_info
block_location
block_shape
input_dtype
resources
```

The context allows block functions to access execution-scoped metadata without needing to understand the full executor.

### 7.5 Block shape and dtype rules

By default, a `BaseBlockMapNode` requires:

- output is `np.ndarray`
- output shape equals input block shape
- output ndim equals input block ndim
- output dtype matches declared return type when constrained

To intentionally change block shape, a node must explicitly set:

```python
ALLOW_SHAPE_CHANGE = True
```

This should be rare and must be documented carefully.

---

## 8. Dask and Chunking Architecture

### 8.1 Chunk policy

`core/chunk_policy.py` centralizes chunk decisions.

Design principles:

1. Readers decide initial working chunks.
2. Operators should preserve upstream chunks when possible.
3. Writers should rechunk only when necessary.
4. All rechunk operations should have a recorded reason.
5. Chunk risk should be assessed for memory-heavy and GPU-heavy workloads.

### 8.2 Why chunking is central

For PB-scale goals, performance depends on:

- number of Dask tasks
- per-task memory footprint
- GPU VRAM pressure
- spill behavior
- scheduler overhead
- avoiding duplicate upstream work
- avoiding unnecessary rechunking

Chunk sizes that are too small can create millions of Dask tasks.
Chunk sizes that are too large can crash workers or GPUs.

### 8.3 Reader behavior

`OMEZarrReader`:

- validates path
- opens direct Zarr arrays or OME-Zarr groups
- reads multiscales metadata when available
- determines axes and voxel size
- creates Dask array using target chunks
- returns `(dask_arr, metadata)`

Reader output is lazy.

### 8.4 Writer behavior

`OMEZarrWriter`:

- prepares compressor
- initializes output Zarr store
- converts Dask array blocks to delayed objects
- creates one delayed write task per block
- finalizes OME-NGFF metadata after all block writes complete
- returns a delayed sink task plus sink progress metadata

Writer graph construction should not compute the data.

Important correctness rule:

> Region writes must use actual chunk sizes, especially at array boundaries where the final chunk may be smaller than `arr.chunksize`.

This is a high-priority area to validate and test.

---

## 9. Progress Architecture

### 9.1 Progress types

Backend node classes declare one of three progress types:

```text
CHUNK_COUNT
STATE_ONLY
STAGE_BASED
```

Frontend type equivalents:

```ts
type ProgressType = 'chunk_count' | 'state_only' | 'stage_based';
```

### 9.2 Progress roles

The executor enriches progress messages with a `progressRole`:

```text
chunk_intermediate  # chunk-count node that is not an output sink
chunk_sink          # chunk-count output node
stage_sink          # stage-based output node
state_only          # non-percent node
```

The frontend uses this to decide what to display.

For example, `DynamicNode` only shows percentage bars for sink-like roles:

```text
chunk_sink
stage_sink
```

Intermediate nodes can still pulse or show state without becoming the main progress source.

### 9.3 Queue-based chunk progress

Chunk progress is reported from workers through `distributed.Queue`.

Queue names are execution-scoped:

```text
queue_<execution_id>_<node_id>
stage_queue_<execution_id>_<node_id>
```

This prevents progress from one run leaking into another run.

Chunk progress payload:

```json
{ "type": "completed" }
{ "type": "skipped" }
{ "type": "failed" }
```

The executor aggregates:

```text
totalChunks
processedChunks
completedInferenceChunks
skippedChunks
failedChunks
```

### 9.4 Stage progress

Stage-based nodes, such as `DaskStats`, report progress through `report_stage_progress()`.

The intended model is:

```text
Phase 1: boundary reconcile
Phase 2: merge graph
Phase 3: partial aggregation
Phase 4: final reduce / CSV write
```

The executor should monitor stage queues for `STAGE_BASED` output nodes during Phase 2. This is an important integration point to keep correct.

---

## 10. Memory and GPU Architecture

### 10.1 App configuration

`core/config.py` detects:

- CPU count
- system memory
- GPU availability
- GPU count
- GPU VRAM

It derives:

- number of workers
- chunk multiplier
- worker memory limit defaults
- Dask local spill directory
- dashboard host override

All major values can be overridden through environment variables.

### 10.2 Dask service

`services/dask_service.py` starts a `LocalCluster`.

Modes:

```text
multi-GPU process mode
single-GPU protection mode
CPU mode
```

Memory thresholds are configured for:

```text
target
spill
pause
terminate
```

Dask spill directory is configured to avoid uncontrolled temp behavior.

### 10.3 GPU assignment

For Windows multi-GPU process mode, a worker plugin assigns workers to GPUs:

```text
worker.assigned_gpu = cuda:<index>
```

BlockMap nodes resolve device hints from the current worker when available.

### 10.4 Cellpose model cache

`nodes/cellpose_node.py` implements a worker-level Cellpose model cache with:

- singleton cache object
- lock protection
- resolved model aliases
- reference counting
- safe clear
- force clear

Critical rule:

> Cellpose models should not be reloaded for every block.

High-frequency GPU cache clearing is avoided by default because it can hurt throughput.

---

## 11. OME-Zarr and Bioimage Data Model

### 11.1 Input

The reader supports:

- direct Zarr array paths
- OME-Zarr group paths
- multiscales metadata
- dataset path discovery
- axes fallback
- voxel size extraction

Default dimension conventions:

```text
2D: Y, X
3D: Z, Y, X
4D: C, Z, Y, X
5D: T, C, Z, Y, X
```

### 11.2 Output

The writer outputs a Zarr group with dataset path:

```text
0
```

It writes OME-NGFF-style multiscales metadata after all blocks are written.

### 11.3 Metadata propagation

The executor appends upstream metadata dictionaries to downstream node outputs when a node does not return metadata itself.

This allows writer nodes to preserve axes and voxel size when possible.

---

## 12. Cellpose and Instance Analysis Pipeline

The current segmentation/statistics pipeline is:

```text
OMEZarrReader
  ↓
Optional ROI / TypeCast / ScaleShift / Debug nodes
  ↓
DaskCellpose
  ↓
CellposePostProcessor
  ↓
DaskStats
```

### 12.1 DaskCellpose

`DaskCellpose` is a `SegmentationBlockMapNode`.

It runs Cellpose per block and returns `uint16` masks.

Segmentation block behavior:

- skips empty blocks
- skips all-zero blocks
- returns zero mask on block failure when failure policy is `zeros_like`
- uses model cache for worker-local model reuse

### 12.2 Instance table extraction

`CellposePostProcessor` converts mask chunks into delayed per-chunk DataFrames.

It extracts:

- local instance ID
- centroid
- voxel count
- chunk boundary contact
- neighboring chunk hints
- bounding box

It returns an `INSTANCE_TABLE` payload:

```text
partitions
numblocks
adjacency_edges
resolution_um
```

### 12.3 DaskStats

`DaskStats` is a 4-phase delayed pipeline:

1. Pairwise reconcile adjacent chunk boundaries.
2. Global Union-Find merge map.
3. Distributed partial aggregation per partition.
4. Tree-reduce and final CSV write.

Important scaling note:

> `DaskStats` contains global barriers. This is acceptable for metadata-scale instance tables, but it should never collect full image arrays.

---

## 13. Frontend Architecture

### 13.1 Main app shell

`App.tsx` renders the main layout:

```text
Header
Sidebar
FlowEditor
BottomPanel
```

`main.tsx` wraps the app with:

```text
ReactFlowProvider
FlowProvider
```

### 13.2 FlowProvider

`FlowContext.tsx` is the central frontend composition point.

It owns:

- ReactFlow nodes and edges
- node definitions fetched from backend
- logs
- theme
- console visibility
- execution state
- WebSocket status
- workflows
- undo/redo
- autosave
- graph operations
- connection validation

It also locks graph editing while execution is active.

Execution-locked phases:

```text
graph_building
submitted
running
cancelling
```

During these phases, add/connect/delete/copy/paste operations are blocked.

### 13.3 useFlowEngine

`useFlowEngine.ts` is the frontend protocol core.

Responsibilities:

- fetch `/object_info`
- open `/ws/run`
- reconnect after unintentional disconnect
- restore execution using `sessionStorage` execution ID
- parse WebSocket messages
- maintain global execution reducer state
- buffer progress messages and apply node updates every 100ms
- serialize ReactFlow graph into backend DAG JSON
- send `execute_graph`
- send `stop_execution`

Run command payload shape:

```json
{
  "command": "execute_graph",
  "executionId": "...",
  "graph": {
    "node_id": {
      "type": "NodeType",
      "inputs": {
        "param": "value",
        "linked_input": ["source_node_id", 0]
      }
    }
  }
}
```

### 13.4 Dynamic node UI

`DynamicNode.tsx` renders nodes from backend-provided `NodeSpec`.

It separates inputs into:

- link inputs
- widgets
- outputs

Widget controls are inferred from backend input declarations:

```text
INT
FLOAT
STRING
BOOLEAN
LONG
dropdown arrays
```

Connection handles are colored by port type.

Runtime status is displayed using:

```text
runState
progressRole
progress
message
waitingFor
```

### 13.5 Sidebar and context menu

`Sidebar.tsx` and `ContextMenu.tsx` both build a category tree from backend node definitions.

Nodes can be added by:

- clicking in Sidebar
- dragging from Sidebar into FlowEditor
- right-click context menu

### 13.6 Header

`Header.tsx` manages:

- run / stop buttons
- workflow tabs
- workflow save/load JSON
- dashboard URL lookup
- theme toggle

When loading a saved workflow, it checks current `nodeDefs` and marks missing node types invalid.

### 13.7 BottomPanel

`BottomPanel.tsx` displays:

- logs
- execution phase
- WebSocket status
- current execution ID
- chunk statistics
- stop button

This is the main operator-facing runtime panel.

---

## 14. Frontend State Model

### 14.1 NodeData

Frontend node data includes both persistent graph data and runtime data.

Persistent data:

```text
opType
nodeSpec
values
```

Runtime data:

```text
progress
message
runState
progressRole
waitingFor
device
totalChunks
processedChunks
completedInferenceChunks
skippedChunks
failedChunks
executionId
_invalid
_warning
```

Runtime data should generally be cleared when:

- loading from autosave
- pasting nodes
- starting a new execution
- importing workflows

### 14.2 ExecutionRuntimeState

Global execution state tracks:

```text
phase
executionId
startedAt
finishedAt
totalNodes
lastError
totalChunks
processedChunks
completedInferenceChunks
skippedChunks
failedChunks
```

Phases:

```text
idle
graph_building
submitted
running
cancelling
succeeded
failed
cancelled
```

### 14.3 Progress buffering

The frontend buffers progress updates in `progressBufferRef` and flushes them every 100ms.

This avoids excessive React re-renders during high-frequency chunk progress.

When modifying progress UI, preserve this buffered update model.

---

## 15. Frontend / Backend Protocol Contract

### 15.1 Node definition contract

Backend `/object_info` returns a map of node type to node spec:

```ts
interface NodeSpec {
  type: string;
  name?: string;
  display_name: string;
  category: string;
  description?: string;
  input: {
    required: NodeInputConfig;
    optional?: NodeInputConfig;
  };
  output: string[];
  output_name?: string[];
  output_node?: boolean;
  progress_type?: ProgressType;
}
```

Changing this protocol requires coordinated frontend and backend updates.

### 15.2 Edge contract

Frontend edges serialize to backend inputs as:

```json
"input_name": ["source_node_id", output_index]
```

Backend validates the source output type and target input type before graph construction.

### 15.3 Progress contract

Backend sends:

```json
{
  "type": "progress",
  "taskId": "node_id",
  "executionId": "...",
  "progressType": "chunk_count",
  "progress": 42,
  "message": "...",
  "runState": "running",
  "progressRole": "chunk_sink",
  "totalChunks": 100,
  "processedChunks": 42,
  "completedInferenceChunks": 30,
  "skippedChunks": 12,
  "failedChunks": 0
}
```

The frontend should treat missing optional fields as unknown, not as errors.

### 15.4 Terminal event contract

Backend sends:

```json
{
  "type": "execution_finished",
  "executionId": "...",
  "status": "succeeded" | "failed" | "cancelled",
  "message": "..."
}
```

Frontend should remove the stored execution ID and stop edge animations on terminal events.

---

## 16. Security and Deployment Notes

### 16.1 API keys

Backend auth supports `BRAINFLOW_API_KEYS`.

If configured:

- HTTP endpoints require `Authorization: Bearer <key>`.
- WebSocket requires `token` or `api_key` query parameter.

The current frontend default requests do not provide API keys. Production deployment with API keys must add frontend auth configuration.

### 16.2 CORS

Allowed origins are configured through:

```text
BRAINFLOW_ALLOWED_ORIGINS
```

Default origins include local Vite dev ports.

### 16.3 Dashboard URL

`/dashboard_url` returns the Dask dashboard URL and supports host override through:

```text
BRAINFLOW_DASHBOARD_HOST
```

This is useful behind reverse proxies or remote deployments.

---

## 17. Large-Data Design Rules

These rules are architecture-level constraints.

### 17.1 Never compute full arrays in normal nodes

Avoid:

```python
arr.compute()
np.asarray(dask_arr)
list(dask_arr)
```

inside ordinary processing nodes.

Allowed only when:

- the node is explicitly an output sink
- the data is known to be tiny metadata
- the computation is intentionally delayed and submitted by the executor

### 17.2 Prefer block-local operations

Algorithm nodes should operate on one block at a time.

Preferred patterns:

```python
dask_arr.map_blocks(...)
dask.delayed(...)
BaseBlockMapNode
SegmentationBlockMapNode
```

### 17.3 Avoid uncontrolled rechunking

Rechunking can duplicate work and create large graph expansions.

If rechunking is necessary:

- use `safe_rechunk`
- record reason
- log location
- consider memory risk

### 17.4 Keep global barriers explicit

Some algorithms require global metadata barriers, such as Union-Find over instance adjacency.

When adding one, document:

- what data is gathered
- why it is safe
- how memory scales
- expected bottleneck

### 17.5 Treat edge chunks carefully

Dask boundary chunks are often smaller than the nominal chunksize.

Any block-region writer or coordinate logic must use actual chunk metadata, not just `arr.chunksize`.

---

## 18. Known Architectural Risks and Improvement Areas

This section records important areas to improve or verify.

### 18.1 OME-Zarr writer boundary chunks

The writer must ensure region slices match actual chunk sizes. Last chunks can be smaller than `arr.chunksize`.

Recommended fix:

- compute each region end using `arr.chunks[axis][block_idx[axis]]`
- add a synthetic test with uneven array shape and chunks

### 18.2 Stage progress monitor integration

`DaskStats` uses stage progress reporting. The executor should ensure `STAGE_BASED` sinks start a stage monitor during the Dask submit phase.

Recommended fix:

- start `monitor_stage_progress()` for `ProgressType.STAGE_BASED` output nodes
- verify frontend receives `stage_sink` progress updates

### 18.3 Worker-side Cellpose cache clearing

Cellpose models may live inside Dask worker processes, not only the main backend process.

Recommended fix:

- call worker-local cache clear through `client.run(...)` when stopping cluster or when safe
- keep main-process clear as fallback

### 18.4 Frontend API-key support

If `BRAINFLOW_API_KEYS` is enabled, frontend HTTP and WebSocket calls need token support.

Recommended fix:

- add `VITE_BRAINFLOW_API_KEY` or equivalent secure runtime config
- send `Authorization` header for HTTP
- append `token` query parameter for WebSocket

### 18.5 Progress buffer update condition

The frontend progress flush currently compares only runState, progress, and message before deciding whether to update a node.

If chunk counters or device fields change without those three changing, the UI may skip a needed update.

Recommended fix:

- include runtime counter fields in the change detection
- or always update nodes with buffered progress payloads, while keeping the 100ms batching

### 18.6 Runtime state and undo history

Undo/redo is designed to avoid recording active execution updates. However, final runtime states such as `done`, `failed`, or `cancelled` may still enter history after execution ends.

Recommended fix:

- exclude runtime-only fields from undo snapshots
- or maintain a sanitized graph state for editing history

### 18.7 2D / 3D / 4D / 5D post-processing boundaries

Instance extraction and statistics should clearly define supported dimensionality.

Recommended fix:

- explicitly support 2D and 3D
- reject unsupported higher-dimensional masks with a clear error
- or define how C/T dimensions are split before instance analysis

### 18.8 DaskStats global barrier scaling

`DaskStats` is metadata-based but still has global barriers.

Recommended improvement:

- expose diagnostics for number of partitions, adjacency edges, and candidate merge edges
- warn when boundary edge count may be too high

---

## 19. Development Workflow

### 19.1 Before changing code

For non-trivial changes:

1. Identify whether the change touches frontend, backend, executor, nodes, or protocol.
2. Check whether `/object_info` protocol changes.
3. Check whether WebSocket message schema changes.
4. Check whether lazy execution is preserved.
5. Check whether execution ID isolation is preserved.
6. Add or describe tests.

### 19.2 Recommended test categories

Backend tests:

- graph acyclicity validation
- graph type validation
- port dtype compatibility
- BlockMap shape/dtype validation
- chunk origin and adjacency helpers
- OME-Zarr reader metadata parsing
- OME-Zarr writer uneven boundary chunks
- progress queue names include execution ID
- state manager transition rules
- cancellation cleanup

Frontend tests:

- graph serialization from nodes/edges
- port compatibility matching backend rules
- execution reducer transitions
- WebSocket message handling
- progress buffer flushing
- runtime state cleanup on load/paste
- workflow save/load compatibility

Integration tests:

- synthetic Dask array through reader-like node to writer
- DaskCellpose can be mocked with simple block function
- DaskStats on small artificial labeled chunks
- reconnect and subscribe snapshot flow

### 19.3 Manual smoke test

A minimal smoke workflow:

```text
OMEZarrReader
  ↓
DaskChunkMarker or DaskScaleShift
  ↓
OMEZarrWriter
```

Expected behavior:

- nodes render from `/object_info`
- graph validates in frontend
- backend accepts `execute_graph`
- execution ID appears in BottomPanel
- writer reports chunk progress
- output Zarr is written
- `execution_finished` arrives
- edge animation stops

---

## 20. How to Add a New Block Algorithm Node

A new block-wise algorithm should follow this pattern:

```python
import numpy as np
from core.registry import register_node, ProgressType
from nodes.base import BaseBlockMapNode


def my_block(block: np.ndarray, scale: float, ctx) -> np.ndarray:
    return (block.astype(np.float32, copy=False) * scale).astype(np.float32, copy=False)


@register_node("DaskMyAlgorithm")
class DaskMyAlgorithm(BaseBlockMapNode):
    CATEGORY = "BrainFlow/Algorithms"
    DISPLAY_NAME = "My Algorithm"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "scale": ("FLOAT", {"default": 1.0, "min": 0.0, "max": 10.0}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[float32]",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "execute"

    PROCESS_BLOCK = my_block
```

Rules:

- Do not call `.compute()`.
- Do not open global files from inside every block unless necessary.
- Do not load a GPU model per block; use a cache/resource abstraction.
- Return the declared dtype.
- Return the same shape unless explicitly documented.
- Use typed ports when possible.

---

## 21. Roadmap Recommendation

### Phase 1 — Correctness and stability

- Fix OME-Zarr writer boundary chunks.
- Wire stage progress monitor for `DaskStats`.
- Clear Cellpose cache on workers.
- Harden frontend progress update detection.
- Add minimal backend tests for chunk helpers and writer region logic.

### Phase 2 — Developer experience

- Add `HOW_TO_WRITE_NODE.md`.
- Add node template files.
- Add synthetic demo workflows.
- Add small test Zarr fixture generation.
- Improve error messages surfaced to frontend.

### Phase 3 — Large-data diagnostics

- Add graph size diagnostics.
- Add chunk count warnings before submit.
- Add worker memory / GPU memory panel.
- Add Dask dashboard deep link handling.
- Add run summary export.

### Phase 4 — Production readiness

- Add frontend API-key support.
- Add persistent execution history.
- Add workflow schema versioning.
- Add backend config documentation.
- Add deployment profiles for local workstation, single GPU server, multi-GPU server, and cluster mode.

---

## 22. Architectural Invariants

These invariants should remain true after future changes:

1. Normal nodes build lazy graphs; output nodes trigger actual computation through executor-managed sinks.
2. Every execution is isolated by `execution_id`.
3. Progress queues include `execution_id`.
4. Frontend and backend type systems stay in sync.
5. `execution_finished` remains the authoritative terminal event.
6. Algorithm engineers can write block-level logic without managing Dask execution directly.
7. Writers do not assume uniform chunk sizes at array boundaries.
8. GPU models are reused per worker where possible.
9. Rechunking is explicit, logged, and justified.
10. Global barriers are documented and operate on metadata, not full image arrays.


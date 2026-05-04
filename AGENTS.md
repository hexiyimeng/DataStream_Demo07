# AGENTS.md — BrainFlow Codex Instructions

This document is the authoritative project guidance for Codex when working in this repository.

BrainFlow is already a large and architecture-sensitive project. Codex must behave like a senior backend/distributed-systems engineer, not like a code generator that only edits the file mentioned in the prompt.

---

## 1. Project mission

BrainFlow is a distributed large-scale bioimage processing framework.

The long-term goal is to process very large datasets, potentially PB-scale, by combining:

- Dask lazy execution
- chunked array processing
- OME-Zarr / Zarr I/O
- GPU-aware block algorithms
- FastAPI + WebSocket execution orchestration
- a node/DAG programming model for algorithm engineers

The most important product goal is:

> Algorithm engineers should usually only need to write block-level algorithms over NumPy arrays. They should not need to manually manage Dask scheduling, OME-Zarr metadata, WebSocket progress, execution sessions, queue isolation, GPU cache cleanup, or writer-side delayed execution.

When making changes, preserve this abstraction.

---

## 2. How Codex should work in this repository

Before making non-trivial changes, Codex must:

1. Read this `AGENTS.md`.
2. Inspect the relevant source files, not only the file named by the user.
3. Identify the execution path affected by the change.
4. Produce a short plan before editing.
5. Prefer small, high-confidence patches.
6. Preserve large-data laziness.
7. Run or propose focused validation.
8. Summarize changed files, behavior changes, risks, and follow-up work.

For very small edits, a plan may be brief, but Codex must still consider architecture impact.

Do not make broad rewrites unless explicitly requested.

Do not introduce new production dependencies without explaining why and asking for confirmation.

Do not silently change frontend/backend protocol fields unless the user explicitly asks for a protocol migration.

---

## 3. Repository architecture

Typical backend layout:

```text
api/
  http_routes.py
  websocket.py

core/
  auth.py
  chunk_policy.py
  config.py
  logger.py
  registry.py
  state_manager.py
  type_system.py

services/
  dask_service.py
  executor.py
  plugin_loader.py

nodes/
  block_map.py or base.py
  cellpose_node.py
  debug_nodes.py
  instance_nodes.py
  ome_zarr_flow.py
  post_processing_nodes.py
  roi_node.py
  scale_shift_node.py
  type_cast_node.py

utils/
  chunk_helpers.py
  memory_monitor.py
  progress_helper.py
  union_find.py

main.py
```

Important responsibilities:

- `core.registry`
  - node registration
  - node metadata export to frontend
  - progress type definition
- `core.type_system`
  - typed ports such as `DASK_ARRAY[uint16]`
  - connection compatibility
- `core.state_manager`
  - execution lifecycle
  - execution-scoped logs
  - execution-scoped node states
  - WebSocket subscriber isolation
- `core.chunk_policy`
  - chunk recommendations
  - rechunk reason tracking
  - writer-side rechunk minimization
- `services.dask_service`
  - LocalCluster lifecycle
  - worker count / memory limit / spill directory
  - GPU worker binding
  - worker-side Cellpose import
- `services.executor`
  - DAG validation
  - lazy graph building
  - unified sink submission
  - progress monitoring
  - cancellation and cleanup
- `nodes.*`
  - user-visible computational graph nodes
- `utils.progress_helper`
  - execution-scoped Dask Queue progress
- `utils.memory_monitor`
  - process, Dask worker, and GPU memory snapshots
- `main.py`
  - FastAPI app startup/shutdown
  - plugin loading before Dask startup
  - route registration
  - static frontend hosting

---

## 4. Core architectural invariants

### 4.1 Preserve lazy execution

This is the most important invariant.

Do not eagerly compute large arrays during normal node execution.

Avoid in normal non-output nodes:

```python
arr.compute()
np.asarray(dask_arr)
list(dask_arr)
dask_arr.persist()
client.compute(...)  # outside executor submit phase
```

Allowed only when explicitly justified:

- tiny metadata computation
- tests using small synthetic arrays
- final output sink execution inside the executor
- controlled diagnostics requested by the user

Preferred patterns:

```python
dask_arr.map_blocks(...)
dask.delayed(...)
dask.array.map_overlap(...)  # only when boundary behavior is intentional
dask.array.from_zarr(...)
to_delayed()
```

If a change requires a global barrier, document it clearly:

- what data is collected
- why it must be global
- expected memory growth
- scaling limit
- possible future distributed alternative

### 4.2 Preserve `execution_id` isolation

Multiple workflows may run or reconnect. Never use global queues or global state that can mix executions.

All progress queues must include `execution_id`.

Use:

```python
get_progress_queue_name(node_id, execution_id)
get_stage_progress_queue_name(node_id, execution_id)
report_progress(node_id, execution_id=execution_id)
report_stage_progress(node_id, ..., execution_id=execution_id)
```

Do not use process-global counters for per-execution progress unless they are protected and execution-scoped.

### 4.3 Preserve two-phase execution

The executor is conceptually two-phase:

1. **GraphBuilding**
   - node `execute()` methods construct lazy Dask graphs
   - no heavy compute
   - progress may say `Ready`, `Queued`, or `GraphBuilding`
2. **Submit/Running**
   - output sink delayed tasks are submitted to Dask
   - progress monitors are started
   - actual compute happens

Do not move real computation into GraphBuilding.

Do not show user-facing `"Computing"` before work has actually been submitted to Dask.

### 4.4 Output nodes are sinks

Output nodes should usually return a delayed sink task.

Typical output node return:

```python
return (
    final_delayed,
    {"sink_progress": {"kind": "queue", "total_chunks": len(write_tasks)}},
)
```

For stage-based output nodes:

```python
return (
    final_delayed,
    {"sink_progress": {"kind": "stage_queue"}},
)
```

When adding a new output node, ensure the executor can find and submit it.

### 4.5 Node protocol stability

Frontend depends on node metadata and execution events.

Avoid breaking:

- `INPUT_TYPES`
- `RETURN_TYPES`
- `RETURN_NAMES`
- `FUNCTION`
- `OUTPUT_NODE`
- `PROGRESS_TYPE`
- WebSocket message fields:
  - `type`
  - `executionId`
  - `taskId`
  - `progress`
  - `progressType`
  - `runState`
  - `progressRole`
  - `message`
  - `waitingFor`
  - chunk counters such as `totalChunks`, `processedChunks`, `skippedChunks`

`execution_finished` is the authoritative final event.

Legacy `done` may exist for compatibility, but new logic should use `execution_finished`.

---

## 5. Node development rules

### 5.1 Register every user-visible node

Use:

```python
@register_node("NodeTypeName")
class MyNode:
    ...
```

Every user-visible node should define:

```python
CATEGORY = "BrainFlow/..."
DISPLAY_NAME = "Human Friendly Name"
PROGRESS_TYPE = ProgressType.STATE_ONLY  # or CHUNK_COUNT / STAGE_BASED
RETURN_TYPES = (...)
RETURN_NAMES = (...)
FUNCTION = "execute"
```

For output nodes:

```python
OUTPUT_NODE = True
```

### 5.2 Prefer `BaseBlockMapNode` for block algorithms

For algorithms that operate independently on chunks, prefer the block map abstraction.

Example pattern:

```python
def my_block(block: np.ndarray, param_a: float, ctx) -> np.ndarray:
    # block-local algorithm only
    return result

@register_node("DaskMyAlgorithm")
class DaskMyAlgorithm(BaseBlockMapNode):
    CATEGORY = "BrainFlow/Algorithm"
    DISPLAY_NAME = "My Algorithm"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "param_a": ("FLOAT", {"default": 1.0}),
            }
        }

    RETURN_TYPES = ("DASK_ARRAY[float32]",)
    RETURN_NAMES = ("dask_arr",)
    FUNCTION = "execute"
    PROCESS_BLOCK = my_block

    def infer_output_dtype(self, input_dtype, params):
        return np.dtype(np.float32)
```

Block functions should:

- accept one NumPy block
- return one NumPy block
- avoid reading unrelated chunks
- avoid modifying global state
- avoid opening large files repeatedly
- avoid creating large objects per block unnecessarily

Unless explicitly allowed:

```python
ALLOW_SHAPE_CHANGE = True
```

the output block must have the same shape and ndim as the input block.

### 5.3 Dtype discipline

Use typed ports when possible:

```python
"DASK_ARRAY[any]"
"DASK_ARRAY[uint8]"
"DASK_ARRAY[uint16]"
"DASK_ARRAY[float32]"
"DASK_ARRAY[bool]"
```

Do not weaken a typed node to `DASK_ARRAY` or `DASK_ARRAY[any]` merely to bypass a type mismatch.

If conversion is needed, prefer an explicit `DaskTypeCast` node or a clearly typed output.

`BaseBlockMapNode` should not silently cast outputs. If a function declares `DASK_ARRAY[uint16]`, the block function should actually return `uint16`.

### 5.4 Progress type rules

Use:

- `ProgressType.STATE_ONLY`
  - reader
  - metadata-only nodes
  - ROI slicing
  - cheap lazy transformations
- `ProgressType.CHUNK_COUNT`
  - block map algorithms
  - segmentation
  - chunked writers
- `ProgressType.STAGE_BASED`
  - multi-stage delayed pipelines
  - statistics/reconciliation/final CSV generation

For `CHUNK_COUNT` nodes, report:

- completed inference chunks
- skipped chunks
- failed chunks
- processed chunks
- total chunks

For `STAGE_BASED` nodes, use clear stage messages and percentages.

---

## 6. Dask large-data rules

### 6.1 Always think in chunks

This project targets out-of-core data.

Before changing Dask code, estimate:

- array shape
- dtype size
- chunk shape
- single chunk memory
- number of chunks
- approximate task graph size
- graph fanout/fanin
- whether the change duplicates upstream computation
- whether the change causes a global barrier

### 6.2 Avoid task explosion

A PB-scale system cannot create one task per tiny pixel/voxel region.

Be careful with:

- very small chunks
- nested `delayed` loops
- `map_overlap` on many chunks
- per-chunk multiple downstream consumers
- writer plans that duplicate upstream reads

When adding new chunked operations, log or expose enough metadata to diagnose:

- `shape`
- `chunks`
- `chunksize`
- `numblocks`
- `npartitions`

### 6.3 Avoid duplicate upstream compute

If two sinks consume the same expensive upstream graph, consider whether computation is duplicated.

Do not call separate `compute()` calls for multiple sinks when they should share a unified graph.

The executor’s unified sink submission exists to reduce duplication.

### 6.4 Rechunking must be intentional

Do not add casual `.rechunk(...)`.

Every rechunk should have:

- a clear reason
- a logged location
- awareness of memory impact
- ideally use `safe_rechunk(...)` or `ChunkPolicy`

Allowed reasons include:

- reader initial chunking
- explicit user manual chunking
- single-chunk writer safety
- necessary algorithmic neighborhood constraints
- controlled optimization

Writer-side rechunking should be minimal.

### 6.5 Edge chunks are real

Never assume every chunk has `arr.chunksize`.

The last chunk along each axis may be smaller.

When writing block regions or computing chunk origins, use actual chunk metadata:

```python
chunk_len = arr.chunks[axis][block_idx[axis]]
```

or the concrete `block.shape` inside a worker.

For region writes, do not blindly use:

```python
origin[axis] + arr.chunksize[axis]
```

for every chunk. This can write past edge regions or mismatch block shapes.

---

## 7. GPU and Cellpose rules

### 7.1 Model caching

Cellpose models must not be reloaded for every chunk.

Use the existing worker-level model cache and reference-counting design.

Do not bypass:

- `CellposeModelCache.acquire_model`
- `release_model`
- `clear_if_safe`
- `force_clear`

unless the task is specifically to redesign model lifecycle.

### 7.2 CUDA cache cleanup

Do not add high-frequency:

```python
torch.cuda.empty_cache()
torch.cuda.synchronize()
```

inside every chunk by default.

These calls can hurt throughput and introduce synchronization stalls.

Prefer:

- low-frequency configurable cleanup
- lifecycle cleanup
- cluster stop cleanup
- safe cleanup when no active references exist

### 7.3 GPU worker assignment

When changing GPU code, consider:

- single GPU vs multi-GPU
- processes vs threads
- `worker.assigned_gpu`
- Dask worker memory limits
- GPU VRAM vs CPU RAM
- model cache per worker process
- Windows behavior

Do not assume Linux-only behavior unless explicitly guarded.

### 7.4 Cellpose version compatibility

Be careful with Cellpose model formats.

If the code distinguishes CP3/CP4 model formats, preserve clear errors rather than silent fallback.

A user should know why a model cannot load.

---

## 8. OME-Zarr / Zarr I/O rules

### 8.1 Reader rules

Readers should:

- validate path safely
- avoid reading full arrays eagerly
- parse OME-NGFF metadata when available
- preserve axes and voxel size metadata
- create Dask arrays with appropriate chunks
- log shape, dtype, original chunks, target chunks, and partition count

Do not expose sensitive filesystem details in user-facing errors unless appropriate for a local scientific tool.

### 8.2 Writer rules

Writers should:

- build a lazy delayed write plan
- initialize the store once
- write per-block regions
- finalize metadata after all block writes complete
- return a delayed task for the executor to submit
- handle natural edge chunks correctly
- avoid recomputing expensive upstream graphs

When writing OME-NGFF metadata, preserve:

- axes
- voxel size / scale
- dataset path
- dtype
- shape
- chunks

### 8.3 Compression

When changing compression, consider:

- CPU cost
- random read performance
- chunk shape
- image dtype
- compatibility with Zarr version
- whether the compressor is serializable in Dask workers

---

## 9. Executor rules

The executor is a critical file. Treat changes there as high risk.

Before editing `services/executor.py`, inspect:

- graph validation
- type validation
- scheduling
- progress callback
- delayed sink collection
- queue monitor startup
- cancellation path
- cleanup path
- memory monitor integration
- frontend protocol expectations

### 9.1 DAG validation

Preserve:

- cycle detection
- source output index validation
- typed connection validation
- dynamic return type resolution

Error messages should include:

- source node id/type
- target node id/type
- source output name/index
- expected type
- actual type
- suggested fix

### 9.2 Scheduling

Do not compute upstream nodes independently if they should remain part of a unified Dask graph.

Do not mutate shared `results` in ways that break concurrent output-node graph building.

When changing scheduling, think about:

- duplicate dependencies
- multiple output nodes
- cancellation
- task reuse
- error propagation

### 9.3 Progress monitoring

Progress monitors should start only after Dask submission.

Queue monitors must drain or be cancelled cleanly.

Avoid monitor deadlocks.

If a queue monitor times out, degrade gracefully to state-only progress instead of killing successful work.

### 9.4 Cancellation

Cancellation should:

- transition state legally
- notify clients
- cancel asyncio tasks
- cancel Dask futures when needed
- stop progress monitors
- release references
- not incorrectly report success

Cancellation during shutdown should usually end as `cancelled`, not `failed`, unless it is a true failure.

### 9.5 Cleanup

Cleanup should:

- stop monitors
- cancel internal tasks
- cancel Dask futures on abnormal exit
- release local references
- clean old executions
- record memory snapshots
- avoid clearing active model references unsafely

---

## 10. State manager and WebSocket rules

### 10.1 Execution lifecycle

Valid states:

- `running`
- `cancelling`
- `cancelled`
- `failed`
- `succeeded`

Do not introduce ad-hoc states without updating frontend assumptions.

Use legal transitions.

### 10.2 Subscriber isolation

WebSocket clients subscribe to one execution at a time.

History sync should only send the requested execution’s logs and node states.

Do not broadcast one execution’s logs to all clients.

### 10.3 WebSocket protocol

Important message types include:

- `execution_started`
- `progress`
- `log`
- `error`
- `execution_snapshot`
- `subscribed`
- `execution_control_ack`
- `execution_finished`
- `done` for legacy compatibility

When changing protocol fields, preserve backwards compatibility unless a migration is requested.

### 10.4 Authentication

Do not log raw API keys or WebSocket tokens.

Use token masking.

Preserve development mode behavior where authentication can be skipped when no API keys are configured.

For production-related changes, prefer explicit secure defaults.

---

## 11. Plugin loader rules

Plugin loading is part of startup safety.

Critical plugins should fail fast. Non-critical plugins may warn and continue.

When adding new required functionality, decide whether the plugin should be critical.

Do not hide critical import errors.

Plugin loading should provide enough logs to diagnose:

- module name
- error type
- whether it is critical
- startup outcome

---

## 12. Memory monitoring rules

Memory diagnostics are important for large-data execution.

When modifying memory-related behavior, consider:

- Python process RSS
- Dask worker RSS
- GPU allocated memory
- GPU reserved memory
- Dask spill directory
- worker memory target/spill/pause/terminate thresholds

Do not treat retained RSS after execution as automatically a leak. Long-running workers, allocators, and CUDA caching can retain memory.

Prefer logging useful deltas and clear warnings.

---

## 13. Security and filesystem rules

This is a local/scientific processing backend, but still handle inputs carefully.

Preserve or improve:

- path normalization
- null-byte checks
- maximum path length checks
- read/write permission checks
- API key masking
- CORS configurability
- safe static file serving
- no raw secrets in logs

Do not add shell command execution based on user input.

When paths are user-provided, avoid unsafe string concatenation for security-sensitive paths.

---

## 14. Testing and validation expectations

When changing code, Codex should look for existing test conventions first.

If no test suite exists, propose or add focused tests where practical.

Useful validation commands:

```bash
python -m compileall api core services nodes utils main.py
python -m pytest
```

Run only commands appropriate for the repository and environment.

Do not require real PB-scale data in tests.

Use small synthetic data:

- small NumPy arrays
- small Dask arrays
- temporary Zarr stores
- fake graph dictionaries
- mocked Dask clients/queues where needed

### 14.1 High-value tests

Add tests around:

- DAG cycle detection
- graph type validation
- dynamic return type resolution
- `DaskTypeCast`
- `BaseBlockMapNode` dtype and shape validation
- execution-scoped queue names
- chunk origin calculation
- chunk adjacency
- writer edge-chunk region slicing
- state transition legality
- cancellation behavior
- output-node delayed plan construction
- OME-Zarr metadata preservation
- plugin load failure behavior

### 14.2 Performance smoke checks

For performance-sensitive changes, include a small check for:

- task count
- chunk count
- no eager compute during graph building
- no full-array NumPy conversion
- no per-block model reload

---

## 15. Documentation expectations

When adding or changing behavior, update relevant docs or comments.

Good comments explain:

- why the design exists
- what invariant is being protected
- what scaling limit exists
- what edge case is handled

Bad comments merely repeat code.

Architecture-sensitive changes should include comments near the code, not only in chat.

---

## 16. Code style

- Prefer explicit, readable Python.
- Keep functions focused.
- Use project logger names consistently.
- Keep protocol field names stable.
- Prefer dataclasses or typed helpers for structured internal state when complexity grows.
- Avoid clever one-liners in scheduler, progress, memory, and writer code.
- Use clear error messages.
- Prefer backwards-compatible changes.

Chinese comments are acceptable where they clarify user-facing or domain-specific behavior.

Protocol names, class names, and public API fields should remain in English and stable.

---

## 17. Common anti-patterns to avoid

Do not:

- call `.compute()` inside a normal processing node
- convert full Dask arrays to NumPy
- add global progress queues without execution id
- log raw tokens
- reload Cellpose model per chunk
- call `torch.cuda.empty_cache()` after every chunk by default
- blindly rechunk without reason
- assume every chunk has `arr.chunksize`
- change WebSocket final event semantics casually
- catch broad exceptions and silently return wrong data
- mark a workflow succeeded after partial sink failure
- add new dependencies without justification
- rewrite executor architecture casually
- ignore Windows behavior when touching Dask/GPU startup
- design tests that need real huge datasets

---

## 18. Known sensitive areas

Treat these as extra-risky and inspect carefully before editing:

### `services/executor.py`

Risk:

- execution lifecycle bugs
- hidden eager computation
- broken progress
- cancellation races
- duplicate compute
- frontend protocol breakage

### `services/dask_service.py`

Risk:

- worker startup failure
- GPU binding issues
- memory limit problems
- Windows/Linux differences

### `nodes/ome_zarr_flow.py`

Risk:

- edge chunk write bugs
- metadata loss
- eager I/O
- broken OME-NGFF compatibility
- duplicated upstream computation

### `nodes/cellpose_node.py`

Risk:

- GPU memory leaks
- model reload overhead
- version/model format compatibility
- worker-level cache correctness

### `core/state_manager.py`

Risk:

- cross-execution state leakage
- invalid terminal states
- history sync bugs
- WebSocket subscriber leaks

### `core/type_system.py`

Risk:

- allowing invalid graph connections
- breaking frontend node compatibility
- hiding dtype bugs

---

## 19. Recommended workflow for Codex tasks

For feature work, use this workflow:

1. Restate the goal.
2. Identify touched subsystems.
3. Inspect files.
4. Make a minimal design.
5. Implement small patch.
6. Run focused validation.
7. Review own diff.
8. Summarize:
   - files changed
   - behavior changed
   - tests run
   - risks
   - follow-up suggestions

For bug fixes:

1. Reproduce or reason from code path.
2. Identify root cause.
3. Add the smallest fix.
4. Add regression test if practical.
5. Explain why the fix preserves large-data behavior.

For refactors:

1. State what behavior must remain identical.
2. Make incremental changes.
3. Keep compatibility shims where needed.
4. Avoid changing public protocol and internal architecture at the same time.

---

## 20. Prompt templates for users

When the user asks Codex for work, prefer prompts like:

```text
Read AGENTS.md first.

Goal:
<what should change>

Context:
<what problem I observed, logs, error, or desired behavior>

Constraints:
- preserve lazy Dask execution
- preserve execution_id isolation
- do not break WebSocket protocol
- avoid new dependencies unless justified

Done when:
- code is changed
- focused validation is run or explained
- risks are summarized
```

For executor work:

```text
Read AGENTS.md first. Inspect services/executor.py, core/state_manager.py, utils/progress_helper.py, and services/dask_service.py.

Goal:
Improve <specific executor behavior>.

Constraints:
- no eager compute during GraphBuilding
- execution_finished remains authoritative
- progress queues remain execution-scoped
- cancellation must not report success

Done when:
- behavior is implemented
- race/cancellation risks are discussed
- focused tests or validation steps are provided
```

For new block node work:

```text
Read AGENTS.md first.

Goal:
Add a new block-wise node named <NodeName>.

The algorithm should operate on one NumPy block at a time.

Constraints:
- subclass BaseBlockMapNode
- preserve lazy execution
- define typed RETURN_TYPES
- report CHUNK_COUNT progress
- include dtype/shape handling
- no full-array compute
```

For OME-Zarr work:

```text
Read AGENTS.md first. Inspect nodes/ome_zarr_flow.py and utils/chunk_helpers.py.

Goal:
Improve <reader/writer behavior>.

Constraints:
- preserve lazy write plan
- handle edge chunks correctly
- preserve OME-NGFF metadata
- avoid duplicate upstream computation
```

---

## 21. Definition of done

A change is done only when:

- it solves the requested problem
- it preserves lazy execution for large data
- it preserves execution-scoped state/progress
- it does not break existing node protocol
- it handles edge chunks and dtype expectations where relevant
- it includes focused validation or clearly explains why validation could not be run
- it summarizes risks honestly
- it avoids unnecessary architectural churn

For this project, “works on a tiny example” is not enough if the change obviously breaks large-data behavior.

---

## 22. Design north star

When in doubt, choose the design that makes this easier:

> An algorithm engineer writes a pure block function, declares input/output types, and gets distributed execution, progress reporting, OME-Zarr I/O, cancellation, and GPU-safe behavior from the framework.

Do not push framework complexity back onto algorithm engineers.

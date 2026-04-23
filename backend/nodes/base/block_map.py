"""
BaseBlockMapNode: Abstract base for BlockMap pattern nodes.

BlockMap pattern: input is a Dask Array, each block is processed independently,
output is still a regular Dask Array, node stays lazy.

Design principles:
- node author only writes process_block (single block algorithm)
- Base handles: map_blocks wrapping, runtime injection, skip/failure/progress, dtype/meta
- device_hint is resolved at execution time (on worker), not at graph-building time
- safe defaults: SKIP_EMPTY_BLOCKS=True, SKIP_ALL_ZERO_BLOCKS=False, FAILURE_POLICY="raise"
- output shape contract: result.ndim == block.ndim and result.shape == block.shape by default
"""

import numpy as np
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Dict, Any, Callable
import inspect

from core.registry import register_node, ProgressType
from utils.progress_helper import report_progress


class BaseBlockMapNode(ABC):
    """
    BlockMap node abstract base class.

    Applicable scope:
    - Input: single regular Dask Array
    - Each block processed independently (no cross-block dependencies)
    - Suitable for map_blocks
    - Output: regular Dask Array (same shape/chunks as input, by default)
    - Node stays lazy (execute() only builds graph, never calls compute)

    Node author only needs to implement:
    1. INPUT_TYPES (class method)
    2. process_block(block, block_info, params, runtime) -> np.ndarray (abstract method)
    3. RETURN_TYPES, FUNCTION="execute"

    Output shape contract (default, enforced by Base):
    - process_block MUST return a np.ndarray
    - result.ndim MUST equal block.ndim
    - result.shape MUST equal block.shape
    - This is the DEFAULT behavior (ALLOW_SHAPE_CHANGE=False).
      Subclasses that intentionally produce shape-changing output can set
      ALLOW_SHAPE_CHANGE=True and override validate_output_block() if needed.

    Optional overrides:
    - OUTPUT_DTYPE: explicit output dtype (default: input array's dtype)
    - SKIP_EMPTY_BLOCKS: skip size==0 blocks (default: True)
    - SKIP_ALL_ZERO_BLOCKS: skip all-zero blocks (default: False, opt-in)
    - FAILURE_POLICY: "raise" (default) or "zeros_like"
    - ALLOW_SHAPE_CHANGE: if True, skip shape validation (default: False)
    - should_skip_block(block, block_info, params, runtime) -> bool: custom skip logic
      (supports legacy single-arg signature for backward compatibility)
    - validate_output_block(result, block) -> None: override for custom validation

    NOT in scope (this base):
    - halo / map_overlap / cross-block dependencies
    - Array -> Table / irregular output
    - global convergence nodes (like DaskStats)
    - sink / writer nodes
    - node-internal rechunking
    """

    # ---------- Node protocol (subclass MUST define) ----------
    # INPUT_TYPES = {...}
    # RETURN_TYPES = (...)
    # FUNCTION = "execute"

    # ---------- Default behavior (usually safe to keep) ----------
    CATEGORY = "BrainFlow/BlockMap"
    DISPLAY_NAME = "BlockMap Node"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    # Skip strategy
    SKIP_EMPTY_BLOCKS = True        # Safe default: size==0 blocks have no data
    SKIP_ALL_ZERO_BLOCKS = False   # Unsafe as default: many algorithms should process zeros normally

    # Failure strategy
    # "raise": exceptions propagate up (safe default for generic nodes)
    # "zeros_like": exception -> return zeros_like(block), report "failed"
    FAILURE_POLICY = "raise"

    # Output shape contract
    # False = enforce result.ndim == block.ndim and result.shape == block.shape
    # True = skip shape validation (for nodes that intentionally change shape)
    ALLOW_SHAPE_CHANGE = False

    # Output dtype
    # None = use input array's dtype
    # Set to specific dtype to override
    OUTPUT_DTYPE = None

    # ---------- Abstract method (subclass MUST implement) ----------

    @abstractmethod
    def process_block(
        self,
        block: np.ndarray,
        block_info: dict,
        params: dict,
        runtime: dict,
    ) -> np.ndarray:
        """
        Single block processing algorithm. Implemented by subclass.

        Parameters
        ----------
        block : np.ndarray
            Current chunk's numpy data (not dask array).
            Note: empty blocks (size==0) may be skipped (if SKIP_EMPTY_BLOCKS=True)
            before this is called. But ALL-ZERO blocks are NOT auto-skipped
            unless SKIP_ALL_ZERO_BLOCKS=True.
        block_info : dict
            Block metadata injected by Dask at execution time. Standard fields:
            - "shape": chunk shape
            - "chunk-location": block index in original array
            - "dtype": data type
            Other Dask map_blocks standard fields may be present.
            Can be used for spatial-aware processing if needed.
        params : dict
            Node's configured parameters (from INPUT_TYPES).
            Does NOT contain framework internal fields like _node_id, _execution_id.
        runtime : dict
            Runtime context containing:
            - "node_id": node ID (for progress reporting)
            - "execution_id": execution ID
            - "device_hint": device string like "cuda:0" or "cpu"
            Note: device_hint is resolved at execution time (on worker),
            not at graph-building time (execute runs on client).

        Returns
        -------
        np.ndarray
            Processed block data. Must satisfy the output shape contract:
            - Must be a np.ndarray
            - result.ndim == block.ndim (unless ALLOW_SHAPE_CHANGE=True)
            - result.shape == block.shape (unless ALLOW_SHAPE_CHANGE=True)
            - The returned numpy array will be automatically assembled by map_blocks
              into the output Dask Array. Do NOT return a dask array.
        """
        ...

    # ---------- execute (system calls, node author does not override) ----------

    def execute(self, dask_arr, **kwargs) -> Tuple:
        """
        Build map_blocks lazy graph.

        This method is called by the executor during graph-building phase.
        It returns a lazy Dask Array (does NOT call compute).
        """
        node_id = kwargs.get("_node_id")
        execution_id = kwargs.get("_execution_id")

        # Build static runtime (determinable at graph-building time)
        static_runtime = {
            "node_id": node_id,
            "execution_id": execution_id,
        }

        # Extract node parameters (filter out framework internal fields)
        params = self._extract_params(kwargs)

        # Determine output dtype
        output_dtype = self._resolve_output_dtype(dask_arr.dtype, params)

        # Build meta for map_blocks
        meta = self._build_meta(output_dtype)

        # Create wrapped function (adds skip/failure/progress/validation logic)
        wrapped_fn = self._make_wrapped_function(
            static_runtime=static_runtime,
            params=params,
            output_dtype=output_dtype,
            node_id=node_id,
        )

        # 构建 map_blocks 的 token/name，使 Dask graph 上能区分不同节点
        # 使用 node_id + 类名作为标识，兼容 Dask 的 name= 参数
        node_cls_name = type(self).__name__
        map_blocks_name = f"{node_cls_name}_{node_id}" if node_id else node_cls_name

        # Call map_blocks
        # Note: we do NOT pass block_info as a kwarg.
        # Dask injects block_info via function signature at execution time.
        # The wrapped function accepts block_info as an optional positional arg.
        result = dask_arr.map_blocks(
            wrapped_fn,
            dtype=output_dtype,
            meta=meta,
            name=map_blocks_name,
        )

        # 轻量日志：打印该节点在 Dask graph 中的前几个 task key
        # 这在 dashboard 不可见时，提供代码级的可追踪证据
        try:
            graph_keys = list(result.__dask_graph__().keys())
            sample_keys = [str(k) for k in graph_keys[:2]]
            logger.debug(
                f"[BlockMap] {node_cls_name}[{node_id}] graph tasks: count={len(graph_keys)}, "
                f"sample={sample_keys}"
            )
        except Exception:
            pass

        return (result,)

    # ---------- Internal methods ----------

    def _extract_params(self, kwargs) -> Dict[str, Any]:
        """Extract node's configurable parameters, filtering framework internal fields."""
        framework_keys = {"_node_id", "_execution_id", "dask_arr"}
        return {k: v for k, v in kwargs.items() if k not in framework_keys}

    def _resolve_output_dtype(self, input_dtype, params) -> np.dtype:
        """Resolve output dtype. Priority: OUTPUT_DTYPE > infer_output_dtype > input_dtype."""
        if self.OUTPUT_DTYPE is not None:
            return self.OUTPUT_DTYPE
        if hasattr(self, "infer_output_dtype"):
            return self.infer_output_dtype(input_dtype, params)
        return input_dtype

    def _build_meta(self, dtype) -> np.ndarray:
        """Build meta array for map_blocks (empty array with correct dtype)."""
        return np.array((), dtype=dtype)

    # ---------- Output shape contract ----------

    def _validate_output_block(self, result: np.ndarray, block: np.ndarray) -> None:
        """
        Validate that process_block output satisfies the shape contract.

        Default contract (when ALLOW_SHAPE_CHANGE=False):
        - result must be a np.ndarray
        - result.ndim == block.ndim
        - result.shape == block.shape

        Raises ValueError with a clear message if the contract is violated.
        Subclasses can override this to customize validation, but should
        generally not relax the default contract without good reason.
        """
        if not isinstance(result, np.ndarray):
            raise ValueError(
                f"BlockMap output contract violation: process_block must return np.ndarray, "
                f"got {type(result).__name__}. "
                f"Hint: if returning a scalar, wrap it in np.array([...]) or np.zeros(...)."
            )

        if self.ALLOW_SHAPE_CHANGE:
            return  # validation skipped

        if result.ndim != block.ndim:
            raise ValueError(
                f"BlockMap output shape contract violation: "
                f"result.ndim ({result.ndim}) must equal block.ndim ({block.ndim}). "
                f"Set ALLOW_SHAPE_CHANGE=True if intentional shape-changing is required."
            )

        if result.shape != block.shape:
            raise ValueError(
                f"BlockMap output shape contract violation: "
                f"result.shape ({result.shape}) must equal block.shape ({block.shape}). "
                f"Set ALLOW_SHAPE_CHANGE=True if intentional shape-changing is required."
            )

    # ---------- Skip logic ----------

    def _should_skip(self, block: np.ndarray, block_info: dict, params: dict, runtime: dict) -> bool:
        """
        Determine if block should be skipped.

        Skip decision order:
        1. SKIP_EMPTY_BLOCKS: if block.size == 0
        2. SKIP_ALL_ZERO_BLOCKS: if np.all(block == 0)
        3. should_skip_block(...): custom node-defined skip logic (if provided)

        Parameters
        ----------
        block : np.ndarray
        block_info : dict (injected by Dask at execution time)
        params : dict (node parameters)
        runtime : dict (node_id, execution_id, device_hint)

        Returns
        -------
        bool: True if block should be skipped
        """
        # 1. Empty block skip (always safe)
        if self.SKIP_EMPTY_BLOCKS and block.size == 0:
            return True

        # 2. All-zero block skip (opt-in)
        if self.SKIP_ALL_ZERO_BLOCKS and np.all(block == 0):
            return True

        # 3. Custom skip hook — supports both new full-signature and legacy single-arg
        custom_skip = getattr(self, "should_skip_block", None)
        if custom_skip is not None:
            sig = getattr(inspect, "signature", None)
            if sig is not None:
                try:
                    params_sig = sig(custom_skip).parameters
                    # New signature: 4 args
                    if len(params_sig) >= 4:
                        return custom_skip(block, block_info, params, runtime)
                    # Legacy signature: 1 arg
                    elif len(params_sig) == 1:
                        return custom_skip(block)
                    else:
                        # Ambiguous signature — try new first, fall back to legacy
                        try:
                            return custom_skip(block, block_info, params, runtime)
                        except TypeError:
                            return custom_skip(block)
                except (ValueError, TypeError):
                    # inspect.signature can fail on builtins — try new signature first
                    try:
                        return custom_skip(block, block_info, params, runtime)
                    except TypeError:
                        return custom_skip(block)
            else:
                # inspect.signature not available — assume new signature
                try:
                    return custom_skip(block, block_info, params, runtime)
                except TypeError:
                    return custom_skip(block)

        return False

    def _resolve_device_hint(self) -> str:
        """
        Resolve device hint at execution time (on worker).

        Called inside wrapped function, not in execute() (which runs on client).
        """
        try:
            from distributed import get_worker
            worker = get_worker()
            return getattr(worker, "assigned_gpu", "cpu")
        except Exception:
            return "cpu"

    def _make_wrapped_function(self, static_runtime: dict, params: dict, output_dtype: np.dtype, node_id: str = None):
        """
        Wrap process_block with:
        1. device_hint resolution (execution time)
        2. runtime merging
        3. skip logic (with full context: block, block_info, params, runtime)
        4. output shape validation
        5. failure handling
        6. progress reporting
        """
        skip_decider = self._should_skip  # bound method with full context
        failure_policy = self.FAILURE_POLICY
        validate_fn = self._validate_output_block
        validate_output_block_method = getattr(self, "validate_output_block", None)
        allow_shape_change = self.ALLOW_SHAPE_CHANGE

        def wrapped(block, block_info=None):
            # block_info: injected by Dask at execution time via function signature
            block_info = block_info or {}

            # ----- Resolve device_hint at execution time -----
            # Priority: worker.assigned_gpu > torch.cuda.is_available() > "cpu"
            device_hint = "cpu"
            try:
                from distributed import get_worker
                worker = get_worker()
                assigned = getattr(worker, "assigned_gpu", None)
                if assigned and assigned != "cpu":
                    device_hint = assigned
                elif assigned == "cpu":
                    device_hint = "cpu"
            except Exception:
                pass

            if device_hint == "cpu":
                try:
                    import torch
                    if torch.cuda.is_available():
                        device_hint = "cuda:0"
                except Exception:
                    pass

            # ----- Build full runtime -----
            runtime = {
                **static_runtime,
                "device_hint": device_hint,
            }

            # ----- Skip decision (with full context) -----
            if skip_decider(block, block_info, params, runtime):
                node_id = static_runtime.get("node_id")
                execution_id = static_runtime.get("execution_id")
                if node_id:
                    report_progress(node_id, execution_id=execution_id, chunk_type="skipped")
                return np.zeros_like(block, dtype=output_dtype)

            # ----- Call process_block -----
            if failure_policy == "zeros_like":
                try:
                    result = self.process_block(block, block_info, params, runtime)

                    # ----- Output shape contract validation -----
                    # Use node's own validate_output_block if overridden, otherwise Base default
                    if validate_output_block_method is not None and validate_output_block_method is not BaseBlockMapNode._validate_output_block:
                        validate_output_block_method(result, block)
                    else:
                        validate_fn(result, block)

                    node_id = static_runtime.get("node_id")
                    execution_id = static_runtime.get("execution_id")
                    if node_id:
                        report_progress(node_id, execution_id=execution_id, chunk_type="completed")
                    return result

                except Exception as e:
                    import logging
                    logger = logging.getLogger("BrainFlow.BlockMap")
                    logger.error(f"BlockMap error in {runtime.get('node_id', '?')}: {e}")
                    node_id = static_runtime.get("node_id")
                    execution_id = static_runtime.get("execution_id")
                    if node_id:
                        report_progress(node_id, execution_id=execution_id, chunk_type="failed")
                    return np.zeros_like(block, dtype=output_dtype)
            else:
                # "raise" policy: exceptions propagate
                result = self.process_block(block, block_info, params, runtime)

                # ----- Output shape contract validation -----
                if validate_output_block_method is not None and validate_output_block_method is not BaseBlockMapNode._validate_output_block:
                    validate_output_block_method(result, block)
                else:
                    validate_fn(result, block)

                node_id = static_runtime.get("node_id")
                execution_id = static_runtime.get("execution_id")
                if node_id:
                    report_progress(node_id, execution_id=execution_id, chunk_type="completed")
                return result

        wrapped._is_blockmap_wrapped = True
        # 设置函数名，增强 Dask graph 可读性
        wrapped.__name__ = f"BlockMap_{node_id}_{type(self).__name__}" if node_id else f"BlockMap_{type(self).__name__}"
        return wrapped

from __future__ import annotations

from dataclasses import dataclass
import inspect
import logging
from typing import Any, Dict, Optional, Tuple

import numpy as np

from core.registry import ProgressType
from core.type_system import dtype_name_to_numpy, is_dask_array_type, parse_port_type
from core.model_ref import ModelRef
from core.resources.worker_resource_manager import BlockResourcesMixin
from utils.progress_helper import report_progress


logger = logging.getLogger("BrainFlow.BlockMap")


@dataclass(frozen=True)
class BlockContext:
    node_id: Optional[str]
    execution_id: Optional[str]
    device: str
    block_info: dict
    block_location: Any
    block_shape: tuple
    input_dtype: np.dtype
    resources: Any = None

    @property
    def device_hint(self) -> str:
        return self.device


class BlockResources(BlockResourcesMixin):
    def __init__(self, owner: "BaseBlockMapNode", ctx: BlockContext):
        super().__init__()
        self._owner = owner
        self._ctx = ctx

    def cellpose_model(self, model_type: str):
        acquire = getattr(self._owner, "_acquire_cellpose_model", None)
        if acquire is None:
            raise RuntimeError("This node does not provide a Cellpose model resource.")
        return acquire(model_type, self._ctx.device)

    def resolve_model(self, model_ref: ModelRef):
        handle = self._resolve_model_handle(model_ref, self._ctx.device)
        return handle.value


class BaseBlockMapNode:
    CATEGORY = "BrainFlow/BlockMap"
    DISPLAY_NAME = "BlockMap Node"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    PROCESS_BLOCK = None

    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = False
    FAILURE_POLICY = "raise"
    ALLOW_SHAPE_CHANGE = False

    # Legacy compatibility. New nodes should declare RETURN_TYPES instead.
    OUTPUT_DTYPE = None

    FUNCTION = "execute"

    def process_block(
        self,
        block: np.ndarray,
        block_info: dict,
        params: dict,
        runtime: dict,
    ) -> np.ndarray:
        raise NotImplementedError(
            f"{type(self).__name__} must define PROCESS_BLOCK or override process_block()."
        )

    def execute(self, dask_arr, **kwargs) -> Tuple:
        node_id = kwargs.get("_node_id")
        execution_id = kwargs.get("_execution_id")
        params = self._extract_params(kwargs)

        return_type = self._declared_return_type()
        output_dtype = self._resolve_output_dtype(dask_arr.dtype, params, return_type)
        meta = np.array((), dtype=output_dtype)

        static_runtime = {
            "node_id": node_id,
            "execution_id": execution_id,
        }

        wrapped_fn = self._make_wrapped_function(
            static_runtime=static_runtime,
            params=params,
            output_dtype=output_dtype,
            return_type=return_type,
        )

        node_cls_name = type(self).__name__
        map_blocks_name = f"{node_cls_name}_{node_id}" if node_id else node_cls_name
        result = dask_arr.map_blocks(
            wrapped_fn,
            dtype=output_dtype,
            meta=meta,
            name=map_blocks_name,
        )

        try:
            graph_keys = list(result.__dask_graph__().keys())
            sample_keys = [str(k) for k in graph_keys[:2]]
            logger.debug(
                "[BlockMap] %s[%s] graph tasks: count=%s sample=%s",
                node_cls_name,
                node_id,
                len(graph_keys),
                sample_keys,
            )
        except Exception:
            pass

        return (result,)

    def _declared_return_type(self) -> str:
        return_types = getattr(type(self), "RETURN_TYPES", ()) or ()
        if not return_types:
            return "DASK_ARRAY"
        return str(return_types[0])

    def _extract_params(self, raw_inputs: Dict[str, Any]) -> Dict[str, Any]:
        framework_keys = {"_node_id", "_execution_id"}
        params: Dict[str, Any] = {}

        input_defs = self._input_definitions()
        for section in ("required", "optional"):
            for name, config in input_defs.get(section, {}).items():
                declared = config[0] if isinstance(config, (tuple, list)) and len(config) > 0 else config
                meta = config[1] if isinstance(config, (tuple, list)) and len(config) > 1 and isinstance(config[1], dict) else {}

                if name in framework_keys:
                    continue
                if isinstance(declared, str) and is_dask_array_type(declared):
                    continue

                value = raw_inputs.get(name)
                if value is None or (isinstance(value, str) and value == ""):
                    if "default" in meta:
                        value = meta["default"]
                    elif isinstance(declared, list) and declared:
                        value = declared[0]
                    elif section == "optional":
                        value = None

                value = self._coerce_param_value(name, declared, value)
                params[name] = value

        for name, value in raw_inputs.items():
            if name in framework_keys or name in params:
                continue
            if name.startswith("_"):
                continue
            params[name] = value

        return params

    def _input_definitions(self) -> dict:
        input_types = getattr(type(self), "INPUT_TYPES", None)
        if input_types is None:
            return {"required": {}, "optional": {}}
        try:
            return input_types()
        except Exception as exc:
            logger.warning("Failed to read INPUT_TYPES from %s: %s", type(self).__name__, exc)
            return {"required": {}, "optional": {}}

    def _coerce_param_value(self, name: str, declared: Any, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(declared, str):
            return value
        try:
            if declared == "INT":
                return int(float(value))
            if declared == "FLOAT":
                return float(value)
            if declared == "BOOLEAN":
                if isinstance(value, str):
                    return value.lower() == "true"
                return bool(value)
        except Exception as exc:
            logger.warning("Failed to convert BlockMap param %s=%r: %s", name, value, exc)
        return value

    def _resolve_output_dtype(self, input_dtype, params: Dict[str, Any], return_type: str) -> np.dtype:
        parsed = parse_port_type(return_type)
        resolved = dtype_name_to_numpy(parsed.dtype, input_dtype=input_dtype)
        if resolved is not None:
            return np.dtype(resolved)

        if self.OUTPUT_DTYPE is not None:
            return np.dtype(self.OUTPUT_DTYPE)

        infer = getattr(self, "infer_output_dtype", None)
        if infer is not None:
            inferred = infer(input_dtype, params)
            if inferred is not None:
                return np.dtype(inferred)

        return np.dtype(input_dtype)

    def _make_wrapped_function(
        self,
        static_runtime: dict,
        params: dict,
        output_dtype: np.dtype,
        return_type: str,
    ):
        failure_policy = self.FAILURE_POLICY

        def wrapped(block, block_info=None):
            block_info = block_info or {}
            device = self._resolve_device_hint()
            runtime = {
                **static_runtime,
                "device_hint": device,
            }
            ctx = self._build_context(
                block=block,
                block_info=block_info,
                runtime=runtime,
            )

            try:
                if self._should_skip(block, block_info, params, runtime):
                    self._report_chunk(static_runtime, "skipped")
                    return np.zeros_like(block, dtype=output_dtype)

                result = self._call_process_block(block, params, ctx)
                self._validate_output_block(result, block)
                self._validate_output_dtype(
                    result=result,
                    block=block,
                    expected_output_dtype=output_dtype,
                    return_type=return_type,
                    node_id=static_runtime.get("node_id"),
                )
                self._report_chunk(static_runtime, "completed")
                return result
            except Exception as exc:
                if failure_policy != "zeros_like":
                    raise
                logger.error(
                    "BlockMap error in %s[%s]: %s",
                    type(self).__name__,
                    static_runtime.get("node_id", "?"),
                    exc,
                )
                self._report_chunk(static_runtime, "failed")
                return np.zeros_like(block, dtype=output_dtype)
            finally:
                resources = getattr(ctx, "resources", None)
                if resources is not None:
                    resources.release_all()

        wrapped._is_blockmap_wrapped = True
        node_id = static_runtime.get("node_id")
        wrapped.__name__ = (
            f"BlockMap_{node_id}_{type(self).__name__}"
            if node_id else f"BlockMap_{type(self).__name__}"
        )
        return wrapped

    def _build_context(self, block: np.ndarray, block_info: dict, runtime: dict) -> BlockContext:
        location = self._extract_block_location(block_info)
        ctx = BlockContext(
            node_id=runtime.get("node_id"),
            execution_id=runtime.get("execution_id"),
            device=runtime.get("device_hint", "cpu"),
            block_info=block_info,
            block_location=location,
            block_shape=tuple(block.shape),
            input_dtype=np.dtype(block.dtype),
            resources=None,
        )
        resources = BlockResources(self, ctx)
        return BlockContext(
            node_id=ctx.node_id,
            execution_id=ctx.execution_id,
            device=ctx.device,
            block_info=ctx.block_info,
            block_location=ctx.block_location,
            block_shape=ctx.block_shape,
            input_dtype=ctx.input_dtype,
            resources=resources,
        )

    def _extract_block_location(self, block_info: Any):
        try:
            if isinstance(block_info, (list, tuple)) and block_info:
                first = block_info[0]
                if isinstance(first, dict):
                    return first.get("chunk-location")
            if isinstance(block_info, dict):
                if "chunk-location" in block_info:
                    return block_info.get("chunk-location")
                if None in block_info and isinstance(block_info[None], dict):
                    return block_info[None].get("chunk-location")
                if 0 in block_info and isinstance(block_info[0], dict):
                    return block_info[0].get("chunk-location")
        except Exception:
            pass
        return None

    def _call_process_block(self, block: np.ndarray, params: dict, ctx: BlockContext) -> np.ndarray:
        fn = self._get_process_block_callable()
        sig = inspect.signature(fn)
        parameters = list(sig.parameters.values())
        if not parameters:
            raise TypeError(f"{type(self).__name__} PROCESS_BLOCK must accept block as its first parameter.")

        if self._is_legacy_process_signature(parameters):
            runtime = {
                "node_id": ctx.node_id,
                "execution_id": ctx.execution_id,
                "device_hint": ctx.device,
            }
            return fn(block, ctx.block_info, params, runtime)

        explicit_kwargs: Dict[str, Any] = {}
        accepts_kwargs = False

        for param in parameters[1:]:
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_kwargs = True
                continue
            if param.name == "ctx":
                explicit_kwargs["ctx"] = ctx
                continue
            if param.name in params:
                explicit_kwargs[param.name] = params[param.name]
                continue
            if param.default is inspect.Parameter.empty:
                raise TypeError(
                    f"{type(self).__name__} PROCESS_BLOCK requires parameter "
                    f"'{param.name}', but it was not provided by INPUT_TYPES."
                )

        if accepts_kwargs:
            for key, value in params.items():
                explicit_kwargs.setdefault(key, value)

        return fn(block, **explicit_kwargs)

    def _get_process_block_callable(self):
        raw = inspect.getattr_static(type(self), "PROCESS_BLOCK", None)
        if isinstance(raw, staticmethod):
            raw = raw.__func__
        if raw is not None:
            return raw
        return self.process_block

    def _is_legacy_process_signature(self, parameters) -> bool:
        names = [p.name for p in parameters[:4]]
        return names == ["block", "block_info", "params", "runtime"]

    def _validate_output_block(self, result: np.ndarray, block: np.ndarray) -> None:
        validator = getattr(self, "validate_output_block", None)
        if validator is not None:
            validator(result, block)
            return

        if not isinstance(result, np.ndarray):
            raise ValueError(
                f"{type(self).__name__} PROCESS_BLOCK must return np.ndarray, "
                f"got {type(result).__name__}."
            )

        if self.ALLOW_SHAPE_CHANGE:
            return

        if result.ndim != block.ndim:
            raise ValueError(
                f"{type(self).__name__} output ndim {result.ndim} must equal "
                f"input block ndim {block.ndim}. Set ALLOW_SHAPE_CHANGE=True if intentional."
            )
        if result.shape != block.shape:
            raise ValueError(
                f"{type(self).__name__} output shape {result.shape} must equal "
                f"input block shape {block.shape}. Set ALLOW_SHAPE_CHANGE=True if intentional."
            )

    def _validate_output_dtype(
        self,
        result: np.ndarray,
        block: np.ndarray,
        expected_output_dtype: np.dtype,
        return_type: str,
        node_id: Optional[str],
    ) -> None:
        parsed = parse_port_type(return_type)
        expected_dtype = None
        requirement = None

        if parsed.dtype == "same":
            expected_dtype = np.dtype(block.dtype)
            requirement = f"dtype {expected_dtype}"
        elif parsed.dtype not in (None, "any"):
            expected_dtype = dtype_name_to_numpy(parsed.dtype)
            requirement = f"dtype {expected_dtype}"
        elif self.OUTPUT_DTYPE is not None:
            expected_dtype = np.dtype(expected_output_dtype)
            requirement = f"legacy OUTPUT_DTYPE {expected_dtype}"

        if expected_dtype is None:
            return

        if np.dtype(result.dtype) != np.dtype(expected_dtype):
            raise TypeError(
                f"{type(self).__name__}[{node_id}] declared RETURN_TYPES {return_type} "
                f"requiring {requirement}, but PROCESS_BLOCK returned {result.dtype}. "
                "BaseBlockMapNode does not auto-cast. Fix PROCESS_BLOCK or insert "
                "an explicit DaskTypeCast node downstream."
            )

    def _should_skip(self, block: np.ndarray, block_info: dict, params: dict, runtime: dict) -> bool:
        if self.SKIP_EMPTY_BLOCKS and block.size == 0:
            return True
        if self.SKIP_ALL_ZERO_BLOCKS and np.all(block == 0):
            return True

        custom_skip = getattr(self, "should_skip_block", None)
        if custom_skip is None:
            return False

        try:
            sig = inspect.signature(custom_skip)
            names = list(sig.parameters)
            if names == ["block"]:
                return bool(custom_skip(block))
            if "ctx" in names:
                ctx = self._build_context(block, block_info, runtime)
                return bool(custom_skip(block, ctx=ctx))
        except Exception:
            pass

        try:
            return bool(custom_skip(block, block_info, params, runtime))
        except TypeError:
            return bool(custom_skip(block))

    def _resolve_device_hint(self) -> str:
        try:
            from distributed import get_worker
            worker = get_worker()
            assigned = getattr(worker, "assigned_gpu", None)
            if assigned:
                return assigned
        except Exception:
            pass

        try:
            import torch
            if torch.cuda.is_available():
                return "cuda:0"
        except Exception:
            pass
        return "cpu"

    def _report_chunk(self, runtime: dict, chunk_type: str) -> None:
        node_id = runtime.get("node_id")
        if not node_id:
            return
        report_progress(
            node_id,
            execution_id=runtime.get("execution_id"),
            chunk_type=chunk_type,
        )


class SegmentationBlockMapNode(BaseBlockMapNode):
    SKIP_EMPTY_BLOCKS = True
    SKIP_ALL_ZERO_BLOCKS = True
    FAILURE_POLICY = "zeros_like"

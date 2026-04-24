"""
BaseBlockMapNode: BlockMap 模式节点的抽象基类。

BlockMap 模式：输入为 Dask Array，每个 block 独立处理，输出仍为常规 Dask Array，节点保持 lazy 特性。

设计原则：
- 节点作者只需编写 process_block（单 block 算法逻辑）
- Base 负责处理：map_blocks 包装、运行时注入、跳过/失败/进度、dtype/meta
- device_hint 在执行时（在 worker 上）解析，而非图构建时
- 安全默认值：SKIP_EMPTY_BLOCKS=True, SKIP_ALL_ZERO_BLOCKS=False, FAILURE_POLICY="raise"
- 输出 shape 约定：result.ndim == block.ndim 且 result.shape == block.shape（默认）
"""

import numpy as np
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Dict, Any, Callable
import inspect

from core.registry import register_node, ProgressType
from utils.progress_helper import report_progress


class BaseBlockMapNode(ABC):
    """
    BlockMap 节点抽象基类。

    适用场景：
    - 输入：单个常规 Dask Array
    - 每个 block 独立处理（无跨 block 依赖）
    - 适用于 map_blocks
    - 输出：常规 Dask Array（默认与输入 shape/chunks 相同）
    - 节点保持 lazy 特性（execute() 只构建图，不调用 compute）

    节点作者只需实现：
    1. INPUT_TYPES（类方法）
    2. process_block(block, block_info, params, runtime) -> np.ndarray（抽象方法）
    3. RETURN_TYPES, FUNCTION="execute"

    输出 shape 约定（默认，由 Base 强制执行）：
    - process_block 必须返回 np.ndarray
    - result.ndim 必须等于 block.ndim
    - result.shape 必须等于 block.shape
    - 这是默认行为（ALLOW_SHAPE_CHANGE=False）。
      如果子类故意产生 shape 变化的输出，可设置 ALLOW_SHAPE_CHANGE=True，
      并在需要时覆盖 validate_output_block()。

    可选覆盖：
    - OUTPUT_DTYPE：显式指定输出 dtype（默认：输入数组的 dtype）
    - SKIP_EMPTY_BLOCKS：跳过 size==0 的 blocks（默认：True）
    - SKIP_ALL_ZERO_BLOCKS：跳过全零 blocks（默认：False，需主动开启）
    - FAILURE_POLICY："raise"（默认）或 "zeros_like"
    - ALLOW_SHAPE_CHANGE：为 True 时跳过 shape 验证（默认：False）
    - should_skip_block(block, block_info, params, runtime) -> bool：自定义跳过逻辑
      （为兼容旧版也支持单参数签名）
    - validate_output_block(result, block) -> None：自定义验证逻辑

    不在当前范畴（由其他 Base 处理）：
    - halo / map_overlap / 跨 block 依赖
    - Array -> Table / 不规则输出
    - 全局收敛节点（如 DaskStats）
    - sink / writer 节点
    - 节点内部 rechunking
    """

    # ---------- 节点协议（子类必须定义）----------
    # INPUT_TYPES = {...}
    # RETURN_TYPES = (...)
    # FUNCTION = "execute"

    # ---------- 默认行为（通常保持即可）----------
    CATEGORY = "BrainFlow/BlockMap"
    DISPLAY_NAME = "BlockMap Node"
    PROGRESS_TYPE = ProgressType.CHUNK_COUNT

    # 跳过策略
    SKIP_EMPTY_BLOCKS = True        # 安全默认值：size==0 的 blocks 无数据
    SKIP_ALL_ZERO_BLOCKS = False   # 非安全默认值：许多算法应正常处理零值

    # 失败策略
    # "raise": 异常向上传播（通用节点的安全默认值）
    # "zeros_like": 异常 -> 返回 zeros_like(block)，报告 "failed"
    FAILURE_POLICY = "raise"

    # 输出 shape 约定
    # False = 强制 result.ndim == block.ndim 且 result.shape == block.shape
    # True = 跳过 shape 验证（适用于故意改变 shape 的节点）
    ALLOW_SHAPE_CHANGE = False

    # 输出 dtype
    # None = 使用输入数组的 dtype
    # 设置特定 dtype 以覆盖
    OUTPUT_DTYPE = None

    # ---------- 抽象方法（子类必须实现）----------

    @abstractmethod
    def process_block(
        self,
        block: np.ndarray,
        block_info: dict,
        params: dict,
        runtime: dict,
    ) -> np.ndarray:
        """
        单个 block 处理算法。由子类实现。

        参数
        ----------
        block : np.ndarray
            当前 chunk 的 numpy 数据（非 dask array）。
            注意：空 blocks（size==0）可能会被跳过（如果 SKIP_EMPTY_BLOCKS=True）
            然后再调用本方法。但全零 blocks 不会自动跳过，
            除非 SKIP_ALL_ZERO_BLOCKS=True。
        block_info : dict
            Dask 在执行时注入的 block 元数据。标准字段：
            - "shape": chunk shape
            - "chunk-location": block 在原始数组中的索引
            - "dtype": 数据类型
            其他 Dask map_blocks 标准字段也可能存在。
            如需要可用于空间感知处理。
        params : dict
            节点配置参数（来自 INPUT_TYPES）。
            不包含框架内部字段如 _node_id、_execution_id。
        runtime : dict
            运行时上下文，包含：
            - "node_id": 节点 ID（用于进度报告）
            - "execution_id": 执行 ID
            - "device_hint": 设备字符串，如 "cuda:0" 或 "cpu"
            注意：device_hint 在执行时（在 worker 上）解析，
            而非图构建时（execute 在客户端运行）。

        返回
        -------
        np.ndarray
            处理的 block 数据。必须满足输出 shape 约定：
            - 必须是 np.ndarray
            - result.ndim == block.ndim（除非 ALLOW_SHAPE_CHANGE=True）
            - result.shape == block.shape（除非 ALLOW_SHAPE_CHANGE=True）
            - 返回的 numpy 数组会被 map_blocks 自动组装成输出 Dask Array。
              不要返回 dask array。
        """
        ...

    # ---------- execute（系统调用，节点作者无需覆盖）----------

    def execute(self, dask_arr, **kwargs) -> Tuple:
        """
        构建 map_blocks 惰性图。

        此方法由执行器在图构建阶段调用。
        返回一个惰性 Dask Array（不调用 compute）。
        """
        node_id = kwargs.get("_node_id")
        execution_id = kwargs.get("_execution_id")

        # 构建静态运行时（在图构建时可确定）
        static_runtime = {
            "node_id": node_id,
            "execution_id": execution_id,
        }

        # 提取节点参数（过滤框架内部字段）
        params = self._extract_params(kwargs)

        # 确定输出 dtype
        output_dtype = self._resolve_output_dtype(dask_arr.dtype, params)

        # 为 map_blocks 构建 meta
        meta = self._build_meta(output_dtype)

        # 创建包装函数（添加跳过/失败/进度/验证逻辑）
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

        # 调用 map_blocks
        # 注意：我们不把 block_info 作为 kwarg 传递。
        # Dask 通过函数签名在执行时注入 block_info。
        # 包装函数接受 block_info 作为可选的位置参数。
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

    # ---------- 内部方法 ----------

    def _extract_params(self, kwargs) -> Dict[str, Any]:
        """提取节点可配置参数，过滤框架内部字段。"""
        framework_keys = {"_node_id", "_execution_id", "dask_arr"}
        return {k: v for k, v in kwargs.items() if k not in framework_keys}

    def _resolve_output_dtype(self, input_dtype, params) -> np.dtype:
        """解析输出 dtype。优先级：OUTPUT_DTYPE > infer_output_dtype > input_dtype。"""
        if self.OUTPUT_DTYPE is not None:
            return self.OUTPUT_DTYPE
        if hasattr(self, "infer_output_dtype"):
            return self.infer_output_dtype(input_dtype, params)
        return input_dtype

    def _build_meta(self, dtype) -> np.ndarray:
        """为 map_blocks 构建 meta 数组（具有正确 dtype 的空数组）。"""
        return np.array((), dtype=dtype)

    # ---------- 输出 shape 约定 ----------

    def _validate_output_block(self, result: np.ndarray, block: np.ndarray) -> None:
        """
        验证 process_block 输出是否满足 shape 约定。

        默认约定（当 ALLOW_SHAPE_CHANGE=False 时）：
        - result 必须是 np.ndarray
        - result.ndim == block.ndim
        - result.shape == block.shape

        如果违反约定，抛出带清晰消息的 ValueError。
        子类可以覆盖此方法自定义验证逻辑，但一般不应随意放宽默认约定。
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

    # ---------- 跳过逻辑 ----------

    def _should_skip(self, block: np.ndarray, block_info: dict, params: dict, runtime: dict) -> bool:
        """
        判断 block 是否应该被跳过。

        跳过决策顺序：
        1. SKIP_EMPTY_BLOCKS：block.size == 0
        2. SKIP_ALL_ZERO_BLOCKS：np.all(block == 0)
        3. should_skip_block(...)：自定义节点定义的跳过逻辑（如果有）

        参数
        ----------
        block : np.ndarray
        block_info : dict（Dask 在执行时注入）
        params : dict（节点参数）
        runtime : dict（node_id, execution_id, device_hint）

        返回
        -------
        bool: True 表示 block 应该被跳过
        """
        # 1. 空 block 跳过（始终安全）
        if self.SKIP_EMPTY_BLOCKS and block.size == 0:
            return True

        # 2. 全零 block 跳过（需主动开启）
        if self.SKIP_ALL_ZERO_BLOCKS and np.all(block == 0):
            return True

        # 3. 自定义跳过钩子 — 同时支持新版全参数签名和旧版单参数签名
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
                        # 签名不明确 — 优先尝试新版，失败则回退到旧版
                        try:
                            return custom_skip(block, block_info, params, runtime)
                        except TypeError:
                            return custom_skip(block)
                except (ValueError, TypeError):
                    # inspect.signature 在内置函数上可能失败 — 优先尝试新版签名
                    try:
                        return custom_skip(block, block_info, params, runtime)
                    except TypeError:
                        return custom_skip(block)
            else:
                # inspect.signature 不可用 — 假设新版签名
                try:
                    return custom_skip(block, block_info, params, runtime)
                except TypeError:
                    return custom_skip(block)

        return False

    def _resolve_device_hint(self) -> str:
        """
        在执行时（在 worker 上）解析 device hint。

        在包装函数内部调用，而非在 execute() 中（execute 在客户端运行）。
        """
        try:
            from distributed import get_worker
            worker = get_worker()
            return getattr(worker, "assigned_gpu", "cpu")
        except Exception:
            return "cpu"

    def _make_wrapped_function(self, static_runtime: dict, params: dict, output_dtype: np.dtype, node_id: str = None):
        """
        包装 process_block，添加：
        1. device_hint 解析（执行时）
        2. runtime 合并
        3. 跳过逻辑（带完整上下文：block, block_info, params, runtime）
        4. 输出 shape 验证
        5. 失败处理
        6. 进度报告
        """
        skip_decider = self._should_skip  # 绑定方法，带完整上下文
        failure_policy = self.FAILURE_POLICY
        validate_fn = self._validate_output_block
        validate_output_block_method = getattr(self, "validate_output_block", None)
        allow_shape_change = self.ALLOW_SHAPE_CHANGE

        def wrapped(block, block_info=None):
            # block_info：通过函数签名由 Dask 在执行时注入
            block_info = block_info or {}

            # ----- 在执行时解析 device_hint -----
            # 优先级：worker.assigned_gpu > torch.cuda.is_available() > "cpu"
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

            # ----- 构建完整 runtime -----
            runtime = {
                **static_runtime,
                "device_hint": device_hint,
            }

            # ----- 跳过决策（带完整上下文）-----
            if skip_decider(block, block_info, params, runtime):
                node_id = static_runtime.get("node_id")
                execution_id = static_runtime.get("execution_id")
                if node_id:
                    report_progress(node_id, execution_id=execution_id, chunk_type="skipped")
                return np.zeros_like(block, dtype=output_dtype)

            # ----- 调用 process_block -----
            if failure_policy == "zeros_like":
                try:
                    result = self.process_block(block, block_info, params, runtime)

                    # ----- 输出 shape 约定验证 -----
                    # 如果子类覆盖了 validate_output_block，则使用子类的；否则使用 Base 的默认验证
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
                # "raise" 策略：异常向上传播
                result = self.process_block(block, block_info, params, runtime)

                # ----- 输出 shape 约定验证 -----
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

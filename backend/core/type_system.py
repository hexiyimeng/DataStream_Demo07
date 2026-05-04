from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np


PORT_DTYPE_TO_NUMPY = {
    "uint8": np.uint8,
    "uint16": np.uint16,
    "uint32": np.uint32,
    "int16": np.int16,
    "int32": np.int32,
    "float32": np.float32,
    "float64": np.float64,
    "bool": np.bool_,
}

SPECIAL_DASK_DTYPES = {"any", "same"}
SPECIAL_MODEL_PROVIDERS = {"any"}


@dataclass(frozen=True)
class ParsedPortType:
    raw: str
    container: str
    dtype: Optional[str] = None


def parse_port_type(port_type: str | object) -> ParsedPortType:
    raw = str(port_type or "").strip()
    if "[" not in raw or not raw.endswith("]"):
        return ParsedPortType(raw=raw, container=raw, dtype=None)

    container, dtype_part = raw.split("[", 1)
    dtype = dtype_part[:-1].strip().lower() or None
    return ParsedPortType(raw=raw, container=container.strip(), dtype=dtype)


def is_dask_array_type(port_type: str | object) -> bool:
    return parse_port_type(port_type).container == "DASK_ARRAY"


def is_model_type(port_type: str | object) -> bool:
    return parse_port_type(port_type).container == "MODEL"


def dtype_name_to_numpy(dtype_name: str | None, input_dtype=None):
    if dtype_name is None or dtype_name == "any":
        return None
    if dtype_name == "same":
        return None if input_dtype is None else np.dtype(input_dtype)
    if dtype_name not in PORT_DTYPE_TO_NUMPY:
        raise ValueError(f"Unsupported DASK_ARRAY dtype '{dtype_name}'.")
    return np.dtype(PORT_DTYPE_TO_NUMPY[dtype_name])


def dtype_name_for_numpy(dtype) -> str:
    np_dtype = np.dtype(dtype)
    if np_dtype == np.dtype(np.bool_):
        return "bool"
    return np_dtype.name


def can_connect_types(source_type: str, target_type: str) -> Tuple[bool, Optional[str]]:
    source = parse_port_type(source_type)
    target = parse_port_type(target_type)

    if source.container != target.container:
        return False, f"source container {source.container} does not match target container {target.container}"

    if source.container == "MODEL":
        source_provider = source.dtype
        target_provider = target.dtype

        if target_provider in (None, "any"):
            return True, None
        if source_provider in (None, "any"):
            return False, "source model provider is unknown"
        if source_provider == target_provider:
            return True, None
        return False, f"source model provider {source_provider} does not match target provider {target_provider}"

    if source.container != "DASK_ARRAY":
        if source.raw == target.raw or source.raw == "*" or target.raw == "*":
            return True, None
        return False, f"source type {source.raw} does not match target type {target.raw}"

    source_dtype = source.dtype
    target_dtype = target.dtype

    if target_dtype in (None, "any"):
        return True, None

    if target_dtype == "same":
        if source_dtype in (None, "any"):
            return False, "source dtype is unknown; insert DaskTypeCast or use a typed source"
        return True, None

    if source_dtype in (None, "any"):
        return False, "source dtype is unknown; insert DaskTypeCast or use a typed source"

    if source_dtype == "same":
        return False, "source dtype is relative; insert DaskTypeCast or use a concrete typed source"

    if source_dtype == target_dtype:
        return True, None

    return False, f"source dtype {source_dtype} does not match target dtype {target_dtype}"

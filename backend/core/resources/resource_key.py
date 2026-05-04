from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _freeze_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((str(k), _freeze_value(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple, set)):
        return tuple(_freeze_value(v) for v in value)
    return value


def freeze_options(options: dict[str, Any] | None) -> tuple[tuple[str, Any], ...]:
    if not options:
        return ()
    return tuple(sorted((str(k), _freeze_value(v)) for k, v in options.items()))


@dataclass(frozen=True)
class ResourceKey:
    provider: str
    name: str
    device: str
    options: tuple[tuple[str, Any], ...] = ()

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ModelRef:
    """Lightweight serializable pointer to a model resource.

    A ModelRef is safe to pass through graph-building and Dask serialization.
    It never owns the actual model object, GPU tensors, file handles, or cache
    state. Worker-side resource managers materialize it only inside block tasks.
    """

    provider: str
    name: str
    options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", str(self.provider or "").strip().lower())
        object.__setattr__(self, "name", str(self.name or "").strip())
        object.__setattr__(self, "options", dict(self.options or {}))

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "name": self.name,
            "options": dict(self.options),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelRef":
        if not isinstance(data, dict):
            raise TypeError(f"ModelRef.from_dict expected dict, got {type(data).__name__}.")
        return cls(
            provider=data.get("provider", ""),
            name=data.get("name", ""),
            options=data.get("options") or {},
        )

    def stable_key(self) -> tuple[str, str, tuple[tuple[str, Any], ...]]:
        return (
            self.provider,
            self.name,
            _freeze_options(self.options),
        )


def _freeze_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((str(k), _freeze_value(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple, set)):
        return tuple(_freeze_value(v) for v in value)
    return value


def _freeze_options(options: dict[str, Any] | None) -> tuple[tuple[str, Any], ...]:
    if not options:
        return ()
    return tuple(sorted((str(k), _freeze_value(v)) for k, v in options.items()))

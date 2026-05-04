from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

from core.model_ref import ModelRef


BACKEND_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODELS_ROOT = BACKEND_ROOT / "models"


class ModelRegistry:
    """Main-process model catalog and lightweight ModelRef validator."""

    _DEFAULT_MODELS = {
        "cellpose": ("cpsam",),
    }

    def __init__(self, models_root: Path | None = None):
        self.models_root = Path(models_root or DEFAULT_MODELS_ROOT)

    def list_models(self, provider: str) -> list[str]:
        provider = self._normalize_provider(provider)
        names = set(self._DEFAULT_MODELS.get(provider, ()))
        for directory in self._provider_dirs(provider):
            if not directory.exists() or not directory.is_dir():
                continue
            for item in directory.iterdir():
                if item.name.startswith("."):
                    continue
                if item.is_file():
                    names.add(item.name)
        return sorted(names)

    def resolve_model_path(self, provider: str, name: str) -> str | None:
        provider = self._normalize_provider(provider)
        name = str(name or "").strip()
        if not name:
            return None

        candidate = Path(name)
        if candidate.exists():
            return str(candidate.resolve())

        for directory in self._provider_dirs(provider):
            path = directory / name
            if path.exists():
                return str(path.resolve())
        return None

    def validate_model_ref(self, model_ref: ModelRef) -> None:
        if not isinstance(model_ref, ModelRef):
            raise TypeError(f"Expected ModelRef, got {type(model_ref).__name__}.")
        provider = self._normalize_provider(model_ref.provider)
        if not provider:
            raise ValueError("ModelRef provider is required.")
        if not model_ref.name:
            raise ValueError(f"ModelRef name is required for provider {provider!r}.")

        if provider == "cellpose":
            if model_ref.name in self._DEFAULT_MODELS["cellpose"]:
                return
            if self.resolve_model_path(provider, model_ref.name):
                return
            searched = ", ".join(str(p) for p in self._provider_dirs(provider))
            raise FileNotFoundError(
                f"Cellpose model {model_ref.name!r} was not found. "
                f"Use built-in 'cpsam' or place the file under one of: {searched}."
            )

    def _provider_dirs(self, provider: str) -> Iterable[Path]:
        yield self.models_root / provider

        # Preserve the existing Cellpose path convention without importing Cellpose.
        if provider == "cellpose":
            legacy_root = os.getenv("CELLPOSE_LOCAL_MODELS_PATH")
            if legacy_root:
                legacy = Path(legacy_root)
                yield legacy / "cellpose"
                yield legacy

    @staticmethod
    def _normalize_provider(provider: str) -> str:
        return str(provider or "").strip().lower()


model_registry = ModelRegistry()


def list_models(provider: str) -> list[str]:
    return model_registry.list_models(provider)


def resolve_model_path(provider: str, name: str) -> str | None:
    return model_registry.resolve_model_path(provider, name)


def validate_model_ref(model_ref: ModelRef) -> None:
    model_registry.validate_model_ref(model_ref)

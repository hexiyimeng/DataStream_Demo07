from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


BACKEND_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODELS_ROOT = BACKEND_ROOT / "models"
KNOWN_MODEL_EXTENSIONS = (
    ".pth",
    ".pt",
    ".ckpt",
    ".onnx",
    ".engine",
    ".safetensors",
    ".bin",
)


class ModelRegistry:
    """
    Local model catalog for WorkFlow.

    The registry scans local ``backend/models/{provider}/`` directories (or
    ``WorkFlow_MODELS_DIR/{provider}/`` when overridden), lists local model
    files/directories, and resolves requested model names or paths to absolute
    local paths.

    It intentionally does not hardcode third-party library built-in models,
    download models, know provider-specific defaults, or decide whether a
    missing model name is valid for a provider library.
    """

    def __init__(self, models_root: Path | None = None):
        env_root = os.getenv("WorkFlow_MODELS_DIR")
        self.models_root = Path(models_root or env_root or DEFAULT_MODELS_ROOT)

    def list_models(self, provider: str) -> list[str]:
        """List local model files/directories for a provider."""
        provider = self._normalize_provider(provider)
        names: set[str] = set()
        for directory in self._provider_dirs(provider):
            if not directory.exists() or not directory.is_dir():
                continue
            for item in directory.iterdir():
                if item.name.startswith("."):
                    continue
                if item.is_file() or item.is_dir():
                    names.add(item.name)
                if item.is_file() and item.suffix.lower() in KNOWN_MODEL_EXTENSIONS:
                    names.add(item.stem)
        return sorted(names)

    def resolve_model_path(self, provider: str, name: str) -> str | None:
        """
        Resolve a local model reference to an absolute path.

        Missing names return ``None`` because they may be valid provider
        built-ins, remote identifiers, or downloadable model names.
        """
        provider = self._normalize_provider(provider)
        name = str(name or "").strip()
        if not name:
            return None

        candidate = Path(name)
        if candidate.exists():
            return str(candidate.resolve())

        for directory in self._provider_dirs(provider):
            resolved = self._resolve_in_directory(directory, name)
            if resolved:
                return resolved

        if provider == "cellpose":
            for directory in self._cellpose_legacy_dirs():
                resolved = self._resolve_in_directory(directory, name)
                if resolved:
                    return resolved
        return None

    def _provider_dirs(self, provider: str) -> Iterable[Path]:
        yield self.models_root / provider

    def _cellpose_legacy_dirs(self) -> Iterable[Path]:
        legacy_root = os.getenv("CELLPOSE_LOCAL_MODELS_PATH")
        if legacy_root:
            legacy = Path(legacy_root)
            yield legacy / "cellpose"
            yield legacy

    def _resolve_in_directory(self, directory: Path, name: str) -> str | None:
        path = directory / name
        if path.exists():
            return str(path.resolve())

        for extension in KNOWN_MODEL_EXTENSIONS:
            path = directory / f"{name}{extension}"
            if path.exists():
                return str(path.resolve())
        return None

    @staticmethod
    def _normalize_provider(provider: str) -> str:
        return str(provider or "").strip().lower()


model_registry = ModelRegistry()


def list_models(provider: str) -> list[str]:
    return model_registry.list_models(provider)


def resolve_model_path(provider: str, name: str) -> str | None:
    return model_registry.resolve_model_path(provider, name)

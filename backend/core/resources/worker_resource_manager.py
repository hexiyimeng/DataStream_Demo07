from __future__ import annotations

from dataclasses import dataclass
import logging
import threading
import time
from typing import Any, Callable

from core.model_ref import ModelRef
from core.model_registry import validate_model_ref
from core.resources.resource_key import ResourceKey
from core.resources.resource_provider import ResourceProvider


logger = logging.getLogger("BrainFlow.Resources")


@dataclass
class CachedResource:
    key: ResourceKey
    value: object
    ref_count: int
    created_at: float
    last_used_at: float
    hit_count: int
    disposer: Callable[[object], None] | None = None


class ResourceHandle:
    def __init__(self, manager: "WorkerResourceManager", key: ResourceKey, value: object):
        self._manager = manager
        self.key = key
        self.value = value
        self._released = False
        self._lock = threading.Lock()

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        self._manager.release(self)


class WorkerResourceManager:
    def __init__(self):
        self._providers: dict[str, ResourceProvider] = {}
        self._cache: dict[ResourceKey, CachedResource] = {}
        self._lock = threading.RLock()

    def register_provider(self, provider: ResourceProvider) -> None:
        with self._lock:
            self._providers[provider.name] = provider

    def acquire_model(self, model_ref: ModelRef, device: str) -> ResourceHandle:
        if not isinstance(model_ref, ModelRef):
            raise TypeError(f"resolve_model expected ModelRef, got {type(model_ref).__name__}.")
        provider = self._get_provider(model_ref.provider)
        provider.validate(model_ref)
        key = provider.build_key(model_ref, device)

        # Keep creation under the lock for correctness. Worker threads are
        # one-per-process in the current cluster profiles, and this avoids
        # duplicate model loads without a more complex pending-load protocol.
        with self._lock:
            cached = self._cache.get(key)
            if cached is None:
                logger.info("[Resources] Loading %s:%s on %s", key.provider, key.name, key.device)
                value = provider.create(model_ref, device)
                now = time.time()
                cached = CachedResource(
                    key=key,
                    value=value,
                    ref_count=0,
                    created_at=now,
                    last_used_at=now,
                    hit_count=0,
                    disposer=provider.dispose,
                )
                self._cache[key] = cached
            cached.ref_count += 1
            cached.hit_count += 1
            cached.last_used_at = time.time()
            return ResourceHandle(self, key, cached.value)

    def release(self, handle: ResourceHandle) -> None:
        with self._lock:
            cached = self._cache.get(handle.key)
            if cached is None:
                logger.warning("[Resources] Release ignored for unknown key %s", handle.key)
                return
            cached.ref_count = max(0, cached.ref_count - 1)
            cached.last_used_at = time.time()

    def clear_idle(self, max_idle_seconds: float | None = None) -> dict[str, Any]:
        now = time.time()
        disposed: list[CachedResource] = []
        with self._lock:
            for key, cached in list(self._cache.items()):
                if cached.ref_count != 0:
                    continue
                if max_idle_seconds is not None and (now - cached.last_used_at) < max_idle_seconds:
                    continue
                disposed.append(self._cache.pop(key))

        errors = []
        for cached in disposed:
            try:
                if cached.disposer:
                    cached.disposer(cached.value)
            except Exception as exc:
                errors.append(f"{cached.key}: {type(exc).__name__}: {exc}")
                logger.warning("[Resources] Dispose failed for %s: %s", cached.key, exc)
        return {"disposed": len(disposed), "errors": errors}

    def force_clear(self) -> dict[str, Any]:
        with self._lock:
            disposed = list(self._cache.values())
            self._cache.clear()

        errors = []
        for cached in disposed:
            try:
                if cached.disposer:
                    cached.disposer(cached.value)
            except Exception as exc:
                errors.append(f"{cached.key}: {type(exc).__name__}: {exc}")
                logger.warning("[Resources] Force dispose failed for %s: %s", cached.key, exc)
        return {"disposed": len(disposed), "errors": errors}

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "providers": sorted(self._providers.keys()),
                "resources": [
                    {
                        "provider": key.provider,
                        "name": key.name,
                        "device": key.device,
                        "ref_count": cached.ref_count,
                        "hit_count": cached.hit_count,
                        "idle_seconds": max(0.0, time.time() - cached.last_used_at),
                    }
                    for key, cached in self._cache.items()
                ],
            }

    def _get_provider(self, provider_name: str) -> ResourceProvider:
        provider_name = str(provider_name or "").strip().lower()
        with self._lock:
            provider = self._providers.get(provider_name)
        if provider is None:
            self._register_builtin_provider(provider_name)
            with self._lock:
                provider = self._providers.get(provider_name)
        if provider is None:
            raise ValueError(f"No worker resource provider registered for {provider_name!r}.")
        return provider

    def _register_builtin_provider(self, provider_name: str) -> None:
        if provider_name == "cellpose":
            from model_providers.cellpose_provider import CellposeResourceProvider

            self.register_provider(CellposeResourceProvider())


class BlockResourcesMixin:
    def __init__(self):
        self._handles_by_key: dict[ResourceKey, ResourceHandle] = {}

    def _resolve_model_handle(self, model_ref: ModelRef, device: str) -> ResourceHandle:
        manager = get_worker_resource_manager()
        provider = manager._get_provider(model_ref.provider)
        key = provider.build_key(model_ref, device)
        if key in self._handles_by_key:
            return self._handles_by_key[key]
        handle = manager.acquire_model(model_ref, device)
        self._handles_by_key[key] = handle
        return handle

    def release_all(self) -> None:
        handles = list(self._handles_by_key.values())
        self._handles_by_key.clear()
        for handle in handles:
            handle.release()


_manager_lock = threading.Lock()
_worker_resource_manager: WorkerResourceManager | None = None


def get_worker_resource_manager() -> WorkerResourceManager:
    global _worker_resource_manager
    if _worker_resource_manager is None:
        with _manager_lock:
            if _worker_resource_manager is None:
                _worker_resource_manager = WorkerResourceManager()
    return _worker_resource_manager


def clear_idle_worker_resources(max_idle_seconds: float | None = None) -> dict[str, Any]:
    return get_worker_resource_manager().clear_idle(max_idle_seconds=max_idle_seconds)


def force_clear_worker_resources() -> dict[str, Any]:
    return get_worker_resource_manager().force_clear()


def get_worker_resource_stats() -> dict[str, Any]:
    return get_worker_resource_manager().stats()


def validate_model_refs_on_worker(refs: list[ModelRef]) -> dict[str, Any]:
    manager = get_worker_resource_manager()
    validated = []
    for ref in refs:
        if isinstance(ref, dict):
            ref = ModelRef.from_dict(ref)
        validate_model_ref(ref)
        provider = manager._get_provider(ref.provider)
        provider.validate(ref)
        validated.append(ref.to_dict())
    return {"validated": validated}

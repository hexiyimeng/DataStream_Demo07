from __future__ import annotations

from abc import ABC, abstractmethod

from core.model_ref import ModelRef
from core.resources.resource_key import ResourceKey, freeze_options


class ResourceProvider(ABC):
    name: str

    def build_key(self, model_ref: ModelRef, device: str) -> ResourceKey:
        return ResourceKey(
            provider=self.name,
            name=model_ref.name,
            device=device,
            options=freeze_options(model_ref.options),
        )

    def validate(self, model_ref: ModelRef) -> None:
        return None

    @abstractmethod
    def create(self, model_ref: ModelRef, device: str):
        raise NotImplementedError

    def dispose(self, resource) -> None:
        return None

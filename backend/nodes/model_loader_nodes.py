from __future__ import annotations

from core.model_ref import ModelRef
from core.model_registry import list_models, validate_model_ref
from core.registry import ProgressType, register_node


@register_node("CellposeModelLoader")
class CellposeModelLoader:
    CATEGORY = "BrainFlow/Models"
    DISPLAY_NAME = "Load Cellpose Model"
    DESCRIPTION = "Creates a lightweight Cellpose ModelRef. The real model loads lazily inside Dask workers."
    PROGRESS_TYPE = ProgressType.STATE_ONLY
    RETURN_TYPES = ("MODEL[cellpose]",)
    RETURN_NAMES = ("model",)
    FUNCTION = "execute"

    @classmethod
    def INPUT_TYPES(cls):
        models = list_models("cellpose") or ["cpsam"]
        default = "cpsam" if "cpsam" in models else models[0]
        return {
            "required": {
                "model_name": (models, {"default": default}),
            }
        }

    def execute(self, model_name: str = "cpsam", **kwargs):
        model_ref = ModelRef(provider="cellpose", name=model_name or "cpsam", options={})
        validate_model_ref(model_ref)
        return (model_ref,)

from __future__ import annotations

from pathlib import Path
import numpy as np

from core.model_registry import list_models, resolve_model_path
from core.registry import register_node
from nodes.base import BaseBlockMapNode



def create_stardist3d_model(load_name: str, device: str):
    from stardist.models import StarDist3D

    p = Path(load_name)

    if p.exists():
        return StarDist3D(
            config=None,
            name=p.name,
            basedir=str(p.parent),
        )

    raise RuntimeError(
    "StarDist3D model was not resolved to a local path. "
    "Please install the model under backend/models/stardist3d/<model_name>."
    )


def stardist3d_block(
    block: np.ndarray,
    model_name: str = "3D_demo",
    prob_thresh: float = 0.5,
    nms_thresh: float = 0.4,
    ctx=None,
):
    if ctx is None:
        raise RuntimeError("StarDist3D block requires ctx.")

    model_spec = ctx.resources.get("model_spec") if ctx.resources else None
    if not isinstance(model_spec, dict):
        requested = model_name or "3D_demo"
        model_spec = {
            "provider": "stardist3d",
            "requested_name": requested,
            "load_name": requested,
            "clear_cuda": True,
        }

    model = ctx.model(
        provider=model_spec["provider"],
        name=model_spec["load_name"],
        factory=create_stardist3d_model,
        clear_cuda=bool(model_spec.get("clear_cuda", True)),
    )

    from csbdeep.utils import normalize

    x = normalize(block)
    labels, details = model.predict_instances(
        x,
        prob_thresh=prob_thresh,
        nms_thresh=nms_thresh,
    )

    return labels.astype(np.uint16, copy=False)


@register_node("DaskStarDist3D")
class DaskStarDist3D(BaseBlockMapNode):
    CATEGORY = "WorkFlow/Segmentation"
    DISPLAY_NAME = "StarDist 3D"

    PROCESS_BLOCK = stardist3d_block

    RETURN_TYPES = ("DASK_ARRAY[uint16]",)
    RETURN_NAMES = ("label_dask",)

    @classmethod
    def INPUT_TYPES(cls):
        local_models = list_models("stardist3d")
        models = sorted(set(local_models))
    

        return {
            "required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "model_name": (models,),
                "prob_thresh": ("FLOAT", {"default": 0.5, "min": 0.0, "max": 1.0}),
                "nms_thresh": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 1.0}),
            },
            "optional": {
                "model_path": ("STRING", {"default": "", "multiline": False}),
            },
        }

    def preprocess(self, dask_arr, params: dict, runtime: dict) -> dict:
        requested_name = (
            params.get("model_path")
            or params.get("model_name")
            or "3D_demo"
        )

        load_name = (
            resolve_model_path("stardist3d", requested_name)
            or requested_name
        )

        return {
            "model_spec": {
                "provider": "stardist3d",
                "requested_name": requested_name,
                "load_name": load_name,
                "clear_cuda": True,
            }
        }
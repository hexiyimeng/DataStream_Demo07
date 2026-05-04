from __future__ import annotations

import logging
import os

from core.model_ref import ModelRef
from core.model_registry import resolve_model_path, validate_model_ref
from core.resources.disposers import collect_python_and_cuda
from core.resources.resource_provider import ResourceProvider


logger = logging.getLogger("BrainFlow.CellposeProvider")


class CellposeResourceProvider(ResourceProvider):
    name = "cellpose"

    def validate(self, model_ref: ModelRef) -> None:
        validate_model_ref(model_ref)

        # If this worker cannot import Cellpose, fail during preflight before
        # expensive Dask computation is submitted. This does not load a model.
        try:
            import cellpose  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "Cellpose is not installed on this worker. Install cellpose or "
                "run the workflow on workers with Cellpose available."
            ) from exc

    def create(self, model_ref: ModelRef, device: str):
        from cellpose import models
        import torch

        model_name = model_ref.name or "cpsam"
        device_obj = torch.device(device or "cpu")
        model_path = self._resolve_cellpose_model_path(model_name)

        self._reject_cp3_checkpoint_if_detectable(model_path, model_name)

        logger.info(
            "[CellposeProvider] Creating CellposeModel(pretrained_model=%r, device=%s)",
            model_path,
            device_obj,
        )
        model = models.CellposeModel(
            gpu=(device_obj.type == "cuda"),
            pretrained_model=model_path,
            device=device_obj,
        )

        actual_path = getattr(model, "pretrained_model", None) or model_path
        resolved_name = os.path.basename(os.path.normpath(str(actual_path)))
        setattr(model, "_brainflow_model_ref", model_ref.to_dict())
        setattr(model, "_brainflow_resolved_name", resolved_name)

        if resolved_name != model_name:
            known_models = {"cyto", "nuclei", "cyto2", "cyto3", "cpsam"}
            is_fallback = resolved_name in known_models and model_name in known_models
            log_fn = logger.warning if is_fallback else logger.error
            log_fn(
                "[CellposeProvider] Requested %r but Cellpose reported %r. "
                "This may be an internal fallback or a version compatibility issue.",
                model_name,
                resolved_name,
            )

        return model

    def dispose(self, resource) -> None:
        try:
            del resource
        finally:
            collect_python_and_cuda()

    def _resolve_cellpose_model_path(self, model_name: str) -> str:
        resolved = resolve_model_path("cellpose", model_name)
        if resolved:
            return resolved
        return model_name

    def _reject_cp3_checkpoint_if_detectable(self, model_path: str, model_name: str) -> None:
        if not model_path or not os.path.exists(model_path) or os.path.isdir(model_path):
            return

        try:
            import torch

            ckpt = torch.load(model_path, map_location="cpu", weights_only=False)
        except Exception:
            return

        if isinstance(ckpt, dict) and "diam_mean" in ckpt:
            raise ValueError(
                f"Cellpose model {model_name!r} appears to be a Cellpose 3 "
                "checkpoint. BrainFlow's current Cellpose provider uses "
                "Cellpose v4 CellposeModel and supports CP4/ViT-SAM style "
                "models. Use a CP4 model such as 'cpsam' or run with a "
                "Cellpose 3 compatible provider."
            )

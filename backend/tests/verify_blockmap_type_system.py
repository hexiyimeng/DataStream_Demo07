import os
import sys
import pickle

import numpy as np
import dask.array as da

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.registry import NODE_CLASS_MAPPINGS
from core.model_ref import ModelRef
from core.resources.resource_provider import ResourceProvider
from core.resources.worker_resource_manager import WorkerResourceManager, get_worker_resource_manager
from core.type_system import can_connect_types, parse_port_type
from nodes.base import BaseBlockMapNode
from nodes.model_loader_nodes import CellposeModelLoader
from nodes.cellpose_node import DaskCellpose
from nodes.type_cast_node import DaskTypeCast
from services.executor import validate_graph_types


def assert_eq(actual, expected):
    assert actual == expected, f"expected {expected!r}, got {actual!r}"


def compute_node(node, arr, **kwargs):
    out = node.execute(arr, **kwargs)[0]
    return out.compute(scheduler="single-threaded")


def verify_type_system():
    cases = {
        "DASK_ARRAY[uint16]": ("DASK_ARRAY", "uint16"),
        "DASK_ARRAY[float32]": ("DASK_ARRAY", "float32"),
        "DASK_ARRAY[any]": ("DASK_ARRAY", "any"),
        "DASK_ARRAY[same]": ("DASK_ARRAY", "same"),
        "DASK_ARRAY": ("DASK_ARRAY", None),
        "MODEL[cellpose]": ("MODEL", "cellpose"),
        "MODEL[any]": ("MODEL", "any"),
        "FLOAT": ("FLOAT", None),
    }
    for raw, expected in cases.items():
        parsed = parse_port_type(raw)
        assert_eq((parsed.container, parsed.dtype), expected)

    matrix = [
        ("DASK_ARRAY[uint16]", "DASK_ARRAY[uint16]", True),
        ("DASK_ARRAY[uint16]", "DASK_ARRAY[float32]", False),
        ("DASK_ARRAY[uint16]", "DASK_ARRAY[any]", True),
        ("DASK_ARRAY[any]", "DASK_ARRAY[uint16]", False),
        ("MODEL[cellpose]", "MODEL[cellpose]", True),
        ("MODEL[cellpose]", "MODEL[any]", True),
        ("MODEL[pytorch]", "MODEL[pytorch]", True),
        ("MODEL[pytorch]", "MODEL[cellpose]", False),
        ("MODEL[cellpose]", "MODEL[pytorch]", False),
        ("FLOAT", "FLOAT", True),
        ("FLOAT", "INT", False),
    ]
    for source, target, expected in matrix:
        ok, _ = can_connect_types(source, target)
        assert_eq(ok, expected)


def verify_blockmap_calls_and_dtype():
    arr = da.from_array(np.arange(8, dtype=np.uint16).reshape(2, 4), chunks=(1, 4))

    def f_plain(block, scale, bias):
        return (block.astype(np.float32) * scale + bias).astype(np.float32)

    class PlainNode(BaseBlockMapNode):
        @classmethod
        def INPUT_TYPES(cls):
            return {"required": {
                "dask_arr": ("DASK_ARRAY[any]",),
                "scale": ("FLOAT", {"default": 2.0}),
                "bias": ("FLOAT", {"default": 1.0}),
                "extra": ("FLOAT", {"default": 99.0}),
            }}

        RETURN_TYPES = ("DASK_ARRAY[float32]",)
        PROCESS_BLOCK = f_plain

    plain = compute_node(PlainNode(), arr)
    assert_eq(plain.dtype, np.dtype("float32"))
    assert np.allclose(plain, np.arange(8, dtype=np.float32).reshape(2, 4) * 2 + 1)

    def f_ctx(block, scale, bias, ctx):
        assert ctx.node_id == "ctx-node"
        return (block.astype(np.float32) * scale + bias).astype(np.float32)

    class CtxNode(PlainNode):
        PROCESS_BLOCK = f_ctx

    compute_node(CtxNode(), arr, _node_id="ctx-node")

    def f_kwargs(block, **params):
        assert params["scale"] == 2.0
        assert params["bias"] == 1.0
        return block.astype(np.float32)

    class KwargsNode(PlainNode):
        PROCESS_BLOCK = f_kwargs

    compute_node(KwargsNode(), arr)

    def f_missing(block, required_param):
        return block

    class MissingNode(BaseBlockMapNode):
        RETURN_TYPES = ("DASK_ARRAY[any]",)
        PROCESS_BLOCK = f_missing

    try:
        MissingNode()._call_process_block(np.zeros((1,), dtype=np.uint8), {}, None)
    except TypeError as exc:
        assert "required_param" in str(exc)
    else:
        raise AssertionError("missing required PROCESS_BLOCK parameter did not fail")

    def f_uint16(block):
        return block.astype(np.uint16)

    class Uint16Node(BaseBlockMapNode):
        @classmethod
        def INPUT_TYPES(cls):
            return {"required": {"dask_arr": ("DASK_ARRAY[any]",)}}

        RETURN_TYPES = ("DASK_ARRAY[uint16]",)
        PROCESS_BLOCK = f_uint16

    assert_eq(compute_node(Uint16Node(), arr).dtype, np.dtype("uint16"))

    def f_float32(block):
        return block.astype(np.float32)

    class BadUint16Node(Uint16Node):
        PROCESS_BLOCK = f_float32

    try:
        compute_node(BadUint16Node(), arr)
    except TypeError as exc:
        assert "does not auto-cast" in str(exc)
    else:
        raise AssertionError("dtype mismatch did not fail")

    class SameNode(Uint16Node):
        RETURN_TYPES = ("DASK_ARRAY[same]",)
        PROCESS_BLOCK = f_uint16

    assert_eq(compute_node(SameNode(), arr).dtype, np.dtype("uint16"))

    class BadSameNode(SameNode):
        PROCESS_BLOCK = f_float32

    try:
        compute_node(BadSameNode(), arr)
    except TypeError as exc:
        assert "DASK_ARRAY[same]" in str(exc)
    else:
        raise AssertionError("same dtype mismatch did not fail")


def verify_type_cast():
    node = DaskTypeCast()

    uint16 = da.from_array(np.array([0, 1, 65535], dtype=np.uint16), chunks=(3,))
    out = compute_node(node, uint16, target_dtype="float32", clip=True)
    assert_eq(out.dtype, np.dtype("float32"))

    floats = da.from_array(np.array([-1.5, 1.2, 70000.0], dtype=np.float32), chunks=(3,))
    clipped = compute_node(node, floats, target_dtype="uint16", clip=True)
    assert_eq(clipped.dtype, np.dtype("uint16"))
    assert_eq(clipped.tolist(), [0, 1, 65535])

    unclipped = compute_node(node, floats, target_dtype="uint16", clip=False)
    assert_eq(unclipped.dtype, np.dtype("uint16"))

    same = compute_node(node, uint16, target_dtype="uint16", clip=True)
    assert_eq(same.dtype, np.dtype("uint16"))

    bools = compute_node(node, floats, target_dtype="bool", clip=True)
    assert_eq(bools.dtype, np.dtype("bool"))


def verify_graph_validation():
    class SourceUint16:
        RETURN_TYPES = ("DASK_ARRAY[uint16]",)
        RETURN_NAMES = ("out",)

    class SourceAny:
        RETURN_TYPES = ("DASK_ARRAY[any]",)
        RETURN_NAMES = ("out",)

    class TargetUint16:
        @classmethod
        def INPUT_TYPES(cls):
            return {"required": {"dask_arr": ("DASK_ARRAY[uint16]",)}}

    class TargetFloat32:
        @classmethod
        def INPUT_TYPES(cls):
            return {"required": {"dask_arr": ("DASK_ARRAY[float32]",)}}

    class TargetAny:
        @classmethod
        def INPUT_TYPES(cls):
            return {"required": {"dask_arr": ("DASK_ARRAY[any]",)}}

    NODE_CLASS_MAPPINGS.update({
        "VerifySourceUint16": SourceUint16,
        "VerifySourceAny": SourceAny,
        "VerifyTargetUint16": TargetUint16,
        "VerifyTargetFloat32": TargetFloat32,
        "VerifyTargetAny": TargetAny,
    })

    validate_graph_types({
        "s": {"type": "VerifySourceUint16", "inputs": {}},
        "t": {"type": "VerifyTargetUint16", "inputs": {"dask_arr": ["s", 0]}},
    })
    validate_graph_types({
        "s": {"type": "VerifySourceUint16", "inputs": {}},
        "t": {"type": "VerifyTargetAny", "inputs": {"dask_arr": ["s", 0]}},
    })
    validate_graph_types({
        "s": {"type": "VerifySourceUint16", "inputs": {}},
        "cast": {
            "type": "DaskTypeCast",
            "inputs": {"dask_arr": ["s", 0], "target_dtype": "float32", "clip": True},
        },
        "t": {"type": "VerifyTargetFloat32", "inputs": {"dask_arr": ["cast", 0]}},
    })

    for target_type in ("VerifyTargetFloat32", "VerifyTargetUint16"):
        source_type = "VerifySourceUint16" if target_type == "VerifyTargetFloat32" else "VerifySourceAny"
        try:
            validate_graph_types({
                "s": {"type": source_type, "inputs": {}},
                "t": {"type": target_type, "inputs": {"dask_arr": ["s", 0]}},
            })
        except ValueError as exc:
            assert "DaskTypeCast" in str(exc)
        else:
            raise AssertionError("invalid graph connection did not fail")


class FakeProvider(ResourceProvider):
    name = "fake"

    def __init__(self):
        self.created = 0
        self.disposed = 0

    def create(self, model_ref: ModelRef, device: str):
        self.created += 1
        return {"name": model_ref.name, "device": device}

    def dispose(self, resource) -> None:
        self.disposed += 1


def verify_model_ref_and_resource_manager():
    ref = ModelRef(provider="fake", name="demo", options={"a": 1})
    assert_eq(ModelRef.from_dict(ref.to_dict()).to_dict(), ref.to_dict())
    assert_eq(pickle.loads(pickle.dumps(ref)).to_dict(), ref.to_dict())

    provider = FakeProvider()
    manager = WorkerResourceManager()
    manager.register_provider(provider)

    h1 = manager.acquire_model(ref, "cpu")
    h2 = manager.acquire_model(ref, "cpu")
    assert h1.value is h2.value
    assert_eq(provider.created, 1)
    assert_eq(manager.stats()["resources"][0]["ref_count"], 2)

    h1.release()
    h1.release()
    assert_eq(manager.stats()["resources"][0]["ref_count"], 1)
    h2.release()
    assert_eq(manager.stats()["resources"][0]["ref_count"], 0)

    result = manager.clear_idle()
    assert_eq(result["disposed"], 1)
    assert_eq(provider.disposed, 1)

    h3 = manager.acquire_model(ref, "cpu")
    h4 = manager.acquire_model(ModelRef("fake", "other"), "cpu")
    result = manager.force_clear()
    assert_eq(result["disposed"], 2)
    assert_eq(provider.disposed, 3)
    h3.release()
    h4.release()


def verify_block_resources_release():
    provider = FakeProvider()
    manager = get_worker_resource_manager()
    manager.force_clear()
    manager.register_provider(provider)

    ref = ModelRef(provider="fake", name="block")
    arr = da.from_array(np.ones((2, 2), dtype=np.uint16), chunks=(1, 2))

    def f_model(block, model, ctx):
        real_model = ctx.resources.resolve_model(model)
        assert_eq(real_model["name"], "block")
        return block

    class ModelNode(BaseBlockMapNode):
        @classmethod
        def INPUT_TYPES(cls):
            return {"required": {"dask_arr": ("DASK_ARRAY[any]",), "model": ("MODEL[fake]",)}}

        RETURN_TYPES = ("DASK_ARRAY[uint16]",)
        PROCESS_BLOCK = f_model

    compute_node(ModelNode(), arr, model=ref)
    assert_eq(manager.stats()["resources"][0]["ref_count"], 0)
    assert_eq(provider.created, 1)

    def f_raises(block, model, ctx):
        ctx.resources.resolve_model(model)
        raise RuntimeError("expected")

    class FailingNode(ModelNode):
        FAILURE_POLICY = "zeros_like"
        PROCESS_BLOCK = f_raises

    compute_node(FailingNode(), arr, model=ref)
    assert_eq(manager.stats()["resources"][0]["ref_count"], 0)
    manager.force_clear()


def verify_model_loader_and_cellpose_specs():
    loader_result = CellposeModelLoader().execute("cpsam")[0]
    assert isinstance(loader_result, ModelRef)
    assert_eq(loader_result.provider, "cellpose")
    assert_eq(loader_result.name, "cpsam")

    inputs = DaskCellpose.INPUT_TYPES()
    assert "model" in inputs.get("optional", {})
    assert_eq(inputs["optional"]["model"][0], "MODEL[cellpose]")
    assert "model_type" in inputs.get("optional", {})
    assert_eq(DaskCellpose.RETURN_TYPES, ("DASK_ARRAY[uint16]",))

    class SourceCellposeModel:
        RETURN_TYPES = ("MODEL[cellpose]",)
        RETURN_NAMES = ("model",)

    class SourcePyTorchModel:
        RETURN_TYPES = ("MODEL[pytorch]",)
        RETURN_NAMES = ("model",)

    NODE_CLASS_MAPPINGS.update({
        "VerifyCellposeModelSource": SourceCellposeModel,
        "VerifyPyTorchModelSource": SourcePyTorchModel,
    })

    validate_graph_types({
        "m": {"type": "VerifyCellposeModelSource", "inputs": {}},
        "c": {"type": "DaskCellpose", "inputs": {"model": ["m", 0]}},
    })
    try:
        validate_graph_types({
            "m": {"type": "VerifyPyTorchModelSource", "inputs": {}},
            "c": {"type": "DaskCellpose", "inputs": {"model": ["m", 0]}},
        })
    except ValueError as exc:
        assert "compatible ModelLoader" in str(exc)
    else:
        raise AssertionError("invalid MODEL provider connection did not fail")


if __name__ == "__main__":
    verify_type_system()
    verify_model_ref_and_resource_manager()
    verify_blockmap_calls_and_dtype()
    verify_block_resources_release()
    verify_type_cast()
    verify_graph_validation()
    verify_model_loader_and_cellpose_specs()
    print("backend blockmap/type-system verification passed")

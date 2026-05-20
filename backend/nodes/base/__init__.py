"""
Base node abstractions for WorkFlow.

BaseMapBlockNode: Unified lifecycle for nodes that process each Dask block independently.
"""

from nodes.base.block_map import (
    BaseBlockMapNode,
    BaseMapBlockNode,
    BlockContext,
    BlockResources,
)

__all__ = [
    "BaseBlockMapNode",
    "BaseMapBlockNode",
    "BlockContext",
    "BlockResources",
]

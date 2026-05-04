"""
Base node abstractions for BrainFlow.

BaseBlockMapNode: Abstract base for nodes that process each Dask block independently.
"""

from nodes.base.block_map import BaseBlockMapNode, BlockContext, BlockResources, SegmentationBlockMapNode

__all__ = ["BaseBlockMapNode", "BlockContext", "BlockResources", "SegmentationBlockMapNode"]

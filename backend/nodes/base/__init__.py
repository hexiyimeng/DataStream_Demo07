"""
Base node abstractions for BrainFlow.

BaseBlockMapNode: Abstract base for nodes that process each Dask block independently.
"""

from nodes.base.block_map import BaseBlockMapNode

__all__ = ["BaseBlockMapNode"]

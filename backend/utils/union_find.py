"""
Simple Union-Find implementation for instance reconciliation.
"""


class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def make_set(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x):
        if x not in self.parent:
            self.make_set(x)
        # Path compression
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px == py:
            return False
        # Union by rank
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1
        return True

    def same(self, x, y):
        return self.find(x) == self.find(y)

    def components(self):
        """Return dict: root -> [members]"""
        comp = {}
        for x in self.parent:
            root = self.find(x)
            comp.setdefault(root, []).append(x)
        return comp

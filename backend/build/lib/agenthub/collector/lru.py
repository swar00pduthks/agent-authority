from __future__ import annotations

from collections import OrderedDict
from collections.abc import Hashable


class LRUSet:
    """Bounded 'have I written this already?' cache.

    A miss only costs an idempotent write (stores upsert by key), so the cache
    is a pure write-reduction optimisation and is safe to lose on restart.
    """

    def __init__(self, capacity: int = 200_000) -> None:
        self.capacity = capacity
        self._d: OrderedDict[Hashable, None] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def seen(self, key: Hashable) -> bool:
        """Return True if key was present; otherwise record it and return False."""
        if key in self._d:
            self._d.move_to_end(key)
            self.hits += 1
            return True
        self.misses += 1
        self._d[key] = None
        if len(self._d) > self.capacity:
            self._d.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._d)

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0

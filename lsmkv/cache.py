"""Thread-safe LRU cache with a byte budget."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Hashable
from typing import Any


class LRUCache:
    def __init__(self, capacity_bytes: int) -> None:
        self.capacity_bytes = capacity_bytes
        self.used_bytes = 0
        self.hits = 0
        self.misses = 0
        self._items: OrderedDict[Hashable, tuple[Any, int]] = OrderedDict()
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._items)

    def get(self, key: Hashable) -> Any:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                self.misses += 1
                return None
            self._items.move_to_end(key)
            self.hits += 1
            return item[0]

    def put(self, key: Hashable, value: Any, charge: int) -> None:
        if charge > self.capacity_bytes:
            return
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self.used_bytes -= old[1]
            self._items[key] = (value, charge)
            self.used_bytes += charge
            while self.used_bytes > self.capacity_bytes:
                _, (_, evicted) = self._items.popitem(last=False)
                self.used_bytes -= evicted

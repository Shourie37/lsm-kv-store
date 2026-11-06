"""Skip list ordered map: the memtable index."""

from __future__ import annotations

import random
from collections.abc import Iterator
from typing import Any

MAX_LEVEL = 16
BRANCH_PROBABILITY = 0.25


class _Node:
    __slots__ = ("key", "value", "forward")

    def __init__(self, key: Any, value: Any, level: int) -> None:
        self.key = key
        self.value = value
        self.forward: list[_Node | None] = [None] * level


class SkipList:
    """Ordered map with expected O(log n) insert and lookup."""

    def __init__(self, seed: int | None = None) -> None:
        self._head = _Node(None, None, MAX_LEVEL)
        self._level = 1
        self._len = 0
        self._rng = random.Random(seed)

    def __len__(self) -> int:
        return self._len

    def _random_level(self) -> int:
        level = 1
        while level < MAX_LEVEL and self._rng.random() < BRANCH_PROBABILITY:
            level += 1
        return level

    def _predecessors(self, key: Any) -> list[_Node]:
        update = [self._head] * MAX_LEVEL
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.forward[i]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.forward[i]
            update[i] = node
        return update

    def _seek(self, key: Any) -> _Node | None:
        """Return the first node whose key is >= key."""
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.forward[i]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.forward[i]
        return node.forward[0]

    def insert(self, key: Any, value: Any) -> bool:
        """Insert or replace. Return True if the key is new."""
        update = self._predecessors(key)
        nxt = update[0].forward[0]
        if nxt is not None and nxt.key == key:
            nxt.value = value
            return False
        level = self._random_level()
        self._level = max(self._level, level)
        node = _Node(key, value, level)
        for i in range(level):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node
        self._len += 1
        return True

    def get(self, key: Any, default: Any = None) -> Any:
        node = self._seek(key)
        if node is not None and node.key == key:
            return node.value
        return default

    def items(self, start: Any = None, end: Any = None) -> Iterator[tuple[Any, Any]]:
        """Yield (key, value) pairs with start <= key < end in key order."""
        node = self._head.forward[0] if start is None else self._seek(start)
        while node is not None and (end is None or node.key < end):
            yield node.key, node.value
            node = node.forward[0]

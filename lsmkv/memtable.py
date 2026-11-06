"""In-memory write buffer backed by a skip list."""

from __future__ import annotations

from collections.abc import Iterator

from .format import ENTRY_HEADER, Entry
from .skiplist import SkipList


class Memtable:
    def __init__(self) -> None:
        self._list = SkipList()
        self.size_bytes = 0

    def __len__(self) -> int:
        return len(self._list)

    def put(self, key: bytes, seq: int, kind: int, value: bytes) -> None:
        self._list.insert(key, (seq, kind, value))
        # Overwrites still add: the estimate tracks bytes written, which bounds memory.
        self.size_bytes += len(key) + len(value) + ENTRY_HEADER.size

    def get(self, key: bytes) -> Entry | None:
        found = self._list.get(key)
        if found is None:
            return None
        return (key, *found)

    def entries(self, start: bytes | None = None, end: bytes | None = None) -> Iterator[Entry]:
        for key, (seq, kind, value) in self._list.items(start, end):
            yield key, seq, kind, value

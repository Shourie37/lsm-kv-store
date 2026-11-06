"""K-way merge over sorted entry streams."""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator

from .format import Entry


def merge_newest(sources: Iterable[Iterable[Entry]]) -> Iterator[Entry]:
    """Merge key-sorted streams and yield only the newest entry per key.

    Tombstones are yielded too; the caller decides whether to hide or drop them.
    """
    heap: list[tuple[bytes, int, int, Entry, Iterator[Entry]]] = []
    for idx, source in enumerate(sources):
        it = iter(source)
        entry = next(it, None)
        if entry is not None:
            heap.append((entry[0], -entry[1], idx, entry, it))
    heapq.heapify(heap)
    last_key: bytes | None = None
    while heap:
        key, _, idx, entry, it = heap[0]
        nxt = next(it, None)
        if nxt is None:
            heapq.heappop(heap)
        else:
            heapq.heapreplace(heap, (nxt[0], -nxt[1], idx, nxt, it))
        if key == last_key:
            continue
        last_key = key
        yield entry

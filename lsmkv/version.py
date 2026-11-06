"""Metadata for live table files and the set of files that form the database."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass


@dataclass(frozen=True)
class FileMeta:
    number: int
    size: int
    smallest: bytes
    largest: bytes
    entries: int
    tombstones: int
    min_seq: int
    max_seq: int

    def overlaps(self, start: bytes | None, end_inclusive: bytes | None) -> bool:
        if end_inclusive is not None and self.smallest > end_inclusive:
            return False
        return start is None or self.largest >= start

    def contains(self, key: bytes) -> bool:
        return self.smallest <= key <= self.largest

    def to_json(self) -> dict:
        return {
            "number": self.number,
            "size": self.size,
            "smallest": self.smallest.hex(),
            "largest": self.largest.hex(),
            "entries": self.entries,
            "tombstones": self.tombstones,
            "min_seq": self.min_seq,
            "max_seq": self.max_seq,
        }

    @classmethod
    def from_json(cls, data: dict) -> FileMeta:
        return cls(
            number=data["number"],
            size=data["size"],
            smallest=bytes.fromhex(data["smallest"]),
            largest=bytes.fromhex(data["largest"]),
            entries=data["entries"],
            tombstones=data["tombstones"],
            min_seq=data["min_seq"],
            max_seq=data["max_seq"],
        )


class Version:
    """An immutable snapshot of which files live at which level."""

    def __init__(self, levels: list[list[FileMeta]]) -> None:
        self.levels = levels
        self._smallest = [[f.smallest for f in level] for level in levels]

    def all_files(self) -> list[FileMeta]:
        return [f for level in self.levels for f in level]

    def find_in_sorted_level(self, level: int, key: bytes) -> FileMeta | None:
        """Return the file covering key in a level whose files do not overlap."""
        i = bisect_right(self._smallest[level], key) - 1
        if i >= 0 and key <= self.levels[level][i].largest:
            return self.levels[level][i]
        return None

    def total_bytes(self) -> int:
        return sum(f.size for f in self.all_files())

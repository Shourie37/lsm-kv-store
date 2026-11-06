"""Compaction strategies: which files to merge and where the output goes."""

from __future__ import annotations

from dataclasses import dataclass

from .options import Options
from .version import FileMeta, Version


@dataclass(frozen=True)
class Compaction:
    inputs: tuple[FileMeta, ...]
    output_level: int
    split_output: bool
    trivial_move: bool = False
    source_level: int = -1


def _without(levels: list[list[FileMeta]], numbers: set[int]) -> list[list[FileMeta]]:
    return [[f for f in level if f.number not in numbers] for level in levels]


class LeveledStrategy:
    """L0 holds overlapping flushed tables. Each level from L1 down holds
    non-overlapping files and is `level_multiplier` times larger than the one
    above. Any entry in level i is newer than any entry for the same key in a
    deeper level, so a point read stops at the first level that has the key.
    """

    name = "leveled"
    levels_ordered = True

    def __init__(self, options: Options) -> None:
        self.o = options
        self._pointer: dict[int, bytes] = {}

    def empty_levels(self) -> list[list[FileMeta]]:
        return [[] for _ in range(self.o.max_levels)]

    def max_bytes(self, level: int) -> int:
        return self.o.level_base_bytes * self.o.level_multiplier ** (level - 1)

    def add_flushed(self, levels: list[list[FileMeta]], meta: FileMeta) -> list[list[FileMeta]]:
        out = [list(level) for level in levels]
        out[0].append(meta)
        return out

    def should_stall(self, version: Version) -> bool:
        return len(version.levels[0]) >= self.o.l0_stop_trigger

    def pick(self, version: Version) -> Compaction | None:
        levels = version.levels
        l0 = levels[0]
        if len(l0) >= self.o.l0_compaction_trigger:
            lo = min(f.smallest for f in l0)
            hi = max(f.largest for f in l0)
            overlap = [f for f in levels[1] if f.overlaps(lo, hi)]
            return Compaction(tuple(l0 + overlap), 1, split_output=True, source_level=0)

        best_level, best_score = -1, 1.0
        for level in range(1, len(levels) - 1):
            score = sum(f.size for f in levels[level]) / self.max_bytes(level)
            if score > best_score:
                best_level, best_score = level, score
        if best_level < 0:
            return None

        files = levels[best_level]
        # Round-robin through the key space so every range gets compacted.
        pointer = self._pointer.get(best_level)
        chosen = next((f for f in files if pointer is None or f.smallest > pointer), files[0])
        overlap = [f for f in levels[best_level + 1] if f.overlaps(chosen.smallest, chosen.largest)]
        return Compaction(
            (chosen, *overlap),
            best_level + 1,
            split_output=True,
            trivial_move=not overlap,
            source_level=best_level,
        )

    def full(self, version: Version) -> Compaction:
        deepest = max((i for i, level in enumerate(version.levels) if level), default=1)
        return Compaction(tuple(version.all_files()), max(deepest, 1), split_output=True)

    def apply(
        self, levels: list[list[FileMeta]], c: Compaction, outputs: list[FileMeta]
    ) -> list[list[FileMeta]]:
        out = _without(levels, {f.number for f in c.inputs})
        out[c.output_level].extend(outputs)
        out[c.output_level].sort(key=lambda f: f.smallest)
        if c.source_level >= 1:
            self._pointer[c.source_level] = c.inputs[0].largest
        return out

    def candidates(self, version: Version, key: bytes) -> list[list[FileMeta]]:
        l0 = sorted(
            (f for f in version.levels[0] if f.contains(key)),
            key=lambda f: f.max_seq,
            reverse=True,
        )
        groups = [l0]
        for level in range(1, len(version.levels)):
            found = version.find_in_sorted_level(level, key)
            if found is not None:
                groups.append([found])
        return groups


class TieredStrategy:
    """Every table is a sorted run. When enough runs have similar sizes, they
    merge into one larger run. Runs overlap, so reads resolve by sequence number.
    """

    name = "tiered"
    levels_ordered = False

    def __init__(self, options: Options) -> None:
        self.o = options

    def empty_levels(self) -> list[list[FileMeta]]:
        return [[]]

    def add_flushed(self, levels: list[list[FileMeta]], meta: FileMeta) -> list[list[FileMeta]]:
        return [[*levels[0], meta]]

    def should_stall(self, version: Version) -> bool:
        return False

    def buckets(self, runs: list[FileMeta]) -> list[list[FileMeta]]:
        buckets: list[list[FileMeta]] = []
        for f in sorted(runs, key=lambda f: f.size):
            if buckets:
                last = buckets[-1]
                avg = sum(g.size for g in last) / len(last)
                small = f.size < self.o.tiered_min_sstable_bytes
                both_small = small and avg < self.o.tiered_min_sstable_bytes
                similar = (
                    self.o.tiered_bucket_low * avg <= f.size <= self.o.tiered_bucket_high * avg
                )
                if both_small or similar:
                    last.append(f)
                    continue
            buckets.append([f])
        return buckets

    def pick(self, version: Version) -> Compaction | None:
        eligible = [
            b for b in self.buckets(version.levels[0]) if len(b) >= self.o.tiered_min_threshold
        ]
        if not eligible:
            return None
        # Merge the smallest runs first: it is cheap and cuts the run count most.
        bucket = min(eligible, key=lambda b: sum(f.size for f in b) / len(b))
        return Compaction(tuple(bucket[: self.o.tiered_max_threshold]), 0, split_output=False)

    def full(self, version: Version) -> Compaction:
        return Compaction(tuple(version.all_files()), 0, split_output=False)

    def apply(
        self, levels: list[list[FileMeta]], c: Compaction, outputs: list[FileMeta]
    ) -> list[list[FileMeta]]:
        out = _without(levels, {f.number for f in c.inputs})
        out[0].extend(outputs)
        return out

    def candidates(self, version: Version, key: bytes) -> list[list[FileMeta]]:
        runs = [f for f in version.levels[0] if f.contains(key)]
        runs.sort(key=lambda f: f.max_seq, reverse=True)
        return [runs]


def make_strategy(options: Options) -> LeveledStrategy | TieredStrategy:
    if options.compaction == "leveled":
        return LeveledStrategy(options)
    return TieredStrategy(options)

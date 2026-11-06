"""Tunable settings for a database."""

from __future__ import annotations

from dataclasses import dataclass

from .wal import SYNC_MODES


@dataclass(frozen=True)
class Options:
    memtable_bytes: int = 1 << 20
    block_size: int = 4096
    bloom_fp_rate: float | None = 0.01
    block_cache_bytes: int = 8 << 20

    wal_sync: str = "batch"
    wal_batch_size: int = 64

    # "leveled" or "tiered" (size-tiered).
    compaction: str = "leveled"
    # False runs flush and compaction inline on the writer thread (deterministic).
    background: bool = True

    target_file_bytes: int = 2 << 20
    l0_compaction_trigger: int = 4
    l0_stop_trigger: int = 12
    level_base_bytes: int = 8 << 20
    level_multiplier: int = 10
    max_levels: int = 7

    tiered_min_threshold: int = 4
    tiered_max_threshold: int = 32
    tiered_bucket_low: float = 0.5
    tiered_bucket_high: float = 1.5
    tiered_min_sstable_bytes: int = 64 << 10

    def __post_init__(self) -> None:
        if self.compaction not in ("leveled", "tiered"):
            raise ValueError("compaction must be 'leveled' or 'tiered'")
        if self.wal_sync not in SYNC_MODES:
            raise ValueError(f"wal_sync must be one of {SYNC_MODES}")
        if self.bloom_fp_rate is not None and not 0 < self.bloom_fp_rate < 1:
            raise ValueError("bloom_fp_rate must be between 0 and 1, or None")
        if self.max_levels < 2:
            raise ValueError("max_levels must be at least 2")
        if self.l0_stop_trigger < self.l0_compaction_trigger:
            raise ValueError("l0_stop_trigger must be >= l0_compaction_trigger")
        if self.tiered_min_threshold < 2:
            raise ValueError("tiered_min_threshold must be at least 2")

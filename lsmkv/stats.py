"""Counters for amplification and cache measurements."""

from dataclasses import dataclass


@dataclass
class WriteStats:
    user_bytes: int = 0
    wal_bytes: int = 0
    flush_bytes: int = 0
    compaction_read_bytes: int = 0
    compaction_write_bytes: int = 0
    flushes: int = 0
    compactions: int = 0
    trivial_moves: int = 0
    tombstones_dropped: int = 0

    @property
    def write_amplification(self) -> float:
        if not self.user_bytes:
            return 0.0
        disk = self.wal_bytes + self.flush_bytes + self.compaction_write_bytes
        return disk / self.user_bytes


@dataclass
class ReadStats:
    gets: int = 0
    tables_probed: int = 0
    bloom_negatives: int = 0
    bloom_false_positives: int = 0
    blocks_read: int = 0
    block_bytes_read: int = 0

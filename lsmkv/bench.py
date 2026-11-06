"""Benchmarks. Run with `python -m lsmkv.bench`; prints markdown tables."""

from __future__ import annotations

import argparse
import dataclasses
import os
import platform
import random
import shutil
import sys
import tempfile
import time
from collections.abc import Callable

from .bloom import BloomFilter, bits_per_key
from .db import DB
from .options import Options

VALUE_SIZE = 100

# Scaled down from the defaults so a Python run builds several levels in seconds.
BASE = Options(
    memtable_bytes=256 << 10,
    target_file_bytes=256 << 10,
    level_base_bytes=1 << 20,
    tiered_min_sstable_bytes=256 << 10,
    block_cache_bytes=0,
    wal_sync="none",
    background=False,
)


def key(i: int) -> bytes:
    return b"user%010d" % i


def table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


class Scratch:
    def __init__(self) -> None:
        self.root = tempfile.mkdtemp(prefix="lsmkv-bench-")
        self._n = 0

    def new(self) -> str:
        self._n += 1
        return os.path.join(self.root, str(self._n))

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


def load(path: str, opts: Options, n: int, seed: int = 1) -> DB:
    rng = random.Random(seed)
    order = list(range(n))
    rng.shuffle(order)
    db = DB(path, opts)
    for i in order:
        db.put(key(i), rng.randbytes(VALUE_SIZE))
    db.flush()
    db.wait_idle()
    return db


def bench_writes(scratch: Scratch, n: int) -> str:
    rows = []
    cases = [
        ("leveled", "none", n),
        ("leveled", "batch", n),
        ("leveled", "always", max(n // 10, 100)),
        ("tiered", "none", n),
    ]
    for strategy, sync, count in cases:
        opts = dataclasses.replace(BASE, compaction=strategy, wal_sync=sync, background=True)
        rng = random.Random(2)
        order = list(range(count))
        rng.shuffle(order)
        values = [rng.randbytes(VALUE_SIZE) for _ in range(1000)]
        db = DB(scratch.new(), opts)
        start = time.perf_counter()
        for j, i in enumerate(order):
            db.put(key(i), values[j % 1000])
        foreground = time.perf_counter() - start
        db.wait_idle()
        total = time.perf_counter() - start
        user_mb = db.write_stats.user_bytes / 1e6
        rows.append(
            [
                strategy,
                sync,
                f"{count:,}",
                f"{count / foreground:,.0f}",
                f"{user_mb / foreground:.2f}",
                f"{total:.2f}",
            ]
        )
        db.close()
    return table(
        ["compaction", "WAL sync", "writes", "writes/s", "MB/s", "s until compaction idle"], rows
    )


def bench_bloom_sizing(n: int) -> str:
    rows = []
    for fp in (0.1, 0.01, 0.001):
        bloom = BloomFilter.for_capacity(n, fp)
        for i in range(n):
            bloom.add(key(i))
        hits = sum(bloom.may_contain(b"absent%010d" % i) for i in range(n))
        rows.append(
            [
                fp,
                f"{bits_per_key(fp):.1f}",
                bloom.num_hashes,
                f"{len(bloom.bits) / 1024:.0f}",
                f"{hits / n:.4f}",
            ]
        )
    return table(
        ["target FP rate", "bits/key", "hashes", f"KiB for {n:,} keys", "measured FP rate"], rows
    )


def timed_lookups(db: DB, keys: list[bytes]) -> tuple[list[float], float, float]:
    stats = db.read_stats
    probes0, blocks0 = stats.tables_probed, stats.blocks_read
    latencies = []
    for k in keys:
        t = time.perf_counter_ns()
        db.get(k)
        latencies.append((time.perf_counter_ns() - t) / 1000)
    return (
        latencies,
        (stats.tables_probed - probes0) / len(keys),
        (stats.blocks_read - blocks0) / len(keys),
    )


def bench_point_reads(scratch: Scratch, n: int, lookups: int) -> str:
    rng = random.Random(3)
    present = [key(rng.randrange(n)) for _ in range(lookups)]
    # Missing keys sort between real keys, so key-range checks cannot skip a table.
    absent = [key(rng.randrange(n)) + b"x" for _ in range(lookups)]
    rows = []
    for strategy in ("leveled", "tiered"):
        for bloom in (0.01, None):
            opts = dataclasses.replace(BASE, compaction=strategy, bloom_fp_rate=bloom)
            db = load(scratch.new(), opts, n)
            files = sum(len(level) for level in db.levels())
            for label, keys in (("present", present), ("missing", absent)):
                lat, probes, blocks = timed_lookups(db, keys)
                rows.append(
                    [
                        strategy,
                        "on (1%)" if bloom else "off",
                        files,
                        label,
                        f"{pct(lat, 0.5):.1f}",
                        f"{pct(lat, 0.99):.1f}",
                        f"{probes:.2f}",
                        f"{blocks:.2f}",
                    ]
                )
            db.close()
    return table(
        [
            "compaction",
            "bloom",
            "tables",
            "keys",
            "p50 µs",
            "p99 µs",
            "tables probed/get",
            "blocks read/get",
        ],
        rows,
    )


def bench_amplification(scratch: Scratch, ops: int, key_space: int) -> str:
    rows = []
    for strategy in ("leveled", "tiered"):
        opts = dataclasses.replace(BASE, compaction=strategy)
        rng = random.Random(4)
        db = DB(scratch.new(), opts)
        start = time.perf_counter()
        for _ in range(ops):
            k = key(rng.randrange(key_space))
            if rng.random() < 0.1:
                db.delete(k)
            else:
                db.put(k, rng.randbytes(VALUE_SIZE))
        db.flush()
        elapsed = time.perf_counter() - start
        ws = db.write_stats
        files = sum(len(level) for level in db.levels())
        disk_before = db.disk_bytes()
        _, probes, blocks = timed_lookups(db, [key(rng.randrange(key_space)) for _ in range(5000)])
        db.compact_all()
        rows.append(
            [
                strategy,
                f"{ws.write_amplification:.1f}",
                f"{(ws.flush_bytes + ws.compaction_write_bytes) / ws.user_bytes:.1f}",
                files,
                f"{probes:.2f}",
                f"{blocks:.2f}",
                f"{disk_before / db.disk_bytes():.2f}",
                f"{elapsed:.1f}",
            ]
        )
        db.close()
    return table(
        [
            "compaction",
            "write amp (incl. WAL)",
            "write amp (tables only)",
            "tables",
            "read amp: tables probed/get",
            "read amp: blocks read/get",
            "space amp",
            "run s",
        ],
        rows,
    )


def bench_recovery(scratch: Scratch, sizes: list[int]) -> str:
    rows = []
    for n in sizes:
        path = scratch.new()
        opts = dataclasses.replace(BASE, memtable_bytes=1 << 34)
        db = DB(path, opts)
        rng = random.Random(5)
        value = rng.randbytes(VALUE_SIZE)
        for i in range(n):
            db.put(key(i), value)
        db.close()  # close does not flush: the next open replays the whole WAL
        wal_mb = sum(os.path.getsize(os.path.join(path, f)) for f in os.listdir(path)) / 1e6
        start = time.perf_counter()
        db = DB(path, opts)
        elapsed = time.perf_counter() - start
        assert db.recovered_records == n
        db.close()
        rows.append([f"{n:,}", f"{wal_mb:.1f}", f"{elapsed:.2f}", f"{wal_mb / elapsed:.1f}"])
    return table(["WAL records", "WAL MB", "recovery s", "MB/s"], rows)


def run(quick: bool = False) -> str:
    scale = 10 if quick else 1
    ops, key_space = 200_000 // scale, 20_000 // scale
    scratch = Scratch()
    sections: list[tuple[str, Callable[[], str]]] = [
        ("Write throughput", lambda: bench_writes(scratch, 50_000 // scale)),
        ("Bloom filter sizing", lambda: bench_bloom_sizing(100_000 // scale)),
        (
            "Point reads (block cache off)",
            lambda: bench_point_reads(scratch, 100_000 // scale, 10_000 // scale),
        ),
        (
            f"Amplification: {ops:,} updates and deletes over {key_space:,} keys",
            lambda: bench_amplification(scratch, ops, key_space),
        ),
        (
            "Recovery time vs WAL size",
            lambda: bench_recovery(
                scratch, [s // scale for s in (25_000, 50_000, 100_000, 200_000)]
            ),
        ),
    ]
    out = [
        f"Python {platform.python_version()} on {platform.system()} {platform.machine()}, "
        f"{VALUE_SIZE}-byte values, 14-byte keys."
    ]
    try:
        for title, fn in sections:
            out.append(f"\n### {title}\n\n{fn()}")
            print(f"done: {title}", file=sys.stderr)
    finally:
        scratch.cleanup()
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="run at 1/10 scale")
    parser.add_argument("--out", help="also write the markdown to this file")
    args = parser.parse_args(argv)
    report = run(args.quick)
    print(report)
    if args.out:
        with open(args.out, "w") as f:
            f.write(report)


if __name__ == "__main__":
    main()

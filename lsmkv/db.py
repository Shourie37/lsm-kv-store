"""The database: write path, read path, recovery, and background work."""

from __future__ import annotations

import contextlib
import fcntl
import os
import threading
from collections.abc import Iterable, Iterator

from .cache import LRUCache
from .compaction import Compaction, make_strategy
from .errors import BackgroundError
from .format import DELETE, PUT, Entry
from .iterators import merge_newest
from .manifest import MANIFEST_TMP, ManifestState, read_manifest, write_manifest
from .memtable import Memtable
from .options import Options
from .sstable import SSTableReader, SSTableWriter
from .stats import ReadStats, WriteStats
from .version import FileMeta, Version
from .wal import WALWriter, fsync_dir, replay

_SST = ".sst"
_WAL = ".log"


def _parse_number(name: str, suffix: str) -> int | None:
    if name.endswith(suffix) and name[: -len(suffix)].isdigit():
        return int(name[: -len(suffix)])
    return None


def _check_bytes(name: str, value: object) -> None:
    if not isinstance(value, bytes):
        raise TypeError(f"{name} must be bytes, not {type(value).__name__}")


class DB:
    """A log-structured merge-tree key-value store over one directory."""

    def __init__(self, path: str | os.PathLike[str], options: Options | None = None) -> None:
        self.path = os.fspath(path)
        self.options = options or Options()
        os.makedirs(self.path, exist_ok=True)
        self._lock_file = open(os.path.join(self.path, "LOCK"), "a")
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock_file.close()
            raise RuntimeError(f"{self.path} is open in another process or handle") from None

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._work_lock = threading.Lock()
        self._strategy = make_strategy(self.options)
        self._cache = LRUCache(self.options.block_cache_bytes)
        self.read_stats = ReadStats()
        self.write_stats = WriteStats()
        self.recovered_records = 0
        self.recovered_torn_tail = False

        self._tables: dict[int, SSTableReader] = {}
        self._mem = Memtable()
        self._imm: Memtable | None = None
        self._closed = False
        self._bg_error: BaseException | None = None
        self._thread: threading.Thread | None = None
        try:
            with self._lock:
                self._recover()
        except BaseException:
            self._release()
            raise
        if self.options.background:
            self._thread = threading.Thread(target=self._bg_loop, name="lsmkv-bg", daemon=True)
            self._thread.start()

    def __enter__(self) -> DB:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- paths and bookkeeping -------------------------------------------------

    def _sst_path(self, number: int) -> str:
        return os.path.join(self.path, f"{number:06d}{_SST}")

    def _wal_path(self, number: int) -> str:
        return os.path.join(self.path, f"{number:06d}{_WAL}")

    def _new_file_number(self) -> int:
        with self._lock:
            number = self._next_file
            self._next_file += 1
            return number

    def _exclusive(self) -> contextlib.AbstractContextManager[object]:
        # Sync mode does all work under the main lock; background mode serializes
        # flushes and compactions with a separate lock so readers keep running.
        return self._work_lock if self.options.background else self._lock

    # ---- recovery ----------------------------------------------------------------

    def _recover(self) -> None:
        state = read_manifest(self.path)
        if state is None:
            state = ManifestState(self._strategy.name, 1, 0, 0, self._strategy.empty_levels())
        elif state.strategy != self._strategy.name:
            raise ValueError(
                f"database uses {state.strategy!r} compaction, options ask for "
                f"{self._strategy.name!r}"
            )
        self._next_file = state.next_file_number
        self._seq = state.last_seq
        self._log_number = state.log_number
        self._version = Version(state.levels)

        live = {f.number for f in self._version.all_files()}
        wal_numbers = []
        for name in os.listdir(self.path):
            if name == MANIFEST_TMP:
                os.unlink(os.path.join(self.path, name))
                continue
            sst = _parse_number(name, _SST)
            wal = _parse_number(name, _WAL)
            number = sst if sst is not None else wal
            if number is None:
                continue
            self._next_file = max(self._next_file, number + 1)
            if sst is not None and sst not in live:
                os.unlink(os.path.join(self.path, name))  # left by a crash before install
            elif wal is not None:
                wal_numbers.append(wal)

        for f in self._version.all_files():
            self._tables[f.number] = self._open_reader(f.number)
            self._seq = max(self._seq, f.max_seq)

        recovered = Memtable()
        for number in sorted(wal_numbers):
            if number < self._log_number:
                continue
            result = replay(self._wal_path(number))
            self.recovered_torn_tail |= result.torn_tail
            for key, seq, kind, value in result.entries:
                recovered.put(key, seq, kind, value)
                self._seq = max(self._seq, seq)
            self.recovered_records += len(result.entries)

        self._open_new_wal()
        if len(recovered):
            self._imm = recovered
            self._flush_imm()
        else:
            self._log_number = self._wal_number
            self._install(self._version.levels, [], [])
            self._delete_obsolete_wals()

    def _open_reader(self, number: int) -> SSTableReader:
        return SSTableReader(self._sst_path(number), number, self._cache, self.read_stats)

    def _open_new_wal(self) -> None:
        self._wal_number = self._new_file_number()
        self._wal = WALWriter(
            self._wal_path(self._wal_number), self.options.wal_sync, self.options.wal_batch_size
        )
        fsync_dir(self.path)

    def _delete_obsolete_wals(self) -> None:
        for name in os.listdir(self.path):
            number = _parse_number(name, _WAL)
            if number is not None and number < self._log_number:
                os.unlink(os.path.join(self.path, name))

    # ---- install a new version ---------------------------------------------------

    def _install(
        self,
        levels: list[list[FileMeta]],
        added: list[tuple[FileMeta, SSTableReader]],
        removed: Iterable[FileMeta],
    ) -> None:
        """Make a new set of live files durable, then visible. Lock must be held."""
        write_manifest(
            self.path,
            ManifestState(
                self._strategy.name, self._next_file, self._seq, self._log_number, levels
            ),
        )
        for meta, reader in added:
            self._tables[meta.number] = reader
        self._version = Version(levels)
        for meta in removed:
            # Open readers keep their descriptor, so in-flight scans still work.
            self._tables.pop(meta.number, None)
            os.unlink(self._sst_path(meta.number))
        self._cv.notify_all()

    # ---- write path --------------------------------------------------------------

    def put(self, key: bytes, value: bytes) -> None:
        _check_bytes("key", key)
        _check_bytes("value", value)
        self._write(key, PUT, value)

    def delete(self, key: bytes) -> None:
        _check_bytes("key", key)
        self._write(key, DELETE, b"")

    def _write(self, key: bytes, kind: int, value: bytes) -> None:
        with self._lock:
            self._check_open()
            self._make_room()
            self._seq += 1
            self.write_stats.wal_bytes += self._wal.append(self._seq, kind, key, value)
            self._mem.put(key, self._seq, kind, value)
            self.write_stats.user_bytes += len(key) + len(value)

    def _make_room(self) -> None:
        while self._mem.size_bytes >= self.options.memtable_bytes:
            if not self.options.background:
                self._rotate()
                self._flush_imm()
                self._compact_pending()
                return
            if self._imm is not None or self._strategy.should_stall(self._version):
                self._cv.wait()  # write stall: background work must catch up
                self._check_open()
                continue
            self._rotate()
            self._cv.notify_all()
            return

    def _rotate(self) -> None:
        """Freeze the memtable and start a new WAL. Lock must be held."""
        self._wal.close()
        self._imm = self._mem
        self._mem = Memtable()
        self._open_new_wal()

    def sync(self) -> None:
        """Force the WAL to stable storage."""
        with self._lock:
            self._check_open()
            self._wal.sync()

    # ---- read path ---------------------------------------------------------------

    def get(self, key: bytes) -> bytes | None:
        _check_bytes("key", key)
        with self._lock:
            self._check_open()
            self.read_stats.gets += 1
            for mem in (self._mem, self._imm):
                if mem is not None:
                    entry = mem.get(key)
                    if entry is not None:
                        return None if entry[2] == DELETE else entry[3]
            groups = [
                [(f, self._tables[f.number]) for f in group]
                for group in self._strategy.candidates(self._version, key)
            ]
        best: Entry | None = None
        for group in groups:
            for meta, reader in group:
                if best is not None and best[1] >= meta.max_seq:
                    continue  # this table holds nothing newer than what we found
                entry = reader.get(key)
                if entry is not None and (best is None or entry[1] > best[1]):
                    best = entry
            if best is not None and self._strategy.levels_ordered:
                break
        if best is None or best[2] == DELETE:
            return None
        return best[3]

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes]]:
        """Yield live (key, value) pairs with start <= key < end, in key order.

        The set of sources is fixed when scan() is called.
        """
        with self._lock:
            self._check_open()
            sources: list[Iterable[Entry]] = [list(self._mem.entries(start, end))]
            if self._imm is not None:
                sources.append(list(self._imm.entries(start, end)))
            for f in self._version.all_files():
                if end is not None and f.smallest >= end:
                    continue
                if start is not None and f.largest < start:
                    continue
                sources.append(self._tables[f.number].iter_range(start, end))
        return ((key, value) for key, _, kind, value in merge_newest(sources) if kind == PUT)

    # ---- flush and compaction ----------------------------------------------------

    def _build_tables(
        self, entries: Iterable[Entry], split: bool
    ) -> list[tuple[FileMeta, SSTableReader]]:
        built: list[tuple[FileMeta, SSTableReader]] = []
        writer: SSTableWriter | None = None
        number = 0
        try:
            for key, seq, kind, value in entries:
                if writer is None:
                    number = self._new_file_number()
                    writer = SSTableWriter(
                        self._sst_path(number), self.options.block_size, self.options.bloom_fp_rate
                    )
                writer.add(key, seq, kind, value)
                if split and writer.estimated_size >= self.options.target_file_bytes:
                    built.append(self._finish_table(writer, number))
                    writer = None
            if writer is not None:
                built.append(self._finish_table(writer, number))
        except BaseException:
            if writer is not None:
                writer.abort()
            for meta, reader in built:
                reader.close()
                os.unlink(self._sst_path(meta.number))
            raise
        return built

    def _finish_table(self, writer: SSTableWriter, number: int) -> tuple[FileMeta, SSTableReader]:
        info = writer.finish()
        meta = FileMeta(
            number,
            info.size,
            info.smallest,
            info.largest,
            info.entries,
            info.tombstones,
            info.min_seq,
            info.max_seq,
        )
        return meta, self._open_reader(number)

    def _flush_imm(self) -> None:
        with self._lock:
            imm = self._imm
        if imm is None:
            return
        built = self._build_tables(imm.entries(), split=False)
        with self._lock:
            levels = self._version.levels
            for meta, _ in built:
                levels = self._strategy.add_flushed(levels, meta)
                self.write_stats.flush_bytes += meta.size
            self.write_stats.flushes += 1
            # Every WAL older than the active one now lives in a table.
            self._log_number = self._wal_number
            self._install(levels, built, [])
            self._imm = None
            self._delete_obsolete_wals()

    def _compact_pending(self) -> None:
        while (c := self._strategy.pick(self._version)) is not None:
            self._run_compaction(c)

    def _droppable_tombstones(
        self, entries: Iterable[Entry], others: list[FileMeta]
    ) -> Iterator[Entry]:
        for entry in entries:
            if entry[2] == DELETE and not any(
                f.contains(entry[0]) and f.min_seq < entry[1] for f in others
            ):
                # No older version can exist outside this compaction.
                self.write_stats.tombstones_dropped += 1
                continue
            yield entry

    def _run_compaction(self, c: Compaction) -> None:
        if c.trivial_move:
            with self._lock:
                levels = self._strategy.apply(self._version.levels, c, list(c.inputs))
                self._install(levels, [], [])
                self.write_stats.trivial_moves += 1
            return
        with self._lock:
            readers = [self._tables[f.number] for f in c.inputs]
            numbers = {f.number for f in c.inputs}
            others = [f for f in self._version.all_files() if f.number not in numbers]
        merged = merge_newest(r.iter_range(fill_cache=False) for r in readers)
        built = self._build_tables(self._droppable_tombstones(merged, others), c.split_output)
        with self._lock:
            levels = self._strategy.apply(self._version.levels, c, [m for m, _ in built])
            self.write_stats.compactions += 1
            self.write_stats.compaction_read_bytes += sum(f.size for f in c.inputs)
            self.write_stats.compaction_write_bytes += sum(m.size for m, _ in built)
            self._install(levels, built, c.inputs)

    def _has_work(self) -> bool:
        return self._imm is not None or self._strategy.pick(self._version) is not None

    def _bg_loop(self) -> None:
        while True:
            with self._lock:
                while not self._closed and not self._has_work():
                    self._cv.wait()
                if self._closed:
                    return
            try:
                with self._work_lock:
                    with self._lock:
                        flush = self._imm is not None
                        c = None if flush else self._strategy.pick(self._version)
                    if flush:
                        self._flush_imm()
                    elif c is not None:
                        self._run_compaction(c)
            except BaseException as exc:
                with self._lock:
                    self._bg_error = exc
                    self._cv.notify_all()
                return

    def _flush_all(self) -> None:
        self._flush_imm()
        with self._lock:
            if not len(self._mem):
                return
            self._rotate()
        self._flush_imm()

    def flush(self) -> None:
        """Write the memtable to a table file now."""
        with self._exclusive():
            self._check_open()
            self._flush_all()

    def compact_all(self) -> None:
        """Flush, then merge every table into one sorted run (a major compaction)."""
        with self._exclusive():
            self._check_open()
            self._flush_all()
            with self._lock:
                if not self._version.all_files():
                    return
                c = self._strategy.full(self._version)
            self._run_compaction(c)

    def wait_idle(self) -> None:
        """Block until no flush or compaction is pending."""
        with self._lock:
            while self.options.background and not self._closed and self._has_work():
                self._check_open()
                self._cv.wait()
            self._check_open()

    # ---- lifecycle and introspection -----------------------------------------------

    def _check_open(self) -> None:
        if self._bg_error is not None:
            raise BackgroundError("background work failed") from self._bg_error
        if self._closed:
            raise RuntimeError("database is closed")

    def close(self) -> None:
        """Stop background work and sync the WAL. The memtable is not flushed;
        the next open replays it from the WAL."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cv.notify_all()
        if self._thread is not None:
            self._thread.join()
        with self._lock:
            self._wal.close()
            for reader in self._tables.values():
                reader.close()
            self._tables.clear()
        self._release()

    def _release(self) -> None:
        fcntl.flock(self._lock_file, fcntl.LOCK_UN)
        self._lock_file.close()

    def levels(self) -> list[list[FileMeta]]:
        with self._lock:
            return [list(level) for level in self._version.levels]

    def disk_bytes(self) -> int:
        """Bytes in live table files (the WAL is not counted)."""
        with self._lock:
            return self._version.total_bytes()

    @property
    def cache(self) -> LRUCache:
        return self._cache

"""Immutable sorted table files.

File layout:
    data block 0 | data block 1 | ... | index block | bloom block | meta block | footer

Every block ends with a crc32 of its bytes. A data block holds entries in key
order. The index is sparse: one (first key, offset, size) per data block. The
footer has a fixed size, so a reader finds everything from the end of the file.
"""

from __future__ import annotations

import os
import struct
import zlib
from bisect import bisect_left, bisect_right
from collections.abc import Iterator
from dataclasses import dataclass

from .bloom import BloomFilter
from .cache import LRUCache
from .errors import CorruptionError
from .format import DELETE, ENTRY_HEADER, Entry
from .stats import ReadStats

MAGIC = 0x4C534D4B56535354  # "LSMKVSST"
_CRC = struct.Struct("<I")
_INDEX_ENTRY = struct.Struct("<IQI")  # key_len, offset, size
_META = struct.Struct("<QQQQI")  # entries, tombstones, min_seq, max_seq, largest_len
_FOOTER_BODY = struct.Struct("<QIQIQIQ")
FOOTER_SIZE = _FOOTER_BODY.size + _CRC.size


def _with_crc(payload: bytes) -> bytes:
    return payload + _CRC.pack(zlib.crc32(payload))


def _strip_crc(block: bytes, what: str, path: str) -> bytes:
    if len(block) < _CRC.size:
        raise CorruptionError(f"{path}: truncated {what}")
    payload = block[: -_CRC.size]
    (crc,) = _CRC.unpack_from(block, len(block) - _CRC.size)
    if zlib.crc32(payload) != crc:
        raise CorruptionError(f"{path}: checksum mismatch in {what}")
    return payload


@dataclass(frozen=True)
class TableInfo:
    size: int
    smallest: bytes
    largest: bytes
    entries: int
    tombstones: int
    min_seq: int
    max_seq: int


class SSTableWriter:
    """Build a table from entries added in strictly increasing key order."""

    def __init__(self, path: str, block_size: int = 4096, bloom_fp_rate: float | None = 0.01):
        self.path = path
        self._block_size = block_size
        self._bloom_fp_rate = bloom_fp_rate
        self._file = open(path, "wb")
        self._offset = 0
        self._block = bytearray()
        self._block_first: bytes | None = None
        self._index: list[tuple[bytes, int, int]] = []
        self._keys: list[bytes] = []
        self._tombstones = 0
        self._min_seq = 2**64 - 1
        self._max_seq = 0

    @property
    def estimated_size(self) -> int:
        return self._offset + len(self._block)

    def add(self, key: bytes, seq: int, kind: int, value: bytes) -> None:
        if self._keys and key <= self._keys[-1]:
            raise ValueError("keys must be added in strictly increasing order")
        if not self._block:
            self._block_first = key
        self._block += ENTRY_HEADER.pack(len(key), len(value), seq, kind)
        self._block += key
        self._block += value
        self._keys.append(key)
        self._tombstones += kind == DELETE
        self._min_seq = min(self._min_seq, seq)
        self._max_seq = max(self._max_seq, seq)
        if len(self._block) >= self._block_size:
            self._flush_block()

    def _write(self, data: bytes) -> int:
        offset = self._offset
        self._file.write(data)
        self._offset += len(data)
        return offset

    def _flush_block(self) -> None:
        data = _with_crc(bytes(self._block))
        assert self._block_first is not None
        self._index.append((self._block_first, self._write(data), len(data)))
        self._block.clear()

    def finish(self) -> TableInfo:
        if not self._keys:
            raise ValueError("cannot finish an empty table")
        if self._block:
            self._flush_block()
        index = bytearray()
        for first, offset, size in self._index:
            index += _INDEX_ENTRY.pack(len(first), offset, size) + first
        index_data = _with_crc(bytes(index))
        index_off = self._write(index_data)

        bloom_data = b""
        if self._bloom_fp_rate is not None:
            bloom = BloomFilter.for_capacity(len(self._keys), self._bloom_fp_rate)
            for key in self._keys:
                bloom.add(key)
            bloom_data = _with_crc(bloom.to_bytes())
        bloom_off = self._write(bloom_data)

        largest = self._keys[-1]
        meta = _META.pack(
            len(self._keys), self._tombstones, self._min_seq, self._max_seq, len(largest)
        )
        meta_data = _with_crc(meta + largest)
        meta_off = self._write(meta_data)

        footer = _FOOTER_BODY.pack(
            index_off,
            len(index_data),
            bloom_off,
            len(bloom_data),
            meta_off,
            len(meta_data),
            MAGIC,
        )
        self._write(_with_crc(footer))
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        return TableInfo(
            size=self._offset,
            smallest=self._keys[0],
            largest=largest,
            entries=len(self._keys),
            tombstones=self._tombstones,
            min_seq=self._min_seq,
            max_seq=self._max_seq,
        )

    def abort(self) -> None:
        self._file.close()
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass


def _parse_block(data: bytes) -> tuple[list[bytes], list[Entry]]:
    keys: list[bytes] = []
    entries: list[Entry] = []
    pos = 0
    header = ENTRY_HEADER
    size = header.size
    while pos < len(data):
        key_len, value_len, seq, kind = header.unpack_from(data, pos)
        pos += size
        key = data[pos : pos + key_len]
        pos += key_len
        value = data[pos : pos + value_len]
        pos += value_len
        keys.append(key)
        entries.append((key, seq, kind, value))
    return keys, entries


class SSTableReader:
    """Random and sequential access to one table.

    Uses pread on a private descriptor, so concurrent reads are safe. The
    descriptor stays open after the file is unlinked, which lets an in-flight
    scan finish after compaction removes the table.
    """

    def __init__(
        self,
        path: str,
        number: int,
        cache: LRUCache | None = None,
        stats: ReadStats | None = None,
    ) -> None:
        self.path = path
        self.number = number
        self._cache = cache
        self._stats = stats or ReadStats()
        self._fd = -1
        self._fd = os.open(path, os.O_RDONLY)
        try:
            self._load_metadata()
        except BaseException:
            self.close()
            raise

    def _pread(self, size: int, offset: int) -> bytes:
        data = os.pread(self._fd, size, offset)
        if len(data) != size:
            raise CorruptionError(f"{self.path}: short read at offset {offset}")
        return data

    def _load_metadata(self) -> None:
        file_size = os.fstat(self._fd).st_size
        if file_size < FOOTER_SIZE:
            raise CorruptionError(f"{self.path}: file too small for footer")
        footer = _strip_crc(self._pread(FOOTER_SIZE, file_size - FOOTER_SIZE), "footer", self.path)
        index_off, index_len, bloom_off, bloom_len, meta_off, meta_len, magic = _FOOTER_BODY.unpack(
            footer
        )
        if magic != MAGIC:
            raise CorruptionError(f"{self.path}: bad magic number")
        self.file_size = file_size

        index = _strip_crc(self._pread(index_len, index_off), "index block", self.path)
        self._first_keys: list[bytes] = []
        self._handles: list[tuple[int, int]] = []
        pos = 0
        while pos < len(index):
            key_len, offset, size = _INDEX_ENTRY.unpack_from(index, pos)
            pos += _INDEX_ENTRY.size
            self._first_keys.append(index[pos : pos + key_len])
            pos += key_len
            self._handles.append((offset, size))

        self.bloom: BloomFilter | None = None
        if bloom_len:
            raw = _strip_crc(self._pread(bloom_len, bloom_off), "bloom block", self.path)
            self.bloom = BloomFilter.from_bytes(raw)

        meta = _strip_crc(self._pread(meta_len, meta_off), "meta block", self.path)
        self.entries, self.tombstones, self.min_seq, self.max_seq, largest_len = _META.unpack_from(
            meta
        )
        self.largest = meta[_META.size : _META.size + largest_len]
        self.smallest = self._first_keys[0]

    @property
    def num_blocks(self) -> int:
        return len(self._handles)

    def _load_block(self, i: int, fill_cache: bool = True) -> tuple[list[bytes], list[Entry]]:
        cache_key = (self.number, i)
        if self._cache is not None:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        offset, size = self._handles[i]
        raw = self._pread(size, offset)
        self._stats.blocks_read += 1
        self._stats.block_bytes_read += size
        parsed = _parse_block(_strip_crc(raw, f"data block {i}", self.path))
        if fill_cache and self._cache is not None:
            self._cache.put(cache_key, parsed, size)
        return parsed

    def get(self, key: bytes) -> Entry | None:
        """Return the entry for key (which may be a tombstone), or None."""
        self._stats.tables_probed += 1
        if key < self.smallest or key > self.largest:
            return None
        if self.bloom is not None and not self.bloom.may_contain(key):
            self._stats.bloom_negatives += 1
            return None
        keys, entries = self._load_block(bisect_right(self._first_keys, key) - 1)
        j = bisect_left(keys, key)
        if j < len(keys) and keys[j] == key:
            return entries[j]
        if self.bloom is not None:
            self._stats.bloom_false_positives += 1
        return None

    def iter_range(
        self, start: bytes | None = None, end: bytes | None = None, fill_cache: bool = True
    ) -> Iterator[Entry]:
        """Yield entries with start <= key < end in key order."""
        first = 0 if start is None else max(0, bisect_right(self._first_keys, start) - 1)
        for i in range(first, len(self._handles)):
            if end is not None and self._first_keys[i] >= end:
                return
            keys, entries = self._load_block(i, fill_cache)
            j = 0 if start is None else bisect_left(keys, start)
            for k in range(j, len(entries)):
                entry = entries[k]
                if end is not None and entry[0] >= end:
                    return
                yield entry
            start = None

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def __del__(self) -> None:
        self.close()

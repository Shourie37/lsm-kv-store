"""Write-ahead log: one checksummed record per write.

Record layout (little endian):
    length u32 | crc32(length) u32 | crc32(payload) u32 | payload
    payload = seq u64 | kind u8 | key_len u32 | key | value

The length has its own checksum so recovery can tell a torn tail (the file
ends inside a record) from corruption (a complete record with bad bytes).
"""

from __future__ import annotations

import os
import struct
import zlib
from dataclasses import dataclass, field

from .errors import CorruptionError
from .format import Entry

_LENGTH = struct.Struct("<I")
_CRCS = struct.Struct("<II")
_HEADER_SIZE = _LENGTH.size + _CRCS.size
_BODY = struct.Struct("<QBI")

SYNC_MODES = ("always", "batch", "none")


def fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class WALWriter:
    """Append-only log writer.

    sync="always" fsyncs every record, "batch" fsyncs every `batch_size`
    records, and "none" leaves flushing to the OS. Every mode issues one
    write() per record, so a process crash loses nothing that was acknowledged;
    only "always" also survives power loss.
    """

    def __init__(self, path: str, sync: str = "batch", batch_size: int = 64) -> None:
        if sync not in SYNC_MODES:
            raise ValueError(f"sync must be one of {SYNC_MODES}")
        self.path = path
        self._sync = sync
        self._batch_size = batch_size
        self._unsynced = 0
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    def append(self, seq: int, kind: int, key: bytes, value: bytes) -> int:
        """Write one record. Return the number of bytes written."""
        payload = _BODY.pack(seq, kind, len(key)) + key + value
        length = _LENGTH.pack(len(payload))
        record = length + _CRCS.pack(zlib.crc32(length), zlib.crc32(payload)) + payload
        view = memoryview(record)
        while view:
            view = view[os.write(self._fd, view) :]
        if self._sync == "always":
            os.fsync(self._fd)
        elif self._sync == "batch":
            self._unsynced += 1
            if self._unsynced >= self._batch_size:
                self.sync()
        return len(record)

    def sync(self) -> None:
        os.fsync(self._fd)
        self._unsynced = 0

    def close(self) -> None:
        if self._fd >= 0:
            self.sync()
            os.close(self._fd)
            self._fd = -1


@dataclass
class ReplayResult:
    entries: list[Entry] = field(default_factory=list)
    valid_bytes: int = 0
    torn_tail: bool = False


def replay(path: str) -> ReplayResult:
    """Read every complete record. A torn tail is dropped; corruption raises."""
    with open(path, "rb") as f:
        data = f.read()
    result = ReplayResult()
    pos = 0
    while pos < len(data):
        if len(data) - pos < _HEADER_SIZE:
            result.torn_tail = True
            break
        (length,) = _LENGTH.unpack_from(data, pos)
        length_crc, payload_crc = _CRCS.unpack_from(data, pos + _LENGTH.size)
        if zlib.crc32(data[pos : pos + _LENGTH.size]) != length_crc:
            raise CorruptionError(f"{path}: bad record header at offset {pos}")
        end = pos + _HEADER_SIZE + length
        if end > len(data):
            result.torn_tail = True
            break
        payload = data[pos + _HEADER_SIZE : end]
        if zlib.crc32(payload) != payload_crc:
            raise CorruptionError(f"{path}: checksum mismatch in record at offset {pos}")
        seq, kind, key_len = _BODY.unpack_from(payload)
        key = payload[_BODY.size : _BODY.size + key_len]
        value = payload[_BODY.size + key_len :]
        result.entries.append((key, seq, kind, value))
        pos = end
        result.valid_bytes = pos
    return result

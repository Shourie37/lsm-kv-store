"""Manifest: the durable list of live files.

The whole state is rewritten to MANIFEST.tmp, fsynced, and renamed over
MANIFEST. A rename is atomic on POSIX, so a reader sees either the old state
or the new state, never a mix. The first line holds a crc32 of the body.
"""

from __future__ import annotations

import json
import os
import zlib
from dataclasses import dataclass

from .errors import CorruptionError
from .version import FileMeta
from .wal import fsync_dir

MANIFEST = "MANIFEST"
MANIFEST_TMP = "MANIFEST.tmp"


@dataclass
class ManifestState:
    strategy: str
    next_file_number: int
    last_seq: int
    log_number: int
    levels: list[list[FileMeta]]

    def to_json(self) -> dict:
        return {
            "strategy": self.strategy,
            "next_file_number": self.next_file_number,
            "last_seq": self.last_seq,
            "log_number": self.log_number,
            "levels": [[f.to_json() for f in level] for level in self.levels],
        }

    @classmethod
    def from_json(cls, data: dict) -> ManifestState:
        return cls(
            strategy=data["strategy"],
            next_file_number=data["next_file_number"],
            last_seq=data["last_seq"],
            log_number=data["log_number"],
            levels=[[FileMeta.from_json(f) for f in level] for level in data["levels"]],
        )


def write_manifest(directory: str, state: ManifestState) -> None:
    body = json.dumps(state.to_json(), sort_keys=True, separators=(",", ":")).encode()
    data = f"{zlib.crc32(body):08x}\n".encode() + body
    tmp = os.path.join(directory, MANIFEST_TMP)
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(directory, MANIFEST))
    fsync_dir(directory)


def read_manifest(directory: str) -> ManifestState | None:
    path = os.path.join(directory, MANIFEST)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return None
    header, _, body = data.partition(b"\n")
    try:
        valid = int(header, 16) == zlib.crc32(body)
    except ValueError:
        valid = False
    if not valid:
        raise CorruptionError(f"{path}: checksum mismatch")
    try:
        return ManifestState.from_json(json.loads(body))
    except (ValueError, KeyError, TypeError) as exc:
        raise CorruptionError(f"{path}: invalid content") from exc

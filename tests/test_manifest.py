import os

import pytest

from lsmkv import DB, CorruptionError
from lsmkv.manifest import MANIFEST, MANIFEST_TMP, ManifestState, read_manifest, write_manifest
from lsmkv.version import FileMeta

from .conftest import small


def state(n: int) -> ManifestState:
    f = FileMeta(n, 10, b"a", b"b", 1, 0, 1, 1)
    return ManifestState("leveled", n + 1, n, n, [[f], []])


def test_crash_before_rename_keeps_old_manifest(tmp_path, monkeypatch):
    write_manifest(str(tmp_path), state(1))

    def crash(src, dst):
        raise OSError("simulated crash")

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(OSError):
        write_manifest(str(tmp_path), state(2))
    monkeypatch.undo()
    assert (tmp_path / MANIFEST_TMP).exists()
    assert read_manifest(str(tmp_path)) == state(1)


def test_corrupt_manifest_is_detected(tmp_path):
    write_manifest(str(tmp_path), state(1))
    data = bytearray((tmp_path / MANIFEST).read_bytes())
    data[-5] ^= 0x01
    (tmp_path / MANIFEST).write_bytes(bytes(data))
    with pytest.raises(CorruptionError):
        read_manifest(str(tmp_path))


def test_crash_between_table_write_and_install_loses_nothing(tmp_path, monkeypatch):
    db = DB(tmp_path, small(memtable_bytes=1 << 20))
    for i in range(300):
        db.put(b"k%03d" % i, b"v%d" % i)
    import lsmkv.db as dbmod

    real = dbmod.write_manifest

    def crash(*args):
        raise OSError("simulated crash")

    monkeypatch.setattr(dbmod, "write_manifest", crash)
    with pytest.raises(OSError):
        db.flush()  # the table file is written, the manifest is not
    monkeypatch.setattr(dbmod, "write_manifest", real)
    orphans = [n for n in os.listdir(tmp_path) if n.endswith(".sst")]
    assert orphans
    db._release()  # drop the process lock without a clean close

    db = DB(tmp_path, small(memtable_bytes=1 << 20))
    assert db.recovered_records == 300
    assert db.get(b"k123") == b"v123"
    live = {f"{f.number:06d}.sst" for level in db.levels() for f in level}
    assert set(orphans).isdisjoint(live)
    assert not any(n in os.listdir(tmp_path) for n in orphans)
    db.close()


def test_leftover_tmp_file_is_ignored_and_removed(tmp_path):
    db = DB(tmp_path, small())
    db.put(b"a", b"1")
    db.close()
    (tmp_path / MANIFEST_TMP).write_bytes(b"garbage from an interrupted write")
    db = DB(tmp_path, small())
    assert db.get(b"a") == b"1"
    assert not (tmp_path / MANIFEST_TMP).exists()
    db.close()


def test_strategy_mismatch_is_refused(tmp_path):
    DB(tmp_path, small(compaction="leveled")).close()
    with pytest.raises(ValueError, match="leveled"):
        DB(tmp_path, small(compaction="tiered"))

import os

import pytest

from lsmkv.errors import CorruptionError
from lsmkv.format import DELETE, PUT
from lsmkv.wal import WALWriter, replay


def write_records(path, n):
    wal = WALWriter(str(path), sync="none")
    sizes = []
    for i in range(n):
        kind = DELETE if i % 3 == 0 else PUT
        sizes.append(wal.append(i + 1, kind, b"k%d" % i, b"" if kind == DELETE else b"v" * i))
    wal.close()
    return sizes


def test_round_trip(tmp_path):
    path = tmp_path / "w.log"
    write_records(path, 50)
    result = replay(str(path))
    assert len(result.entries) == 50 and not result.torn_tail
    assert result.entries[4] == (b"k4", 5, PUT, b"vvvv")
    assert result.entries[3] == (b"k3", 4, DELETE, b"")


def test_every_truncation_point_inside_last_record_is_a_torn_tail(tmp_path):
    path = tmp_path / "w.log"
    sizes = write_records(path, 10)
    full = path.read_bytes()
    start_of_last = sum(sizes[:-1])
    for cut in range(start_of_last + 1, len(full)):
        path.write_bytes(full[:cut])
        result = replay(str(path))
        assert result.torn_tail
        assert len(result.entries) == 9
        assert result.valid_bytes == start_of_last


@pytest.mark.parametrize("offset_in_record", [0, 13, 20])  # length, payload header, value
def test_flipped_byte_in_a_complete_record_raises(tmp_path, offset_in_record):
    path = tmp_path / "w.log"
    sizes = write_records(path, 10)
    data = bytearray(path.read_bytes())
    data[sum(sizes[:5]) + offset_in_record] ^= 0x40
    path.write_bytes(bytes(data))
    with pytest.raises(CorruptionError, match="offset"):
        replay(str(path))


def test_batch_mode_fsyncs_every_n_records(tmp_path, monkeypatch):
    calls = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (calls.append(fd), real_fsync(fd)))
    wal = WALWriter(str(tmp_path / "w.log"), sync="batch", batch_size=4)
    for i in range(10):
        wal.append(i, PUT, b"k", b"v")
    assert len(calls) == 2
    wal.close()

import os
import signal
import subprocess
import sys
import textwrap

import pytest

from lsmkv import DB, CorruptionError

from .conftest import small


def wal_files(path):
    return sorted(n for n in os.listdir(path) if n.endswith(".log"))


def write_unflushed(path, n):
    db = DB(path, small(memtable_bytes=1 << 30))
    for i in range(n):
        db.put(b"k%04d" % i, b"value-%d" % i)
    db.close()  # close does not flush, so all data lives only in the WAL
    (wal,) = wal_files(path)
    return os.path.join(path, wal)


def test_reopen_replays_wal(tmp_path):
    write_unflushed(tmp_path, 500)
    db = DB(tmp_path, small())
    assert db.recovered_records == 500
    assert db.get(b"k0499") == b"value-499"
    db.close()


def test_truncated_wal_recovers_every_complete_record(tmp_path):
    wal = write_unflushed(tmp_path, 500)
    size = os.path.getsize(wal)
    with open(wal, "r+b") as f:
        f.truncate(size - 7)  # cut into the last record
    db = DB(tmp_path, small())
    assert db.recovered_torn_tail
    assert db.recovered_records == 499
    assert db.get(b"k0498") == b"value-498"
    assert db.get(b"k0499") is None
    db.put(b"after", b"crash")
    db.close()
    db = DB(tmp_path, small())
    assert db.get(b"after") == b"crash" and db.get(b"k0498") == b"value-498"
    db.close()


def test_corrupt_wal_checksum_is_detected(tmp_path):
    wal = write_unflushed(tmp_path, 500)
    with open(wal, "r+b") as f:
        f.seek(os.path.getsize(wal) // 2)
        byte = f.read(1)
        f.seek(-1, os.SEEK_CUR)
        f.write(bytes([byte[0] ^ 0xFF]))
    with pytest.raises(CorruptionError):
        DB(tmp_path, small())


def test_deletes_survive_recovery(tmp_path):
    db = DB(tmp_path, small())
    for i in range(1000):
        db.put(b"k%04d" % i, b"v")
    db.flush()
    for i in range(0, 1000, 2):
        db.delete(b"k%04d" % i)
    db.close()
    db = DB(tmp_path, small())
    assert [k for k, _ in db.scan()] == [b"k%04d" % i for i in range(1, 1000, 2)]
    db.close()


CHILD = textwrap.dedent(
    """
    import sys
    from lsmkv import DB, Options
    db = DB(sys.argv[1], Options(memtable_bytes=8192, wal_sync="always", block_size=512,
                                 target_file_bytes=16384, level_base_bytes=65536))
    i = 0
    while True:
        db.put(b"k%07d" % i, b"v%d" % i)
        print(i, flush=True)  # acknowledged
        i += 1
    """
)


def test_kill_minus_nine_loses_no_acknowledged_write(tmp_path):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.Popen(
        [sys.executable, "-c", CHILD, str(tmp_path)],
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": root},
    )
    acked = -1
    for line in proc.stdout:
        acked = int(line)
        if acked >= 1500:
            break
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    db = DB(tmp_path, small())
    for i in range(acked + 1):
        assert db.get(b"k%07d" % i) == b"v%d" % i
    db.close()

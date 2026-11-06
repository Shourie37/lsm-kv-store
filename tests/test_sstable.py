import os

import pytest

from lsmkv.cache import LRUCache
from lsmkv.errors import CorruptionError
from lsmkv.format import DELETE, PUT
from lsmkv.sstable import FOOTER_SIZE, SSTableReader, SSTableWriter
from lsmkv.stats import ReadStats


def build(path, n=2000, bloom=0.01, block_size=512):
    writer = SSTableWriter(str(path), block_size=block_size, bloom_fp_rate=bloom)
    for i in range(n):
        kind = DELETE if i % 10 == 0 else PUT
        writer.add(b"key%06d" % (i * 2), i + 1, kind, b"" if kind == DELETE else b"v%d" % i)
    return writer.finish()


def test_round_trip_point_lookups_and_metadata(tmp_path):
    path = tmp_path / "t.sst"
    info = build(path)
    reader = SSTableReader(str(path), 1)
    assert info.entries == 2000 and info.tombstones == 200
    assert (reader.smallest, reader.largest) == (b"key000000", b"key003998")
    assert reader.num_blocks > 20  # sparse index: one entry per block
    assert reader.get(b"key000002") == (b"key000002", 2, PUT, b"v1")
    assert reader.get(b"key000000")[2] == DELETE
    assert reader.get(b"key000001") is None
    assert reader.get(b"zzz") is None


def test_iter_range_respects_bounds(tmp_path):
    path = tmp_path / "t.sst"
    build(path, n=500)
    reader = SSTableReader(str(path), 1)
    keys = [e[0] for e in reader.iter_range(b"key000101", b"key000111")]
    assert keys == [b"key%06d" % k for k in (102, 104, 106, 108, 110)]
    assert len(list(reader.iter_range())) == 500


def test_bloom_filter_skips_block_reads_for_missing_keys(tmp_path):
    for bloom, expect_reads in ((0.01, False), (None, True)):
        path = tmp_path / f"t{bloom}.sst"
        build(path, bloom=bloom)
        stats = ReadStats()
        reader = SSTableReader(str(path), 1, LRUCache(0), stats)
        for i in range(1000):
            assert reader.get(b"key%06d" % (i * 2 + 1)) is None
        assert (stats.blocks_read > 500) == expect_reads


def test_footer_corruption_is_detected(tmp_path):
    path = tmp_path / "t.sst"
    build(path)
    with open(path, "r+b") as f:
        f.seek(-FOOTER_SIZE + 3, os.SEEK_END)
        byte = f.read(1)
        f.seek(-1, os.SEEK_CUR)
        f.write(bytes([byte[0] ^ 0xFF]))
    with pytest.raises(CorruptionError, match="footer"):
        SSTableReader(str(path), 1)


def test_data_block_corruption_is_detected_on_read(tmp_path):
    path = tmp_path / "t.sst"
    build(path)
    with open(path, "r+b") as f:
        f.seek(20)
        f.write(b"\x00\x01\x02")
    reader = SSTableReader(str(path), 1, LRUCache(0))
    with pytest.raises(CorruptionError, match="data block 0"):
        reader.get(b"key000002")


def test_writer_rejects_unsorted_keys(tmp_path):
    writer = SSTableWriter(str(tmp_path / "t.sst"))
    writer.add(b"b", 1, PUT, b"")
    with pytest.raises(ValueError):
        writer.add(b"a", 2, PUT, b"")
    writer.abort()
    assert not (tmp_path / "t.sst").exists()

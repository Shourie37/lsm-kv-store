import dataclasses

import pytest

from lsmkv import Options

# Tiny sizes so a few thousand writes exercise flushes and several levels.
SMALL = Options(
    memtable_bytes=4096,
    block_size=512,
    target_file_bytes=8192,
    level_base_bytes=32 << 10,
    tiered_min_sstable_bytes=4096,
    block_cache_bytes=64 << 10,
    wal_sync="none",
    background=False,
)


def small(**overrides) -> Options:
    return dataclasses.replace(SMALL, **overrides)


@pytest.fixture(params=["leveled", "tiered"])
def strategy(request) -> str:
    return request.param

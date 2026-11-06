"""Random operations checked against a dict, over many seeds."""

import random

import pytest

from lsmkv import DB

from .conftest import small

KEY_SPACE = 400


def key(i: int) -> bytes:
    return b"key-%04d" % i


def check(db: DB, model: dict, rng: random.Random) -> None:
    for i in rng.sample(range(KEY_SPACE + 20), 40):
        assert db.get(key(i)) == model.get(key(i))
    lo, hi = sorted(rng.sample(range(KEY_SPACE), 2))
    expected = sorted((k, v) for k, v in model.items() if key(lo) <= k < key(hi))
    assert list(db.scan(key(lo), key(hi))) == expected


def run_model(path, seed: int, ops: int, **options) -> DB:
    rng = random.Random(seed)
    opts = small(**options)
    db = DB(path, opts)
    model: dict[bytes, bytes] = {}
    for step in range(ops):
        r = rng.random()
        k = key(rng.randrange(KEY_SPACE))
        if r < 0.55:
            v = rng.randbytes(rng.randrange(0, 60))
            db.put(k, v)
            model[k] = v
        elif r < 0.75:
            db.delete(k)
            model.pop(k, None)
        elif r < 0.97:
            assert db.get(k) == model.get(k)
        elif r < 0.99:
            check(db, model, rng)
        else:
            db.close()
            db = DB(path, opts)
        if step % 500 == 0:
            check(db, model, rng)
    db.wait_idle()
    assert list(db.scan()) == sorted(model.items())
    db.close()
    db = DB(path, opts)
    assert list(db.scan()) == sorted(model.items())
    db.compact_all()
    assert list(db.scan()) == sorted(model.items())
    return db


@pytest.mark.parametrize("seed", range(12))
def test_random_ops_match_dict(tmp_path, strategy, seed):
    run_model(tmp_path, seed, 3000, compaction=strategy).close()


@pytest.mark.parametrize("seed", range(3))
def test_random_ops_with_background_compaction(tmp_path, strategy, seed):
    run_model(tmp_path, 100 + seed, 3000, compaction=strategy, background=True).close()


def test_without_bloom_filters_or_cache(tmp_path, strategy):
    run_model(
        tmp_path, 7, 2000, compaction=strategy, bloom_fp_rate=None, block_cache_bytes=0
    ).close()


def test_scan_snapshot_survives_compaction_deleting_its_files(tmp_path):
    db = DB(tmp_path, small())
    for i in range(2000):
        db.put(key(i % KEY_SPACE), b"%d" % i)
    expected = list(db.scan())
    it = db.scan()
    first = next(it)
    db.compact_all()  # unlinks every file the scan reads
    assert [first, *it] == expected
    db.close()


def test_rejects_non_bytes(tmp_path):
    with DB(tmp_path, small()) as db, pytest.raises(TypeError):
        db.put("text", b"v")

import itertools
import random

from lsmkv import DB
from lsmkv.compaction import Compaction, TieredStrategy
from lsmkv.version import FileMeta, Version

from .conftest import small


def fill(db: DB, n: int, key_space: int, seed: int = 0) -> dict:
    rng = random.Random(seed)
    model = {}
    for _ in range(n):
        k = b"k%05d" % rng.randrange(key_space)
        v = rng.randbytes(40)
        db.put(k, v)
        model[k] = v
    return model


def test_compaction_preserves_data(tmp_path, strategy):
    db = DB(tmp_path, small(compaction=strategy))
    model = fill(db, 6000, 1500)
    assert db.write_stats.compactions > 0
    assert list(db.scan()) == sorted(model.items())
    db.compact_all()
    assert list(db.scan()) == sorted(model.items())
    db.close()


def test_compact_all_removes_tombstoned_keys(tmp_path, strategy):
    db = DB(tmp_path, small(compaction=strategy))
    model = fill(db, 4000, 1000)
    for k in list(model)[::2]:
        db.delete(k)
        del model[k]
    db.compact_all()
    files = [f for level in db.levels() for f in level]
    assert sum(f.tombstones for f in files) == 0
    assert sum(f.entries for f in files) == len(model)
    assert db.write_stats.tombstones_dropped > 0
    assert all(db.get(k) is None for k in [b"k%05d" % i for i in range(1000)] if k not in model)
    db.close()


def test_leveled_shape_invariants(tmp_path):
    opts = small(compaction="leveled")
    db = DB(tmp_path, opts)
    fill(db, 15000, 5000)
    levels = db.levels()
    assert len(levels[0]) < opts.l0_compaction_trigger
    assert any(levels[2:])  # data reached L2 or deeper
    for depth, files in enumerate(levels[1:], start=1):
        for a, b in itertools.pairwise(files):
            assert a.largest < b.smallest, f"overlap in L{depth}"
        if depth < len(levels) - 1:
            limit = opts.level_base_bytes * opts.level_multiplier ** (depth - 1)
            assert sum(f.size for f in files) <= limit
    db.close()


def test_tiered_merges_runs_of_similar_size(tmp_path):
    opts = small(compaction="tiered")
    db = DB(tmp_path, opts)
    fill(db, 15000, 5000)
    buckets = TieredStrategy(opts).buckets(db.levels()[0])
    assert all(len(b) < opts.tiered_min_threshold for b in buckets)
    assert len(db.levels()[0]) < 16
    db.close()


def meta(number, smallest, largest, min_seq, max_seq, size=100):
    return FileMeta(number, size, smallest, largest, 1, 0, min_seq, max_seq)


def test_tombstone_kept_while_an_older_version_lives_outside_the_compaction(tmp_path):
    db = DB(tmp_path, small(compaction="tiered"))
    db.put(b"a", b"old")
    db.flush()
    db.put(b"filler", b"x")
    db.flush()
    db.delete(b"a")
    db.flush()
    old_run, *newer = sorted(db.levels()[0], key=lambda f: f.max_seq)
    # Merge only the two newer runs; the run holding "a"=old stays outside.
    db._run_compaction(Compaction(tuple(newer), 0, split_output=False))
    assert db.get(b"a") is None
    assert sum(f.tombstones for f in db.levels()[0]) == 1
    db.compact_all()
    assert db.get(b"a") is None
    assert sum(f.tombstones for f in db.levels()[0]) == 0
    db.close()


def test_tiered_read_resolves_newest_across_unordered_runs():
    # A merged run can have a higher max_seq than a run with newer data for a key.
    version = Version([[meta(1, b"a", b"z", 1, 10), meta(2, b"a", b"z", 5, 5)]])
    order = TieredStrategy(small()).candidates(version, b"m")
    assert [f.number for f in order[0]] == [1, 2]  # both are probed; seq decides

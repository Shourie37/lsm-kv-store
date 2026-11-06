import random

from lsmkv.skiplist import SkipList


def test_matches_sorted_dict_under_random_inserts():
    rng = random.Random(7)
    sl = SkipList(seed=1)
    model = {}
    for _ in range(5000):
        key = rng.randrange(2000)
        value = rng.random()
        assert sl.insert(key, value) == (key not in model)
        model[key] = value
    assert len(sl) == len(model)
    assert list(sl.items()) == sorted(model.items())
    for key in range(-5, 2005):
        assert sl.get(key) == model.get(key)


def test_range_bounds_are_start_inclusive_end_exclusive():
    sl = SkipList(seed=2)
    for k in range(0, 100, 2):
        sl.insert(k, str(k))
    assert [k for k, _ in sl.items(10, 20)] == [10, 12, 14, 16, 18]
    assert [k for k, _ in sl.items(11, 15)] == [12, 14]
    assert [k for k, _ in sl.items(None, 4)] == [0, 2]
    assert [k for k, _ in sl.items(95)] == [96, 98]

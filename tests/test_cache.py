from lsmkv.cache import LRUCache


def test_evicts_least_recently_used_by_bytes():
    cache = LRUCache(capacity_bytes=30)
    cache.put("a", 1, 10)
    cache.put("b", 2, 10)
    cache.put("c", 3, 10)
    assert cache.get("a") == 1  # a is now most recent
    cache.put("d", 4, 10)
    assert cache.get("b") is None
    assert [cache.get(k) for k in "acd"] == [1, 3, 4]
    assert cache.used_bytes == 30


def test_oversized_item_is_not_cached_and_zero_capacity_disables():
    cache = LRUCache(capacity_bytes=5)
    cache.put("big", 1, 6)
    assert cache.get("big") is None
    disabled = LRUCache(0)
    disabled.put("x", 1, 1)
    assert len(disabled) == 0


def test_replacing_a_key_updates_charge():
    cache = LRUCache(capacity_bytes=10)
    cache.put("a", 1, 8)
    cache.put("a", 2, 3)
    assert cache.used_bytes == 3 and cache.get("a") == 2

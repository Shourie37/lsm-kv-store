import pytest

from lsmkv.bloom import BloomFilter, bits_per_key


@pytest.mark.parametrize("fp_rate", [0.1, 0.01, 0.001])
def test_false_positive_rate_within_bounds(fp_rate):
    n = 10_000
    bloom = BloomFilter.for_capacity(n, fp_rate)
    for i in range(n):
        bloom.add(b"member-%d" % i)
    assert all(bloom.may_contain(b"member-%d" % i) for i in range(n))
    trials = 50_000
    false_positives = sum(bloom.may_contain(b"absent-%d" % i) for i in range(trials))
    measured = false_positives / trials
    # Generous upper bound; the expected value is fp_rate.
    assert measured <= fp_rate * 1.5 + 0.0005, measured


def test_sizing_follows_formula():
    bloom = BloomFilter.for_capacity(1000, 0.01)
    assert bloom.num_hashes == 7
    assert bloom.num_bits == pytest.approx(1000 * bits_per_key(0.01), abs=1)


def test_round_trip_through_bytes():
    bloom = BloomFilter.for_capacity(100, 0.05)
    for i in range(100):
        bloom.add(bytes([i]))
    copy = BloomFilter.from_bytes(bloom.to_bytes())
    assert copy.num_bits == bloom.num_bits and copy.num_hashes == bloom.num_hashes
    assert all(copy.may_contain(bytes([i])) for i in range(100))

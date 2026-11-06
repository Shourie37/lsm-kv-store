"""Bloom filter with double hashing."""

from __future__ import annotations

import hashlib
import math
import struct

_HEADER = struct.Struct("<IB")


def bits_per_key(fp_rate: float) -> float:
    """Optimal bits per key for a target false-positive rate."""
    return -math.log(fp_rate) / (math.log(2) ** 2)


class BloomFilter:
    def __init__(self, num_bits: int, num_hashes: int, bits: bytearray | None = None) -> None:
        self.num_bits = max(num_bits, 8)
        self.num_hashes = num_hashes
        self.bits = bits if bits is not None else bytearray((self.num_bits + 7) // 8)

    @classmethod
    def for_capacity(cls, expected_keys: int, fp_rate: float) -> BloomFilter:
        if not 0 < fp_rate < 1:
            raise ValueError("fp_rate must be between 0 and 1")
        per_key = bits_per_key(fp_rate)
        num_bits = math.ceil(max(expected_keys, 1) * per_key)
        num_hashes = max(1, round(per_key * math.log(2)))
        return cls(num_bits, num_hashes)

    def _positions(self, key: bytes) -> list[int]:
        digest = hashlib.blake2b(key, digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:], "little") | 1
        m = self.num_bits
        return [(h1 + i * h2) % m for i in range(self.num_hashes)]

    def add(self, key: bytes) -> None:
        for pos in self._positions(key):
            self.bits[pos >> 3] |= 1 << (pos & 7)

    def may_contain(self, key: bytes) -> bool:
        bits = self.bits
        return all(bits[pos >> 3] & (1 << (pos & 7)) for pos in self._positions(key))

    def to_bytes(self) -> bytes:
        return _HEADER.pack(self.num_bits, self.num_hashes) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> BloomFilter:
        num_bits, num_hashes = _HEADER.unpack_from(data)
        return cls(num_bits, num_hashes, bytearray(data[_HEADER.size :]))

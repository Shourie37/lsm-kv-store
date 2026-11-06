"""Shared constants for the on-disk entry encoding."""

import struct

DELETE = 0
PUT = 1

# An entry is (key, seq, kind, value). Higher seq means newer.
Entry = tuple[bytes, int, int, bytes]

# key_len, value_len, seq, kind
ENTRY_HEADER = struct.Struct("<IIQB")

"""Exceptions raised by the storage engine."""


class CorruptionError(Exception):
    """Stored bytes fail a checksum or have an invalid layout."""


class BackgroundError(Exception):
    """A background flush or compaction failed; the database refuses new work."""

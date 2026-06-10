"""Deterministic record ids for dedup / idempotent writes.

A stable id derived from a record's identifying fields means replaying the same
data (e.g. re-running a transform over immutable raw) yields the same ids, so a
``merge`` sink upserts instead of duplicating. The project chooses *which* fields
identify a record (business logic); the SDK only provides the hash.
"""

from __future__ import annotations

import hashlib

_SEP = "\x1f"  # unit separator — unlikely to occur in field values


def deterministic_id(*parts: object) -> str:
    """A stable hex id from the given parts (order-sensitive)."""
    payload = _SEP.join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

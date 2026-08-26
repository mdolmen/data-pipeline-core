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
    # ``surrogatepass``: a lone surrogate reaches us whenever an upstream payload
    # carries an unpaired \\uD800-\\uDFFF escape — ``json.loads`` decodes those
    # happily, and plain UTF-8 encoding then raises. Minting an id must not be the
    # thing that fails a run over one mangled character upstream, and the encoding
    # stays injective, so ids remain collision-free. No existing id changes: any
    # input this affects used to raise instead of hashing.
    return hashlib.sha256(payload.encode("utf-8", "surrogatepass")).hexdigest()

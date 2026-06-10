"""Redis access: the connection factory and a latest-state cache.

``make_redis`` builds the client from a URL; ``RedisCache`` is a thin
JSON latest-snapshot store (the IP-guard counters use the raw client directly,
see ``ingestion/ip_guard.py``).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any

import redis

from data_pipeline_core.storage.protocols import Record, Sink, WriteResult


def make_redis(url: str) -> redis.Redis:
    """Build a Redis client from a connection URL."""
    return redis.Redis.from_url(url)


class RedisCache:
    """Latest-state snapshots keyed by id, JSON-encoded with a TTL."""

    def __init__(self, client: redis.Redis, *, ttl_seconds: int = 3600) -> None:
        self._redis = client
        self._ttl = ttl_seconds

    def set_snapshot(self, key: str, value: dict[str, Any]) -> None:
        self._redis.set(f"snapshot:{key}", json.dumps(value), ex=self._ttl)

    def get_snapshot(self, key: str) -> dict[str, Any] | None:
        raw = self._redis.get(f"snapshot:{key}")
        if raw is None:
            return None
        decoded: dict[str, Any] = json.loads(raw)
        return decoded


class _RedisLatestSink:
    def __init__(self, key_fields: Sequence[str], cache: RedisCache) -> None:
        self._key_fields = tuple(key_fields)
        self._cache = cache

    def write(self, records: Iterable[Record]) -> WriteResult:
        row_count = 0
        for record in records:
            key = ":".join(str(record[field]) for field in self._key_fields)
            self._cache.set_snapshot(key, record)
            row_count += 1
        return WriteResult(row_count=row_count)


def redis_latest_sink(
    key_fields: Sequence[str],
    client: redis.Redis,
    *,
    ttl_seconds: int = 3600,
) -> Sink:
    """A ``Sink`` upserting each record as the latest snapshot for its key.

    Idempotent by construction: re-running overwrites the same keys. The optional
    hot tier — latest state in Redis, no relational database.
    """
    return _RedisLatestSink(key_fields, RedisCache(client, ttl_seconds=ttl_seconds))

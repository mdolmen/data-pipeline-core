"""Redis access: the connection factory and a latest-state cache.

``make_redis`` builds the client from a URL; ``RedisCache`` is a thin
JSON latest-snapshot store (the IP-guard counters use the raw client directly,
see ``ingestion/ip_guard.py``).
"""

from __future__ import annotations

import json
from typing import Any

import redis


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

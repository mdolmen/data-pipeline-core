"""RedisCache: latest-state snapshots round-trip and expire."""

from __future__ import annotations

import fakeredis

from data_pipeline_core import redis_latest_sink
from data_pipeline_core.storage.redis_cache import RedisCache


def test_set_and_get_snapshot(fake_redis: fakeredis.FakeStrictRedis) -> None:
    cache = RedisCache(fake_redis, ttl_seconds=60)
    cache.set_snapshot("match-1", {"home": 2.1, "away": 3.2})

    assert cache.get_snapshot("match-1") == {"home": 2.1, "away": 3.2}
    assert fake_redis.ttl("snapshot:match-1") > 0


def test_missing_snapshot_returns_none(fake_redis: fakeredis.FakeStrictRedis) -> None:
    cache = RedisCache(fake_redis)
    assert cache.get_snapshot("absent") is None


def test_latest_sink_upserts_idempotently(
    fake_redis: fakeredis.FakeStrictRedis,
) -> None:
    sink = redis_latest_sink(["match"], fake_redis)
    records = [
        {"match": "om-ol", "odds": 2.1},
        {"match": "psg-asm", "odds": 1.5},
    ]

    sink.write(records)
    sink.write([{"match": "om-ol", "odds": 2.4}])  # newer tick for the same key

    # One entry per key (no duplication) and it holds the latest value.
    keys = fake_redis.keys("snapshot:*")
    assert len(keys) == 2
    cache = RedisCache(fake_redis)
    assert cache.get_snapshot("om-ol") == {"match": "om-ol", "odds": 2.4}

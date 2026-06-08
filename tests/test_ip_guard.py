"""IP guard: counter increments within a window and classifies the mode."""

from __future__ import annotations

import fakeredis

from data_pipeline_core.ingestion.ip_guard import IpGuard, Mode


def _guard(redis_client: fakeredis.FakeStrictRedis) -> IpGuard:
    return IpGuard(
        "s",
        redis_client,
        warning_at=300,
        aggressive_at=500,
        window_seconds=3600,
        clock=lambda: 0.0,
    )


def test_classify_thresholds(fake_redis: fakeredis.FakeStrictRedis) -> None:
    guard = _guard(fake_redis)
    assert guard.classify(1) is Mode.SAFE
    assert guard.classify(299) is Mode.SAFE
    assert guard.classify(300) is Mode.WARNING
    assert guard.classify(499) is Mode.WARNING
    assert guard.classify(500) is Mode.AGGRESSIVE


def test_record_increments_counter(fake_redis: fakeredis.FakeStrictRedis) -> None:
    guard = _guard(fake_redis)
    assert guard.record() == 1
    assert guard.record() == 2
    # bucket key carries a TTL so old windows expire
    assert fake_redis.ttl("ratelimit:s:0") > 0


def test_evaluate_switches_to_aggressive(fake_redis: fakeredis.FakeStrictRedis) -> None:
    fake_redis.set("ratelimit:s:0", 500)  # simulate prior density >500 req/hr
    guard = _guard(fake_redis)
    assert guard.evaluate() is Mode.AGGRESSIVE  # 501 after this request

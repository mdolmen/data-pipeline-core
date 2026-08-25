"""Circuit breaker: opens on repeated 429s, resets on success, reopens closed."""

from __future__ import annotations

import fakeredis
from prometheus_client import CollectorRegistry

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.obs.metrics import StandardMetrics


def _make(
    clock: list[float],
    *,
    threshold: int = 3,
    cooldown_seconds: float = 900.0,
    client: fakeredis.FakeStrictRedis | None = None,
) -> tuple[CircuitBreaker, CollectorRegistry]:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="s", stage="ingest")
    breaker = CircuitBreaker(
        "s",
        threshold=threshold,
        cooldown_seconds=cooldown_seconds,
        metrics=metrics,
        clock=lambda: clock[0],
        client=client,
    )
    return breaker, registry


def _state(registry: CollectorRegistry) -> float | None:
    return registry.get_sample_value(
        "circuit_breaker_state", {"source": "s", "stage": "ingest"}
    )


def test_opens_after_threshold_failures() -> None:
    breaker, registry = _make([0.0], threshold=3)

    breaker.record_failure()
    breaker.record_failure()
    assert not breaker.is_open
    assert _state(registry) == 0

    breaker.record_failure()  # third consecutive 429
    assert breaker.is_open
    assert _state(registry) == 1


def test_success_resets_failure_streak() -> None:
    breaker, _ = _make([0.0], threshold=3)

    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    breaker.record_failure()
    assert not breaker.is_open  # only two failures since the reset


def test_redis_halt_survives_into_another_breaker() -> None:
    # The point of the whole fix: two breakers over one Redis are two runs of the
    # same one-shot job. The second must inherit the halt the first opened.
    shared = fakeredis.FakeStrictRedis()
    first, _ = _make([0.0], threshold=1, client=shared)
    second, registry = _make([0.0], threshold=1, client=shared)

    first.record_failure()

    assert first.is_open
    assert second.is_open
    # ...and reports it, despite never having seen the transition itself.
    assert _state(registry) == 1


def test_redis_cooldown_is_the_key_ttl() -> None:
    shared = fakeredis.FakeStrictRedis()
    breaker, _ = _make([0.0], threshold=1, cooldown_seconds=900.0, client=shared)

    breaker.record_failure()

    assert shared.ttl("breaker:s:open") == 900  # expiry *is* the cooldown
    shared.delete("breaker:s:open")  # stand in for that expiry
    assert not breaker.is_open


def test_redis_failure_streak_is_shared_across_breakers() -> None:
    # Consecutive means consecutive against the IP, not against one process.
    shared = fakeredis.FakeStrictRedis()
    first, _ = _make([0.0], threshold=3, client=shared)
    second, _ = _make([0.0], threshold=3, client=shared)

    first.record_failure()
    first.record_failure()
    assert not second.is_open

    second.record_failure()  # third across both → opens
    assert first.is_open


def test_redis_success_clears_the_streak_but_not_an_open_halt() -> None:
    # An in-flight 200 from a sibling worker must not cancel an IP-wide halt.
    shared = fakeredis.FakeStrictRedis()
    breaker, _ = _make([0.0], threshold=2, client=shared)

    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert not breaker.is_open  # streak was cleared, so one failure isn't two

    breaker.record_failure()
    assert breaker.is_open
    breaker.record_success()
    assert breaker.is_open  # still halted — only the TTL lifts it


def test_reopens_closed_after_cooldown() -> None:
    clock = [0.0]
    breaker, registry = _make(clock, threshold=1, cooldown_seconds=100.0)

    breaker.record_failure()
    assert breaker.is_open
    assert _state(registry) == 1

    clock[0] = 100.0  # cooldown elapsed
    assert not breaker.is_open
    assert _state(registry) == 0

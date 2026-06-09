"""Circuit breaker: opens on repeated 429s, resets on success, reopens closed."""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.obs.metrics import StandardMetrics


def _make(
    clock: list[float], *, threshold: int = 3, cooldown_seconds: float = 900.0
) -> tuple[CircuitBreaker, CollectorRegistry]:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="s", stage="ingest")
    breaker = CircuitBreaker(
        "s",
        threshold=threshold,
        cooldown_seconds=cooldown_seconds,
        metrics=metrics,
        clock=lambda: clock[0],
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


def test_reopens_closed_after_cooldown() -> None:
    clock = [0.0]
    breaker, registry = _make(clock, threshold=1, cooldown_seconds=100.0)

    breaker.record_failure()
    assert breaker.is_open
    assert _state(registry) == 1

    clock[0] = 100.0  # cooldown elapsed
    assert not breaker.is_open
    assert _state(registry) == 0

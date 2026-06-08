"""Per-source circuit breaker.

Protects IP reputation: after a configurable number of consecutive ``429 Too
Many Requests`` the breaker opens and halts that source for a cooldown (default
15 min), flipping ``circuit_breaker_state`` to 1. A success resets the streak.

Phase 4 keeps the state in-memory for the duration of a run; cross-run
persistence (so a halt survives worker restarts) is Redis-backed in Phase 5.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from data_pipeline_core.obs.metrics import StandardMetrics


class CircuitBreaker:
    """Opens after ``threshold`` consecutive 429s; reopens closed after cooldown."""

    def __init__(
        self,
        source: str,
        *,
        threshold: int,
        cooldown_seconds: float,
        metrics: StandardMetrics | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._threshold = threshold
        self._cooldown = cooldown_seconds
        self._metrics = metrics
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None
        self._publish(0)

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if self._clock() - self._opened_at >= self._cooldown:
            self._reset()  # cooldown elapsed → allow traffic again
            return False
        return True

    def record_success(self) -> None:
        if self._opened_at is not None:
            self._reset()
        else:
            self._failures = 0

    def record_failure(self) -> None:
        self._failures += 1
        if self._opened_at is None and self._failures >= self._threshold:
            self._opened_at = self._clock()
            self._publish(1)

    def _reset(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._publish(0)

    def _publish(self, state: int) -> None:
        if self._metrics is not None:
            self._metrics.circuit_breaker_state.labels(source=self._source).set(state)

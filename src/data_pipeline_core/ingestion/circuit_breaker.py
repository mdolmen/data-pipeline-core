"""Per-source circuit breaker.

Protects IP reputation: after a configurable number of consecutive ``429 Too
Many Requests`` the breaker opens and halts that source for a cooldown (default
15 min), flipping ``circuit_breaker_state`` to 1. A success resets the streak.

State lives in Redis when a client is supplied, so a halt survives the process —
workers are one-shot Cloud Run Jobs, and an in-memory halt dies at exit, which
makes a cooldown longer than one run unreachable. The Redis keys use the TTL
*as* the cooldown: expiry is the reset, so there is no clock arithmetic, no
skew between workers, and nothing to clean up. Without a client it falls back to
in-memory state, which is per-run only.

Redis-backed, the halt is also shared *across* workers, not just across runs.
That is the intent — every worker for a source shares an egress IP, so
protection belongs to the IP — but one worker tripping the breaker halts them
all. If Redis is unreachable the error surfaces to the caller rather than being
swallowed; the run loop already treats a failed run as a failed run.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import redis

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
        client: redis.Redis | None = None,
    ) -> None:
        self._source = source
        self._threshold = threshold
        self._cooldown = cooldown_seconds
        self._metrics = metrics
        self._clock = clock
        self._redis = client
        self._open_key = f"breaker:{source}:open"
        self._failures_key = f"breaker:{source}:failures"
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        if self._redis is not None:
            # The key's TTL is the cooldown, so its absence *is* the reset.
            # Publish what we observed: a worker that starts life already halted
            # by another run never sees a transition, and would otherwise report
            # circuit_breaker_state=0 while refusing every request.
            halted = bool(self._redis.exists(self._open_key))
            self._publish(is_open=halted)
            return halted
        if self._opened_at is None:
            return False
        if self._clock() - self._opened_at >= self._cooldown:
            self._reset()  # cooldown elapsed → allow traffic again
            return False
        return True

    def record_success(self) -> None:
        if self._redis is not None:
            # Only the streak. An in-flight success from another worker must not
            # cancel an IP-wide halt — that is the TTL's job.
            self._redis.delete(self._failures_key)
            return
        if self._opened_at is not None:
            self._reset()
        else:
            self._failures = 0

    def record_failure(self) -> None:
        if self._redis is not None:
            self._record_failure_shared()
            return
        self._failures += 1
        if self._opened_at is None and self._failures >= self._threshold:
            self._opened_at = self._clock()
            self._publish(is_open=True)

    def _record_failure_shared(self) -> None:
        assert self._redis is not None
        pipe = self._redis.pipeline()
        pipe.incr(self._failures_key)
        # A streak that has gone quiet for a whole cooldown is stale, not
        # consecutive — let it lapse rather than carrying it forever.
        pipe.expire(self._failures_key, self._ttl_seconds)
        streak = int(pipe.execute()[0])
        if streak >= self._threshold:
            # Plain SET, so continued failures extend the halt: still being
            # throttled is a reason to stay down, not to come back on schedule.
            self._redis.set(self._open_key, 1, ex=self._ttl_seconds)
            self._publish(is_open=True)

    @property
    def _ttl_seconds(self) -> int:
        # Redis expiries are whole seconds; never round a cooldown down to 0.
        return max(1, int(self._cooldown))

    def _reset(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._publish(is_open=False)

    def _publish(self, *, is_open: bool) -> None:
        if self._metrics is not None:
            self._metrics.set_circuit_breaker_open(is_open)

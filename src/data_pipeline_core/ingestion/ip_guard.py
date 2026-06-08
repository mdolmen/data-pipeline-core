"""IP guard — request-density tracking that drives a mode switch.

Each request increments a per-source, per-window counter in Redis; the count
classifies the source into Safe / Warning / Aggressive. The thresholds are
config (defaults from the betting architecture §6: Safe <300, Warning 300-500,
Aggressive >=500 req/hr). The HTTP client acts on the mode: extra jitter in
Warning, route via proxy in Aggressive.
"""

from __future__ import annotations

import enum
import time
from collections.abc import Callable

import redis


class Mode(enum.Enum):
    SAFE = "safe"
    WARNING = "warning"
    AGGRESSIVE = "aggressive"


class IpGuard:
    """Sliding-window request counter (fixed buckets) with a mode classifier."""

    def __init__(
        self,
        source: str,
        client: redis.Redis,
        *,
        warning_at: int,
        aggressive_at: int,
        window_seconds: int = 3600,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._source = source
        self._redis = client
        self._warning_at = warning_at
        self._aggressive_at = aggressive_at
        self._window = window_seconds
        self._clock = clock

    def evaluate(self) -> Mode:
        """Record this request and return the resulting mode."""
        return self.classify(self.record())

    def record(self) -> int:
        """Increment the current window's counter and return its value."""
        bucket = int(self._clock() // self._window)
        key = f"ratelimit:{self._source}:{bucket}"
        pipe = self._redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, self._window * 2)  # keep last window for analysis
        count = pipe.execute()[0]
        return int(count)

    def classify(self, count: int) -> Mode:
        if count >= self._aggressive_at:
            return Mode.AGGRESSIVE
        if count >= self._warning_at:
            return Mode.WARNING
        return Mode.SAFE

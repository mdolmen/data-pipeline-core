"""Instrumented HTTP client used by a ``Source`` (via ``ctx.http``).

Wraps ``httpx`` with the generic ingestion plumbing: retry with jitter on
transient failures (network / 5xx), User-Agent rotation, per-response metrics
(``http_status_total``, request count → ``request_rate``), and circuit-breaker
integration. A 429 is recorded with the breaker (not retried — it's a rate
signal); when the breaker is open, a request raises ``CircuitOpenError`` so the
source can stop cleanly. All knobs come from ``Settings``.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import httpx
from structlog.typing import FilteringBoundLogger

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.obs.metrics import StandardMetrics

if TYPE_CHECKING:
    from data_pipeline_core.runtime.config import Settings


class CircuitOpenError(RuntimeError):
    """Raised when a request is attempted while the source's breaker is open."""


class HttpClient:
    """An ``httpx.Client`` with retry, UA rotation, metrics, and a breaker."""

    def __init__(
        self,
        source: str,
        *,
        settings: Settings,
        breaker: CircuitBreaker,
        metrics: StandardMetrics | None = None,
        logger: FilteringBoundLogger | None = None,
        sleep: Callable[[float], None] = time.sleep,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._source = source
        self._settings = settings
        self._breaker = breaker
        self._metrics = metrics
        self._log = logger
        self._sleep = sleep
        self._rng = random.Random()
        self._user_agents = tuple(settings.http_user_agents)
        self._client = httpx.Client(
            timeout=settings.http_timeout_seconds, transport=transport
        )
        self.request_count = 0

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        if self._breaker.is_open:
            raise CircuitOpenError(f"circuit open for source {self._source!r}")

        headers = dict(kwargs.pop("headers", None) or {})
        if self._user_agents:
            headers.setdefault("User-Agent", self._rng.choice(self._user_agents))

        attempts = max(1, self._settings.http_max_retries + 1)
        transport_error: httpx.TransportError | None = None
        for attempt in range(attempts):
            try:
                response = self._client.request(method, url, headers=headers, **kwargs)
            except httpx.TransportError as exc:
                transport_error = exc
                self._backoff(attempt)
                continue

            self.request_count += 1
            self._record_status(response.status_code)

            if response.status_code == 429:
                self._breaker.record_failure()
                return response
            if response.status_code >= 500 and attempt + 1 < attempts:
                self._backoff(attempt)
                continue
            if response.is_success:
                self._breaker.record_success()
            return response

        assert transport_error is not None  # loop only exits here via continue
        raise transport_error

    def close(self) -> None:
        self._client.close()

    def _backoff(self, attempt: int) -> None:
        # Exponential backoff with full jitter.
        ceiling = self._settings.http_backoff_base_seconds * (2**attempt)
        self._sleep(self._rng.uniform(0, ceiling))

    def _record_status(self, code: int) -> None:
        if self._metrics is not None:
            self._metrics.http_status_total.labels(
                source=self._source, code=str(code)
            ).inc()

"""Instrumented HTTP client used by a ``Source`` (via ``ctx.http``).

Wraps ``httpx`` with the generic ingestion plumbing: retry with jitter on
transient failures (network / 5xx), User-Agent rotation, per-response metrics
(``http_status_total``, request count → ``request_rate``), circuit-breaker
integration, and IP-guard-driven mode switching (extra jitter in Warning, proxy
routing in Aggressive → feeds ``proxy_usage_ratio``). A 429 is recorded with the
breaker (not retried); an open breaker raises ``CircuitOpenError``.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

import httpx
from structlog.typing import FilteringBoundLogger

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.ingestion.impersonation import (
    Client,
    Response,
    _CurlClient,
    make_client,
)
from data_pipeline_core.ingestion.ip_guard import IpGuard, Mode
from data_pipeline_core.ingestion.proxy import ProxyRouter
from data_pipeline_core.obs.metrics import StandardMetrics

if TYPE_CHECKING:
    from data_pipeline_core.runtime.config import Settings


class CircuitOpenError(RuntimeError):
    """Raised when a request is attempted while the source's breaker is open."""


class HttpClient:
    """An ``httpx.Client`` with retry, UA rotation, metrics, breaker, IP guard."""

    def __init__(
        self,
        source: str,
        *,
        settings: Settings,
        breaker: CircuitBreaker,
        metrics: StandardMetrics | None = None,
        ip_guard: IpGuard | None = None,
        proxy: ProxyRouter | None = None,
        logger: FilteringBoundLogger | None = None,
        sleep: Callable[[float], None] = time.sleep,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._source = source
        self._settings = settings
        self._breaker = breaker
        self._metrics = metrics
        self._ip_guard = ip_guard
        self._proxy = proxy
        self._log = logger
        self._sleep = sleep
        self._rng = random.Random()
        self._user_agents = tuple(settings.http_user_agents)
        self._client: Client = make_client(
            settings.impersonate,
            timeout=settings.http_timeout_seconds,
            transport=transport,
        )
        self.request_count = 0
        self.proxied_count = 0

    def get(self, url: str, *, force_proxy: bool = False, **kwargs: Any) -> Response:
        return self.request("GET", url, force_proxy=force_proxy, **kwargs)

    def request(
        self, method: str, url: str, *, force_proxy: bool = False, **kwargs: Any
    ) -> Response:
        if self._breaker.is_open:
            raise CircuitOpenError(f"circuit open for source {self._source!r}")

        headers = dict(kwargs.pop("headers", None) or {})
        # When impersonating, curl_cffi sets a matching browser UA — don't override it.
        if self._user_agents and not self._settings.impersonate:
            headers.setdefault("User-Agent", self._rng.choice(self._user_agents))

        mode = self._ip_guard.evaluate() if self._ip_guard is not None else Mode.SAFE
        if mode in (Mode.WARNING, Mode.AGGRESSIVE):
            self._sleep(self._rng.uniform(0, self._settings.warning_jitter_seconds))

        client = self._client
        use_proxy = False
        if self._proxy is not None and self._proxy.should_use(mode, force=force_proxy):
            proxied = self._proxy.client
            if proxied is not None:
                client, use_proxy = proxied, True
        if use_proxy:
            self.proxied_count += 1

        attempts = max(1, self._settings.http_max_retries + 1)
        transport_error: httpx.TransportError | None = None
        for attempt in range(attempts):
            try:
                response = client.request(method, url, headers=headers, **kwargs)
            except httpx.TransportError as exc:
                transport_error = exc
                if self._log is not None:
                    self._log.warning(
                        "http request failed",
                        method=method,
                        url=url,
                        attempt=attempt + 1,
                        error=str(exc),
                    )
                self._backoff(attempt)
                continue

            self.request_count += 1
            self._record_status(response.status_code)
            if self._log is not None:
                self._log.info(
                    "http request",
                    method=method,
                    url=url,
                    status=response.status_code,
                    proxied=use_proxy,
                    attempt=attempt + 1,
                )

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

    def stream(
        self, method: str, url: str, *, force_proxy: bool = False, **kwargs: Any
    ) -> Iterator[bytes]:
        """Yield response body chunks — for streaming endpoints (e.g. gRPC-web).

        Same breaker / IP-guard / proxy guards as ``request``; no retry (a stream
        can't be safely replayed). The caller reads as much as it needs and stops.
        """
        if self._breaker.is_open:
            raise CircuitOpenError(f"circuit open for source {self._source!r}")

        headers = dict(kwargs.pop("headers", None) or {})
        if self._user_agents and not self._settings.impersonate:
            headers.setdefault("User-Agent", self._rng.choice(self._user_agents))

        mode = self._ip_guard.evaluate() if self._ip_guard is not None else Mode.SAFE
        if mode in (Mode.WARNING, Mode.AGGRESSIVE):
            self._sleep(self._rng.uniform(0, self._settings.warning_jitter_seconds))

        client = self._client
        use_proxy = False
        if self._proxy is not None and self._proxy.should_use(mode, force=force_proxy):
            proxied = self._proxy.client
            if proxied is not None:
                client, use_proxy = proxied, True
        if use_proxy:
            self.proxied_count += 1

        self.request_count += 1
        if self._log is not None:
            self._log.info("http stream", method=method, url=url, proxied=use_proxy)

        if isinstance(client, _CurlClient):
            yield from client.stream(
                method,
                url,
                headers=headers,
                content=kwargs.get("content"),
                params=kwargs.get("params"),
            )
        else:
            with client.stream(method, url, headers=headers, **kwargs) as response:
                self._record_status(response.status_code)
                yield from response.iter_bytes()

    def close(self) -> None:
        self._client.close()

    def _backoff(self, attempt: int) -> None:
        # Exponential backoff with full jitter.
        ceiling = self._settings.http_backoff_base_seconds * (2**attempt)
        self._sleep(self._rng.uniform(0, ceiling))

    def _record_status(self, code: int) -> None:
        if self._metrics is not None:
            self._metrics.observe_http_status(code)

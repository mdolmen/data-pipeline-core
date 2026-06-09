"""The standard operational series — the stable observability surface.

Fixed names + labels so Grafana dashboards are shared across every consumer:
**do not rename or relabel** (see ARCHITECTURE.md §8). Every series carries
``source`` and ``stage`` (ingest vs transform worker). The ``(source, stage)``
label set is owned here and applied through the semantic methods below, so
callers (breaker, HTTP client, run loop) never handle labels directly.

Some series are populated by mechanisms in earlier phases (HTTP client, breaker,
proxy); any not yet driven read 0.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge


class StandardMetrics:
    """Holds the standard series, bound to one run's registry, source and stage."""

    def __init__(self, registry: CollectorRegistry, *, source: str, stage: str) -> None:
        self._base = {"source": source, "stage": stage}
        labels = ["source", "stage"]

        self._worker_up = Gauge(
            "worker_up",
            "Worker liveness: 1 if the run completed successfully, else 0.",
            labels,
            registry=registry,
        )
        self._request_rate = Gauge(
            "request_rate",
            "Outbound HTTP request rate (requests/second) over the run.",
            labels,
            registry=registry,
        )
        # Counter base name "http_status" → exposed series "http_status_total".
        self._http_status = Counter(
            "http_status",
            "Outbound HTTP responses by status code.",
            [*labels, "code"],
            registry=registry,
        )
        self._ingestion_lag = Gauge(
            "ingestion_lag_seconds",
            "Seconds since the last successful tick for this source.",
            labels,
            registry=registry,
        )
        self._circuit_breaker = Gauge(
            "circuit_breaker_state",
            "Per-source circuit breaker: 0 closed, 1 open.",
            labels,
            registry=registry,
        )
        self._proxy_ratio = Gauge(
            "proxy_usage_ratio",
            "Share of requests routed via the proxy (0..1).",
            labels,
            registry=registry,
        )

        # Initialise the gauges so the series export at 0 before their mechanism
        # populates them (the counter appears once a status is first seen).
        for gauge in (
            self._worker_up,
            self._request_rate,
            self._ingestion_lag,
            self._circuit_breaker,
            self._proxy_ratio,
        ):
            gauge.labels(**self._base).set(0)

    def set_worker_up(self, up: bool) -> None:
        self._worker_up.labels(**self._base).set(1 if up else 0)

    def set_ingestion_lag(self, seconds: float) -> None:
        self._ingestion_lag.labels(**self._base).set(seconds)

    def set_request_rate(self, rate: float) -> None:
        self._request_rate.labels(**self._base).set(rate)

    def set_proxy_usage_ratio(self, ratio: float) -> None:
        self._proxy_ratio.labels(**self._base).set(ratio)

    def set_circuit_breaker_open(self, is_open: bool) -> None:
        self._circuit_breaker.labels(**self._base).set(1 if is_open else 0)

    def observe_http_status(self, code: int) -> None:
        self._http_status.labels(**self._base, code=str(code)).inc()

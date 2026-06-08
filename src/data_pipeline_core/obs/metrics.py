"""The standard operational series — the stable observability surface.

Fixed names + labels so Grafana dashboards are shared across every consumer:
**do not rename or relabel** (see ARCHITECTURE.md §8). Several series are
populated by mechanisms that arrive in later phases (the HTTP client →
``request_rate`` / ``http_status_total`` in Phase 4, the circuit breaker →
``circuit_breaker_state`` in Phase 4, the proxy → ``proxy_usage_ratio`` in
Phase 5); they are declared here now and read 0 until then.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge


class StandardMetrics:
    """Holds the standard series, all bound to one run's registry."""

    def __init__(self, registry: CollectorRegistry, *, source: str) -> None:
        self.source = source

        self.worker_up = Gauge(
            "worker_up",
            "Worker liveness: 1 if the run completed successfully, else 0.",
            ["source"],
            registry=registry,
        )
        self.request_rate = Gauge(
            "request_rate",
            "Outbound HTTP request rate (requests/second) over the run.",
            ["source"],
            registry=registry,
        )
        # Counter base name "http_status" → exposed series "http_status_total".
        self.http_status_total = Counter(
            "http_status",
            "Outbound HTTP responses by status code.",
            ["source", "code"],
            registry=registry,
        )
        self.ingestion_lag_seconds = Gauge(
            "ingestion_lag_seconds",
            "Seconds since the last successful tick for this source.",
            ["source"],
            registry=registry,
        )
        self.circuit_breaker_state = Gauge(
            "circuit_breaker_state",
            "Per-source circuit breaker: 0 closed, 1 open.",
            ["source"],
            registry=registry,
        )
        self.proxy_usage_ratio = Gauge(
            "proxy_usage_ratio",
            "Share of requests routed via the proxy (0..1).",
            ["source"],
            registry=registry,
        )

        # Initialise the gauges so the series export at 0 before their mechanism
        # populates them (counters appear once a labelled value is first seen).
        for gauge in (
            self.worker_up,
            self.request_rate,
            self.ingestion_lag_seconds,
            self.circuit_breaker_state,
            self.proxy_usage_ratio,
        ):
            gauge.labels(source=source).set(0)

"""The standard series exist with the fixed names + labels, initialised to 0."""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from data_pipeline_core.obs.metrics import StandardMetrics


def test_gauges_initialised_to_zero() -> None:
    registry = CollectorRegistry()
    StandardMetrics(registry, source="demo")

    labels = {"source": "demo"}
    assert registry.get_sample_value("worker_up", labels) == 0
    assert registry.get_sample_value("request_rate", labels) == 0
    assert registry.get_sample_value("ingestion_lag_seconds", labels) == 0
    assert registry.get_sample_value("circuit_breaker_state", labels) == 0
    assert registry.get_sample_value("proxy_usage_ratio", labels) == 0


def test_http_status_total_series_name_and_labels() -> None:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="demo")

    metrics.http_status_total.labels(source="demo", code="429").inc()

    value = registry.get_sample_value(
        "http_status_total", {"source": "demo", "code": "429"}
    )
    assert value == 1

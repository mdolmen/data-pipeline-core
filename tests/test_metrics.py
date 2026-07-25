"""The standard series exist with the fixed names + (source, stage) labels."""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from data_pipeline_core.obs.metrics import StandardMetrics


def test_gauges_initialised_to_zero() -> None:
    registry = CollectorRegistry()
    StandardMetrics(registry, source="demo", stage="ingest")

    labels = {"source": "demo", "stage": "ingest"}
    assert registry.get_sample_value("worker_up", labels) == 0
    assert registry.get_sample_value("request_rate", labels) == 0
    assert registry.get_sample_value("ingestion_lag_seconds", labels) == 0
    assert registry.get_sample_value("circuit_breaker_state", labels) == 0
    assert registry.get_sample_value("proxy_usage_ratio", labels) == 0


def test_semantic_setters() -> None:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="demo", stage="transform")
    labels = {"source": "demo", "stage": "transform"}

    metrics.set_worker_up(True)
    metrics.set_request_rate(4.5)
    metrics.set_proxy_usage_ratio(0.5)
    metrics.set_circuit_breaker_open(True)

    assert registry.get_sample_value("worker_up", labels) == 1
    assert registry.get_sample_value("request_rate", labels) == 4.5
    assert registry.get_sample_value("proxy_usage_ratio", labels) == 0.5
    assert registry.get_sample_value("circuit_breaker_state", labels) == 1


def test_http_status_total_series_name_and_labels() -> None:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="demo", stage="ingest")

    metrics.observe_http_status(429)

    value = registry.get_sample_value(
        "http_status_total", {"source": "demo", "stage": "ingest", "code": "429"}
    )
    assert value == 1


def _runs(
    registry: CollectorRegistry, source: str, stage: str, status: str
) -> float | None:
    return registry.get_sample_value(
        "worker_runs_total", {"source": source, "stage": stage, "status": status}
    )


def test_run_and_write_counters_start_at_zero() -> None:
    registry = CollectorRegistry()
    StandardMetrics(registry, source="demo", stage="ingest")

    labels = {"source": "demo", "stage": "ingest"}
    assert _runs(registry, "demo", "ingest", "success") == 0
    assert _runs(registry, "demo", "ingest", "failure") == 0
    assert registry.get_sample_value("records_written_total", labels) == 0
    assert registry.get_sample_value("bytes_written_total", labels) == 0


def test_run_and_write_counters_increment() -> None:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="demo", stage="ingest")
    labels = {"source": "demo", "stage": "ingest"}

    metrics.observe_run_finished(success=True)
    metrics.observe_records_written(3)
    metrics.observe_bytes_written(128)

    assert _runs(registry, "demo", "ingest", "success") == 1
    assert _runs(registry, "demo", "ingest", "failure") == 0
    assert registry.get_sample_value("records_written_total", labels) == 3
    assert registry.get_sample_value("bytes_written_total", labels) == 128

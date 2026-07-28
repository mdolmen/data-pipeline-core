"""OTLP/HTTP push: the registry encodes to the OTLP JSON shape (names preserved,
counters → monotonic sums, gauges → gauges), and the POST carries basic-auth and
never raises."""

from __future__ import annotations

from typing import Any

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge
from pytest_httpx import HTTPXMock

from data_pipeline_core.obs.otlp_push import _encode_metrics, otlp_push_metrics

log = structlog.get_logger()


def _metrics_by_name(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    scope = payload["resourceMetrics"][0]["scopeMetrics"][0]
    return {m["name"]: m for m in scope["metrics"]}


def test_encode_gauge_and_counter_shapes_and_names() -> None:
    registry = CollectorRegistry()
    Gauge("worker_up", "liveness", ["source"], registry=registry).labels(
        source="s"
    ).set(1)
    Counter("worker_runs", "runs", ["source", "status"], registry=registry).labels(
        source="s", status="success"
    ).inc()

    payload = _encode_metrics(registry, job="worker-a", timestamp_ns=123)
    metrics = _metrics_by_name(payload)

    # names preserved exactly (counter exposes worker_runs_total).
    assert set(metrics) == {"worker_up", "worker_runs_total"}

    gauge = metrics["worker_up"]
    assert "gauge" in gauge
    gp = gauge["gauge"]["dataPoints"][0]
    assert gp["asDouble"] == 1
    assert gp["timeUnixNano"] == "123"
    assert {"key": "source", "value": {"stringValue": "s"}} in gp["attributes"]

    counter = metrics["worker_runs_total"]
    assert counter["sum"]["isMonotonic"] is True
    assert counter["sum"]["aggregationTemporality"] == 2  # cumulative
    cp = counter["sum"]["dataPoints"][0]
    assert cp["asDouble"] == 1
    assert cp["startTimeUnixNano"] == "123"


def test_encode_skips_created_series() -> None:
    registry = CollectorRegistry()
    Counter("worker_runs", "runs", registry=registry).inc()

    metrics = _metrics_by_name(_encode_metrics(registry, job="j", timestamp_ns=1))

    assert "worker_runs_total" in metrics
    assert "worker_runs_created" not in metrics


def test_encode_sets_service_name_resource_attribute() -> None:
    payload = _encode_metrics(CollectorRegistry(), job="worker-a", timestamp_ns=1)
    attrs = payload["resourceMetrics"][0]["resource"]["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "worker-a"}} in attrs


def test_otlp_push_posts_json_with_basic_auth(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200)
    registry = CollectorRegistry()
    Gauge("worker_up", "d", registry=registry).set(1)

    otlp_push_metrics(
        registry,
        url="https://otlp.example/otlp/v1/metrics",
        username="12345",
        password="tok",
        job="worker-a",
        logger=log,
    )

    request = httpx_mock.get_request()
    assert request is not None
    assert request.method == "POST"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Authorization"] == "Basic MTIzNDU6dG9r"  # 12345:tok


def test_otlp_push_never_raises_on_http_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=500)
    registry = CollectorRegistry()
    Gauge("worker_up", "d", registry=registry).set(1)

    otlp_push_metrics(
        registry,
        url="https://otlp.example/otlp/v1/metrics",
        username=None,
        password=None,
        job="worker-a",
        logger=log,
    )


def test_otlp_push_skipped_when_no_url(httpx_mock: HTTPXMock) -> None:
    otlp_push_metrics(
        CollectorRegistry(),
        url=None,
        username=None,
        password=None,
        job="worker-a",
        logger=log,
    )
    assert httpx_mock.get_requests() == []

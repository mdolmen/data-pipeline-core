"""End-of-run metrics push over **OTLP/HTTP (JSON)** — the preferred path.

Short-lived workers exit before a scrape can reach them, so the worker writes its
final series straight to an OTLP-accepting backend (e.g. Grafana Cloud's OTLP
gateway) at exit — no PushGateway/scraper middle-boxes. OTLP/HTTP with a JSON body
needs no protobuf or compression, so this is a plain ``httpx`` POST of the
``ExportMetricsServiceRequest`` shape built straight from the Prometheus registry.

Naming: metric names are sent **exactly** as the Prometheus registry exposes them
(``worker_runs_total``, ``worker_up``, …) with no OTLP ``unit`` set, so Grafana's
OTLP→Prometheus translation keeps them as-is (it won't double a ``_total`` suffix
or append a unit). Counters map to a monotonic cumulative ``sum``, gauges to
``gauge``. A push never fails the run — observability must not break ingestion.
"""

from __future__ import annotations

import base64
import time
from typing import Any

import httpx
from prometheus_client import CollectorRegistry
from structlog.typing import FilteringBoundLogger

_SCOPE_NAME = "data-pipeline-core"
_AGGREGATION_TEMPORALITY_CUMULATIVE = 2
_SKIP_SUFFIX = "_created"  # prometheus_client's per-counter creation-time gauge


def _attributes(labels: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {"key": key, "value": {"stringValue": value}} for key, value in labels.items()
    ]


def _encode_metrics(
    registry: CollectorRegistry, *, job: str, timestamp_ns: int
) -> dict[str, Any]:
    """Build the OTLP ``ExportMetricsServiceRequest`` JSON from the registry."""
    timestamp = str(timestamp_ns)
    kinds: dict[str, str] = {}
    points: dict[str, list[dict[str, Any]]] = {}
    for family in registry.collect():
        kind = "sum" if family.type == "counter" else "gauge"
        for sample in family.samples:
            if sample.name.endswith(_SKIP_SUFFIX):
                continue
            kinds[sample.name] = kind
            points.setdefault(sample.name, []).append(
                {
                    "asDouble": sample.value,
                    "timeUnixNano": timestamp,
                    "attributes": _attributes(dict(sample.labels)),
                }
            )

    metrics: list[dict[str, Any]] = []
    for name, data_points in points.items():
        if kinds[name] == "sum":
            for point in data_points:
                point["startTimeUnixNano"] = timestamp
            metrics.append(
                {
                    "name": name,
                    "sum": {
                        "dataPoints": data_points,
                        "aggregationTemporality": _AGGREGATION_TEMPORALITY_CUMULATIVE,
                        "isMonotonic": True,
                    },
                }
            )
        else:
            metrics.append({"name": name, "gauge": {"dataPoints": data_points}})

    return {
        "resourceMetrics": [
            {
                "resource": {"attributes": _attributes({"service.name": job})},
                "scopeMetrics": [{"scope": {"name": _SCOPE_NAME}, "metrics": metrics}],
            }
        ]
    }


def otlp_push_metrics(
    registry: CollectorRegistry,
    *,
    url: str | None,
    username: str | None,
    password: str | None,
    job: str,
    logger: FilteringBoundLogger,
    timeout_seconds: float = 10.0,
) -> None:
    """OTLP-push ``registry`` to ``url``. No-op (logged) when no url is set."""
    if not url:
        logger.info("metrics otlp push skipped", reason="no otlp url")
        return
    headers = {"Content-Type": "application/json"}
    if username is not None and password is not None:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    try:
        payload = _encode_metrics(registry, job=job, timestamp_ns=time.time_ns())
        response = httpx.post(
            url, json=payload, headers=headers, timeout=timeout_seconds
        )
        response.raise_for_status()
    except Exception:
        logger.exception("metrics otlp push failed", url=url)
    else:
        logger.info("metrics otlp pushed", url=url, job=job)

"""End-of-run push: skipped without a gateway, called with one, never raises."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

import data_pipeline_core.obs.gmp_push as gmp_push
from data_pipeline_core.obs.gmp_push import push_metrics
from data_pipeline_core.runtime.logging import get_logger


def test_skips_when_no_gateway() -> None:
    # No gateway → no push, no exception.
    push_metrics(CollectorRegistry(), gateway_url=None, job="demo", logger=get_logger())


def test_pushes_to_configured_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_push(gateway_url: str, **kwargs: object) -> None:
        calls.append({"gateway_url": gateway_url, **kwargs})

    monkeypatch.setattr(gmp_push, "push_to_gateway", fake_push)
    registry = CollectorRegistry()

    push_metrics(
        registry, gateway_url="http://pg:9091", job="demo", logger=get_logger()
    )

    assert len(calls) == 1
    assert calls[0]["gateway_url"] == "http://pg:9091"
    assert calls[0]["job"] == "demo"
    assert calls[0]["registry"] is registry
    assert calls[0]["grouping_key"] == {"source": "demo"}


def test_push_failure_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: object, **kwargs: object) -> None:
        raise ConnectionError("gateway down")

    monkeypatch.setattr(gmp_push, "push_to_gateway", boom)

    # Must swallow: observability never fails the run.
    push_metrics(
        CollectorRegistry(),
        gateway_url="http://pg:9091",
        job="demo",
        logger=get_logger(),
    )

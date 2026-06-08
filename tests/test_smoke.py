"""Phase-0 smoke tests: the package imports and the scaffolding works."""

from __future__ import annotations

import fakeredis
from prometheus_client import CollectorRegistry, Counter

import data_pipeline_core


def test_package_imports() -> None:
    assert data_pipeline_core.__version__


def test_fake_redis_roundtrips(fake_redis: fakeredis.FakeStrictRedis) -> None:
    fake_redis.set("k", "v")
    assert fake_redis.get("k") == b"v"


def test_metrics_registry_is_isolated(metrics_registry: CollectorRegistry) -> None:
    counter = Counter("smoke_total", "smoke counter", registry=metrics_registry)
    counter.inc()
    assert metrics_registry.get_sample_value("smoke_total") == 1.0

"""ProxyRouter: routing decisions and the disabled-by-config path."""

from __future__ import annotations

from data_pipeline_core.ingestion.ip_guard import Mode
from data_pipeline_core.ingestion.proxy import ProxyRouter


def _router(*, proxy_url: str | None, enabled: bool) -> ProxyRouter:
    return ProxyRouter(proxy_url=proxy_url, enabled=enabled, timeout_seconds=30.0)


def test_routes_in_aggressive_mode() -> None:
    router = _router(proxy_url="http://proxy:8000", enabled=True)
    assert router.enabled
    assert router.should_use(Mode.SAFE) is False
    assert router.should_use(Mode.WARNING) is False
    assert router.should_use(Mode.AGGRESSIVE) is True
    router.close()


def test_force_routes_regardless_of_mode() -> None:
    router = _router(proxy_url="http://proxy:8000", enabled=True)
    assert router.should_use(Mode.SAFE, force=True) is True
    router.close()


def test_disabled_by_config_never_routes() -> None:
    # Polytricks: proxy off → no routing even when Aggressive, client is None.
    router = _router(proxy_url="http://proxy:8000", enabled=False)
    assert router.enabled is False
    assert router.should_use(Mode.AGGRESSIVE, force=True) is False
    assert router.client is None


def test_disabled_when_no_url() -> None:
    router = _router(proxy_url=None, enabled=True)
    assert router.enabled is False
    assert router.should_use(Mode.AGGRESSIVE) is False

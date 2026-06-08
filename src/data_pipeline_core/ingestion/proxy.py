"""Proxy middleware — routes traffic through a SaaS proxy when needed.

Owns the proxied ``httpx`` client and the decision of when to use it: in
Aggressive mode (the IP guard saw high request density) or when the caller
forces it (e.g. a volatility trigger, whose detection is business logic and
stays in the project). Disabled by config — a low-frequency consumer
(Polytricks) sets ``proxy_enabled=False`` and keeps only retry/jitter.
"""

from __future__ import annotations

import httpx

from data_pipeline_core.ingestion.ip_guard import Mode


class ProxyRouter:
    """Decides when to route via the proxy and owns the proxied client."""

    def __init__(
        self,
        *,
        proxy_url: str | None,
        enabled: bool,
        timeout_seconds: float,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.enabled = bool(enabled and proxy_url)
        self._client = (
            httpx.Client(proxy=proxy_url, timeout=timeout_seconds, transport=transport)
            if self.enabled
            else None
        )

    def should_use(self, mode: Mode, *, force: bool = False) -> bool:
        if not self.enabled:
            return False
        return force or mode is Mode.AGGRESSIVE

    @property
    def client(self) -> httpx.Client | None:
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

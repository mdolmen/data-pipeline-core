"""End-of-run metrics push.

Workers are short-lived Cloud Run Jobs that exit before a scrape can reach them,
so metrics are pushed on completion. This implements the PushGateway transport
(the fallback named in ARCHITECTURE.md §8); the preferred GMP remote-write/OTLP
path is a config swap deferred until a real GCP target exists. A push never
fails the run — observability must not break ingestion.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, push_to_gateway
from structlog.typing import FilteringBoundLogger


def push_metrics(
    registry: CollectorRegistry,
    *,
    gateway_url: str | None,
    job: str,
    logger: FilteringBoundLogger,
) -> None:
    """Push ``registry`` to the gateway. No-op (logged) when none is configured."""
    if not gateway_url:
        logger.info("metrics push skipped", reason="no push gateway configured")
        return
    try:
        push_to_gateway(
            gateway_url, job=job, registry=registry, grouping_key={"source": job}
        )
    except Exception:
        logger.exception("metrics push failed", gateway=gateway_url)
    else:
        logger.info("metrics pushed", gateway=gateway_url, job=job)

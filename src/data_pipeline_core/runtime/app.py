"""``WorkerApp`` — the run loop that wraps a project's business code.

Builds config + structured logging, the resilience stack (instrumented HTTP
client + circuit breaker) handed to the source via ``ctx.http``, graceful
shutdown, and the standard metrics pushed at exit.
"""

from __future__ import annotations

import time
import uuid

from prometheus_client import CollectorRegistry

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.ingestion.http import HttpClient
from data_pipeline_core.ingestion.ip_guard import IpGuard
from data_pipeline_core.ingestion.proxy import ProxyRouter
from data_pipeline_core.obs.gmp_push import push_metrics
from data_pipeline_core.obs.metrics import StandardMetrics
from data_pipeline_core.runtime.config import Settings
from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.runtime.lifecycle import Lifecycle, handle_shutdown
from data_pipeline_core.runtime.logging import configure_logging, get_logger
from data_pipeline_core.storage.protocols import Sink, Source
from data_pipeline_core.storage.redis_cache import make_redis


class WorkerApp:
    """Wraps a ``Source`` and a ``Sink`` into a single runnable worker."""

    def __init__(
        self,
        source: Source,
        sink: Sink,
        *,
        settings: Settings | None = None,
    ) -> None:
        self._source = source
        self._sink = sink
        self._settings = settings or Settings()

    def run(self) -> int:
        """Run one ingestion pass. Returns a process exit code (0 ok, 1 failed)."""
        settings = self._settings
        source_name = self._source.name
        configure_logging(level=settings.log_level, fmt=settings.log_format)

        registry = CollectorRegistry()
        metrics = StandardMetrics(registry, source=source_name)
        breaker = CircuitBreaker(
            source_name,
            threshold=settings.circuit_breaker_threshold,
            cooldown_seconds=settings.circuit_breaker_cooldown_seconds,
            metrics=metrics,
        )
        run_id = uuid.uuid4().hex
        log = get_logger().bind(run_id=run_id, source=source_name)
        redis_client = make_redis(settings.redis_url) if settings.redis_url else None
        ip_guard = (
            IpGuard(
                source_name,
                redis_client,
                warning_at=settings.ip_guard_warning_at,
                aggressive_at=settings.ip_guard_aggressive_at,
                window_seconds=settings.ip_guard_window_seconds,
            )
            if redis_client is not None
            else None
        )
        proxy = ProxyRouter(
            proxy_url=settings.proxy_url,
            enabled=settings.proxy_enabled,
            timeout_seconds=settings.http_timeout_seconds,
        )
        http = HttpClient(
            source_name,
            settings=settings,
            breaker=breaker,
            metrics=metrics,
            ip_guard=ip_guard,
            proxy=proxy,
            logger=log,
        )
        lifecycle = Lifecycle()
        ctx = RunContext.create(
            source_name=source_name,
            http=http,
            run_id=run_id,
            logger=log,
            should_stop=lambda: lifecycle.should_stop,
        )

        started = time.monotonic()
        with handle_shutdown(lifecycle, log):
            log.info("worker starting")
            exit_code = 0
            try:
                result = self._sink.write(self._source.fetch(ctx))
            except Exception:
                log.exception("worker failed")
                metrics.worker_up.labels(source=source_name).set(0)
                exit_code = 1
            else:
                metrics.worker_up.labels(source=source_name).set(1)
                metrics.ingestion_lag_seconds.labels(source=source_name).set(0)
                log.info("worker finished", row_count=result.row_count)
            finally:
                elapsed = time.monotonic() - started
                requests = http.request_count
                rate = requests / elapsed if elapsed > 0 else 0.0
                metrics.request_rate.labels(source=source_name).set(rate)
                ratio = http.proxied_count / requests if requests > 0 else 0.0
                metrics.proxy_usage_ratio.labels(source=source_name).set(ratio)
                http.close()
                proxy.close()
                if redis_client is not None:
                    redis_client.close()
                push_metrics(
                    registry,
                    gateway_url=settings.metrics_push_gateway,
                    job=source_name,
                    logger=log,
                )
            return exit_code

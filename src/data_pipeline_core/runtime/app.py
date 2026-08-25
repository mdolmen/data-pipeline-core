"""``WorkerApp`` — the run loop that wraps a project's business code.

Builds config + structured logging, the resilience stack (instrumented HTTP
client + circuit breaker) handed to the source via ``ctx.http``, graceful
shutdown, and the standard metrics pushed at exit. An optional ``transform`` is
applied in-process between ``fetch`` and ``write``; the same loop also runs a
dedicated transform worker (raw-landing source → transform → curated sink).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Generic, TypeVar, overload

from prometheus_client import CollectorRegistry

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.ingestion.http import HttpClient
from data_pipeline_core.ingestion.ip_guard import IpGuard
from data_pipeline_core.ingestion.proxy import ProxyRouter
from data_pipeline_core.obs.gmp_push import push_metrics
from data_pipeline_core.obs.metrics import StandardMetrics
from data_pipeline_core.obs.otlp_push import otlp_push_metrics
from data_pipeline_core.runtime.config import Settings
from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.runtime.lifecycle import Lifecycle, handle_shutdown
from data_pipeline_core.runtime.logging import configure_logging, get_logger
from data_pipeline_core.storage.protocols import Sink, Source, Transform
from data_pipeline_core.storage.redis_cache import make_redis

# The record types the worker moves: ``RecordT`` out of the source, ``RecordU``
# into the sink. Invariant — the app both accepts and produces each — and equal
# to each other when no transform is wired (see the ``__init__`` overloads).
RecordT = TypeVar("RecordT", bound=Mapping[str, object])
RecordU = TypeVar("RecordU", bound=Mapping[str, object])


class WorkerApp(Generic[RecordT, RecordU]):
    """Wraps a ``Source``, optional ``Transform`` and ``Sink`` into a worker."""

    @overload
    def __init__(
        self: WorkerApp[RecordT, RecordT],
        source: Source[RecordT],
        sink: Sink[RecordT],
        *,
        transform: None = None,
        settings: Settings | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        source: Source[RecordT],
        sink: Sink[RecordU],
        *,
        transform: Transform[RecordT, RecordU],
        settings: Settings | None = None,
    ) -> None: ...

    def __init__(
        self,
        source: Source[RecordT],
        sink: Sink[Any],
        *,
        transform: Transform[RecordT, RecordU] | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._source = source
        self._sink = sink
        self._transform = transform
        self._settings = settings or Settings()

    def run(self) -> int:
        """Run one pass. Returns a process exit code (0 ok, 1 failed)."""
        settings = self._settings
        source_name = self._source.name
        configure_logging(level=settings.log_level, fmt=settings.log_format)

        registry = CollectorRegistry()
        metrics = StandardMetrics(registry, source=source_name, stage=settings.stage)
        run_id = uuid.uuid4().hex
        log = get_logger().bind(run_id=run_id, source=source_name, stage=settings.stage)
        redis_client = make_redis(settings.redis_url) if settings.redis_url else None
        breaker = CircuitBreaker(
            source_name,
            threshold=settings.circuit_breaker_threshold,
            cooldown_seconds=settings.circuit_breaker_cooldown_seconds,
            metrics=metrics,
            client=redis_client,  # None → per-run in-memory state
        )
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
            impersonate=settings.impersonate,
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
                # ``Iterable[Any]`` here only because the two wirings share one
                # variable; the chain itself is checked at construction by the
                # ``__init__`` overloads.
                records: Iterable[Any] = self._source.fetch(ctx)
                if self._transform is not None:
                    records = self._apply_transform(records, ctx)
                result = self._sink.write(records)
            except Exception:
                log.exception("worker failed")
                metrics.set_worker_up(False)
                exit_code = 1
            else:
                metrics.set_worker_up(True)
                metrics.set_ingestion_lag(0)
                metrics.observe_records_written(result.row_count)
                if result.byte_count is not None:
                    metrics.observe_bytes_written(result.byte_count)
                log.info("worker finished", row_count=result.row_count)
            finally:
                metrics.observe_run_finished(success=exit_code == 0)
                elapsed = time.monotonic() - started
                requests = http.request_count
                metrics.set_request_rate(requests / elapsed if elapsed > 0 else 0.0)
                metrics.set_proxy_usage_ratio(
                    http.proxied_count / requests if requests > 0 else 0.0
                )
                http.close()
                proxy.close()
                if redis_client is not None:
                    redis_client.close()
                if settings.metrics_otlp_url:
                    otlp_push_metrics(
                        registry,
                        url=settings.metrics_otlp_url,
                        username=settings.metrics_otlp_username,
                        password=settings.metrics_otlp_password,
                        job=source_name,
                        logger=log,
                    )
                else:
                    push_metrics(
                        registry,
                        gateway_url=settings.metrics_push_gateway,
                        job=source_name,
                        logger=log,
                    )
            return exit_code

    def _apply_transform(
        self, records: Iterable[RecordT], ctx: RunContext
    ) -> Iterator[RecordU]:
        assert self._transform is not None
        for record in records:
            yield from self._transform.transform(record, ctx)

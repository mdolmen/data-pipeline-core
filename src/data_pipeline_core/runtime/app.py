"""``WorkerApp`` — the run loop that wraps a project's business code.

Builds config + structured logging, installs graceful-shutdown handling, runs
the source→sink pass, and pushes the standard metrics at exit. Resilience is
wired into this same loop by later phases.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from data_pipeline_core.obs.gmp_push import push_metrics
from data_pipeline_core.obs.metrics import StandardMetrics
from data_pipeline_core.runtime.config import Settings
from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.runtime.lifecycle import Lifecycle, handle_shutdown
from data_pipeline_core.runtime.logging import configure_logging
from data_pipeline_core.storage.protocols import Sink, Source


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
        lifecycle = Lifecycle()
        ctx = RunContext.create(
            source_name=source_name,
            should_stop=lambda: lifecycle.should_stop,
        )
        log = ctx.logger

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
                push_metrics(
                    registry,
                    gateway_url=settings.metrics_push_gateway,
                    job=source_name,
                    logger=log,
                )
            return exit_code

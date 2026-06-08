"""``WorkerApp`` — the run loop that wraps a project's business code.

Builds config + structured logging, installs graceful-shutdown handling, then
streams ``source.fetch`` into ``sink.write`` and returns a clean exit code.
Resilience and observability are wired into this same loop by later phases.
"""

from __future__ import annotations

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
        configure_logging(level=self._settings.log_level, fmt=self._settings.log_format)
        lifecycle = Lifecycle()
        ctx = RunContext.create(
            source_name=self._source.name,
            should_stop=lambda: lifecycle.should_stop,
        )
        log = ctx.logger
        with handle_shutdown(lifecycle, log):
            log.info("worker starting")
            try:
                result = self._sink.write(self._source.fetch(ctx))
            except Exception:
                log.exception("worker failed")
                return 1
            log.info("worker finished", row_count=result.row_count)
            return 0

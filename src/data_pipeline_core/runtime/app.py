"""``WorkerApp`` — the run loop that wraps a project's business code.

Phase 1 is the spine only: build a ``RunContext``, stream ``source.fetch`` into
``sink.write``, return a clean exit code. Resilience, observability and config
are wired into this same loop by later phases.
"""

from __future__ import annotations

from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.storage.protocols import Sink, Source


class WorkerApp:
    """Wraps a ``Source`` and a ``Sink`` into a single runnable worker."""

    def __init__(self, source: Source, sink: Sink) -> None:
        self._source = source
        self._sink = sink

    def run(self) -> int:
        """Run one ingestion pass. Returns a process exit code (0 ok, 1 failed)."""
        ctx = RunContext.create(source_name=self._source.name)
        log = ctx.logger
        log.info("worker starting", extra={"run_id": ctx.run_id})
        try:
            result = self._sink.write(self._source.fetch(ctx))
        except Exception:
            log.exception("worker failed", extra={"run_id": ctx.run_id})
            return 1
        log.info(
            "worker finished",
            extra={"run_id": ctx.run_id, "row_count": result.row_count},
        )
        return 0

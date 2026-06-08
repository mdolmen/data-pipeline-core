"""``RunContext`` — the per-run handle the framework passes to ``Source.fetch``.

Carries a run id, a bound structured logger, a clock, a cooperative
``should_stop`` check (set on SIGTERM), and the instrumented ``http`` client the
source makes outbound calls through.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from structlog.typing import FilteringBoundLogger

from data_pipeline_core.runtime.logging import get_logger

if TYPE_CHECKING:
    from data_pipeline_core.ingestion.http import HttpClient


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RunContext:
    """Carried through a single worker run."""

    run_id: str
    logger: FilteringBoundLogger
    clock: Callable[[], datetime]
    should_stop: Callable[[], bool]
    http: HttpClient

    @classmethod
    def create(
        cls,
        *,
        source_name: str,
        http: HttpClient,
        run_id: str | None = None,
        logger: FilteringBoundLogger | None = None,
        clock: Callable[[], datetime] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> RunContext:
        """Build a context, defaulting the run id, logger and clock."""
        run_id = run_id or uuid.uuid4().hex
        return cls(
            run_id=run_id,
            logger=logger or get_logger().bind(run_id=run_id, source=source_name),
            clock=clock or _utc_now,
            should_stop=should_stop or (lambda: False),
            http=http,
        )

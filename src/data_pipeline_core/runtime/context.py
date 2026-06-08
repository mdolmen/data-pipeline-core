"""``RunContext`` — the per-run handle the framework passes to ``Source.fetch``.

Carries a run id, a bound structured logger, a clock, and a cooperative
``should_stop`` check (set on SIGTERM). Config and Redis are layered in by later
phases without changing this surface.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from structlog.typing import FilteringBoundLogger

from data_pipeline_core.runtime.logging import get_logger


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RunContext:
    """Carried through a single worker run."""

    run_id: str
    logger: FilteringBoundLogger
    clock: Callable[[], datetime]
    should_stop: Callable[[], bool]

    @classmethod
    def create(
        cls,
        *,
        source_name: str,
        clock: Callable[[], datetime] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> RunContext:
        """Build a context with a fresh run id and a source-scoped bound logger."""
        run_id = uuid.uuid4().hex
        return cls(
            run_id=run_id,
            logger=get_logger().bind(run_id=run_id, source=source_name),
            clock=clock or _utc_now,
            should_stop=should_stop or (lambda: False),
        )

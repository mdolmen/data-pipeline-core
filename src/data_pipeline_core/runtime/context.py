"""``RunContext`` — the per-run handle the framework passes to ``Source.fetch``.

Phase 1 keeps it minimal: a run id, a logger, and a clock. Config, Redis and
structured logging are layered in by later phases without changing this surface.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RunContext:
    """Carried through a single worker run."""

    run_id: str
    logger: logging.Logger
    clock: Callable[[], datetime]

    @classmethod
    def create(
        cls,
        *,
        source_name: str,
        clock: Callable[[], datetime] | None = None,
    ) -> RunContext:
        """Build a context with a fresh run id and a source-scoped logger."""
        return cls(
            run_id=uuid.uuid4().hex,
            logger=logging.getLogger(f"data_pipeline_core.{source_name}"),
            clock=clock or _utc_now,
        )

"""Structured logging setup (structlog).

Workers are short-lived Cloud Run Jobs, so logs go to stdout as one JSON object
per line (picked up by Cloud Logging); ``console`` is offered for local dev.
``run_id`` / ``source`` are bound once on the context logger and ride every
event.
"""

from __future__ import annotations

import logging
from typing import cast

import structlog
from structlog.typing import FilteringBoundLogger


def configure_logging(*, level: str = "INFO", fmt: str = "json") -> None:
    """Configure the process-wide structlog pipeline. Called once per run."""
    level_no = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level_no),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(**initial_values: object) -> FilteringBoundLogger:
    """A bound logger; keyword args become permanent context on every event."""
    return cast(FilteringBoundLogger, structlog.get_logger(**initial_values))

"""Graceful shutdown for preemptible Cloud Run Jobs.

A SIGTERM flips a shared flag rather than killing the process mid-write; the run
loop exposes it as ``RunContext.should_stop`` so a polling ``Source`` can finish
its current item and stop cleanly. The handler is installed only for the
duration of a run and restored on exit.
"""

from __future__ import annotations

import signal
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event

from structlog.typing import FilteringBoundLogger


@dataclass
class Lifecycle:
    """Shared shutdown state for a single run."""

    _event: Event = field(default_factory=Event)

    @property
    def should_stop(self) -> bool:
        return self._event.is_set()

    def request_stop(self) -> None:
        self._event.set()


@contextmanager
def handle_shutdown(
    lifecycle: Lifecycle, logger: FilteringBoundLogger
) -> Iterator[Lifecycle]:
    """Route SIGTERM to ``lifecycle.request_stop`` for the duration of the block."""

    def _on_sigterm(signum: int, frame: object) -> None:
        logger.warning("shutdown requested", signal=signal.Signals(signum).name)
        lifecycle.request_stop()

    previous = signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        yield lifecycle
    finally:
        signal.signal(signal.SIGTERM, previous)

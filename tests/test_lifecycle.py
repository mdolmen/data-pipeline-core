"""SIGTERM flips the shutdown flag and the handler is restored afterwards."""

from __future__ import annotations

import signal

from data_pipeline_core.runtime.lifecycle import Lifecycle, handle_shutdown
from data_pipeline_core.runtime.logging import get_logger


def test_sigterm_requests_stop_and_restores_handler() -> None:
    lifecycle = Lifecycle()
    previous = signal.getsignal(signal.SIGTERM)
    assert not lifecycle.should_stop

    with handle_shutdown(lifecycle, get_logger()):
        signal.raise_signal(signal.SIGTERM)
        assert lifecycle.should_stop

    assert signal.getsignal(signal.SIGTERM) == previous

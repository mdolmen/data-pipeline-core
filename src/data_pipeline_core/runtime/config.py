"""``Settings`` — the SDK's generic runtime configuration.

Holds only plumbing knobs that every worker shares (logging). Projects subclass
this to add their own fields (dataset names, thresholds, …) and their own env
prefix; values come from the environment / a ``.env`` file, with GCP Secret
Manager layered in by a later phase. Project-specific defaults live in the
subclass, never as hard-coded SDK constants.
"""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    # End-of-run metrics push. None → push is skipped (logged). A real
    # deployment sets a PushGateway URL; GMP remote-write/OTLP is a later swap.
    metrics_push_gateway: str | None = None

    # HTTP client (defaults betting-tuned, overridable per project).
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 3
    http_backoff_base_seconds: float = 0.5
    http_user_agents: tuple[str, ...] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    )

    # Circuit breaker: open after N consecutive 429s, halt for the cooldown.
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown_seconds: float = 900.0

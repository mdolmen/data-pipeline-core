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

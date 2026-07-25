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

    # Worker role in the topology, used as the `stage` metric label so ingest
    # and transform workers are distinguishable on shared dashboards.
    stage: str = "ingest"

    # Raw-landing staging boundary (ingest writes here, transform reads here).
    raw_bucket_url: str | None = None

    # End-of-run metrics push. None → push is skipped (logged). A real
    # deployment sets a PushGateway URL; the preferred path is remote-write below.
    metrics_push_gateway: str | None = None

    # Prometheus remote-write (the preferred path for short-lived jobs — the worker
    # writes its final series straight to a TSDB at exit, no PushGateway/scraper).
    # url set → remote-write is used instead of the PushGateway. username/password
    # are HTTP basic-auth (e.g. Grafana Cloud: username = instance id, password =
    # an API token). The token comes from the environment/Secret Manager, never here.
    metrics_remote_write_url: str | None = None
    metrics_remote_write_username: str | None = None
    metrics_remote_write_password: str | None = None

    # HTTP client (defaults betting-tuned, overridable per project).
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 3
    http_backoff_base_seconds: float = 0.5
    # Browser-TLS impersonation for anti-bot targets (JA3/JA4 fingerprinting).
    # None → standard httpx; a curl_cffi profile (e.g. "chrome") → browser TLS.
    impersonate: str | None = None
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

    # IP guard & proxy. redis_url None → guard disabled (always Safe). proxy
    # disabled by default-off config (Polytricks) keeps only retry/jitter.
    redis_url: str | None = None
    proxy_url: str | None = None
    proxy_enabled: bool = True
    ip_guard_warning_at: int = 300
    ip_guard_aggressive_at: int = 500
    ip_guard_window_seconds: int = 3600
    warning_jitter_seconds: float = 0.5

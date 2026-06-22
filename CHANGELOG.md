# Changelog

All notable changes to `data-pipeline-core` are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/). During
co-development the SDK stays in `0.x`; breaking changes are free until `v0.1.0`
is cut.

## [Unreleased] — `0.1.0-dev`

### Added

- **Read-until-predicate with instant abort (`HttpClient.read_until`).** Streams
  a request and returns the body the moment a caller predicate (`until(buffer)`)
  is satisfied, under the same breaker / IP-guard / proxy guards as `request` (no
  retry — a stream can't be replayed). On the curl_cffi backend it drives curl at
  the low level and aborts from inside the write callback
  (`CURL_WRITEFUNC_ERROR`) the instant the predicate trips, so a never-closing
  server stream is torn down immediately; on httpx it stops iterating and closes
  the context. Surfaced by betclic's `…WithNotifications` odds endpoint: the
  snapshot is the first gRPC-web frame and arrives in ~100 ms, but the stream
  never closes — a graceful close blocked ~30–40 s (curl_cffi only checks its
  stop flag on the *next* write callback, which an idle stream never delivers),
  so the consumer reads the first frame and aborts, the browser's own behaviour.
  On this low-level path the caller owns the full header set (browser identity
  included); curl owns `Accept-Encoding` so it still decompresses.
- **Browser-TLS impersonation (`curl_cffi`).** New `impersonate` setting (e.g.
  `"chrome"`): when set, the HTTP client swaps `httpx` for `curl_cffi` so the
  TLS/HTTP2 handshake matches a real browser, defeating JA3/JA4 fingerprinting
  (DataDome/Akamai) that a proxy can't — the fingerprint, not the IP, is what's
  blocked. `ingestion/impersonation.py` wraps it behind an httpx-shaped shim
  (raises `httpx.TransportError` on failure), so retry/breaker/proxy handling is
  unchanged; both the direct and proxied clients honour `impersonate`. Surfaced
  by `proba-markets-analysis`: betclic.fr blocked httpx but returned 200 to curl
  from the same IP.
- **Phase 7 — storage maturity (GCS-only + optional Redis hot tier).**
  `storage/ids.py` — `deterministic_id(*parts)` for stable record ids.
  `dlt_sink` gains `primary_key` → idempotent dlt `merge`; on the filesystem
  destination it auto-selects **Delta Lake**, giving ACID upsert on plain GCS
  objects with no database service. `redis_latest_sink` upserts each record as
  the latest snapshot per key — the optional hot store, idempotent by
  construction. `make_redis` is exported at the top level. `dlt[deltalake]` is
  a runtime dep. **Scope change (user decision):** Postgres/Cloud SQL is dropped
  in favour of Delta-on-GCS for cold idempotency and Redis for hot state.
- **Phase 6 — transform & staging.** `Transform` protocol
  (`transform(record, ctx) -> Iterable[Record]`) and an optional `transform=`
  slot on `WorkerApp`, applied in-process (streamed) between fetch and write.
  `storage/staging.py` — `raw_landing_sink` / `raw_landing_source` over fsspec
  (`file://` dev, `gs://` cloud): immutable, replayable JSONL handoff between an
  ingest worker and a transform worker. Every metric series now carries a
  `stage` label (ingest vs transform); the `(source, stage)` label set is owned
  by `StandardMetrics` and applied through semantic methods (the breaker and
  HTTP client no longer touch labels). New `Settings`: `stage`, `raw_bucket_url`.
- **Phase 5 — IP guard & proxy.** `ingestion/ip_guard.py` — a per-source Redis
  sliding-window counter classifying request density into Safe / Warning /
  Aggressive (thresholds config defaults <300 / 300-500 / >=500 req/hr).
  `ingestion/proxy.py` — `ProxyRouter` routes via the SaaS proxy in Aggressive
  mode or on a forced trigger, disabled by config (Polytricks). `HttpClient`
  consults the guard per request (extra jitter in Warning, proxy in Aggressive →
  `proxy_usage_ratio`). `storage/redis_cache.py` — `make_redis` + latest-state
  `RedisCache`. New `Settings`: `redis_url`, `proxy_url`, `proxy_enabled`, guard
  thresholds/window, `warning_jitter_seconds`. `redis` is now a runtime dep.
- **Phase 4 — resilience.** `ingestion/http.py` — an instrumented httpx client
  (retry with full-jitter backoff on network/5xx, User-Agent rotation, per-status
  metrics, request counting → `request_rate`) exposed to the source as
  `ctx.http`; a 429 feeds the breaker (not retried) and an open breaker raises
  `CircuitOpenError`. `ingestion/circuit_breaker.py` — per-source breaker that
  opens after N consecutive 429s and halts for a cooldown (default 15 min),
  flipping `circuit_breaker_state`. `WorkerApp` builds and wires both. New
  `Settings`: HTTP timeout/retries/backoff/UA list, breaker threshold/cooldown.
- **Phase 3 — observability.** `obs/metrics.py` declares the standard series
  (`worker_up`, `request_rate`, `http_status_total{code}`,
  `ingestion_lag_seconds`, `circuit_breaker_state`, `proxy_usage_ratio`) with
  fixed names + labels — the stable surface Grafana depends on. `obs/gmp_push.py`
  pushes the run's registry at exit (PushGateway transport) and never fails the
  run. `WorkerApp` builds a per-run registry, sets `worker_up`, and pushes in a
  `finally`. `prometheus-client` is now a runtime dependency. `Settings` gains
  `metrics_push_gateway`.
- **Phase 2 — config & structured logging.** `Settings` (pydantic-settings)
  for generic runtime config (log level/format), env/`.env` driven and meant to
  be subclassed per project. `structlog` JSON logging to stdout (console option
  for dev) with `run_id`/`source` bound. `lifecycle` SIGTERM handling exposed as
  `RunContext.should_stop` for cooperative graceful shutdown. `WorkerApp` takes
  an optional `settings=` and configures all three. New runtime deps:
  `pydantic-settings`, `structlog`.
- **Phase 1 — walking skeleton.** The core contract
  (`Source` / `Sink` / `Record` / `WriteResult`), a minimal `RunContext`
  (run_id, logger, clock), `WorkerApp(source, sink).run()` streaming
  `fetch()` → `write()` with a clean exit code, and `dlt_sink(dataset,
  destination)` loading records as parquet (filesystem / GCS). Public API is
  exported from `data_pipeline_core`. Validated by a real betting worker
  (Betclic Ligue 1) landing parquet end-to-end.
- **Phase 0 — repo & tooling.** uv + hatchling packaging (`src/` layout),
  `ruff` (lint + format), `mypy --strict`, `pytest`, `pre-commit`, and a CI
  workflow (lint → type → test). Test scaffolding (`conftest.py`): `fake_redis`
  (fakeredis), `metrics_registry` (isolated Prometheus registry), and the
  `httpx_mock` fixture from pytest-httpx. The package is a hollow but importable
  scaffold; the public contract arrives in Phase 1.

### Fixed

- **PEP 561 `py.typed` marker** added to the package, so consumers see the
  SDK's inline type hints instead of treating it as untyped. Surfaced by
  `proba-markets-analysis` enabling `mypy --strict`: without the marker, every
  `from data_pipeline_core import ...` raised `import-untyped`, cascading into
  spurious subclass-`Any` and `no-any-return` errors downstream.

## Decision log

What went **into the SDK**, what stayed **in the consuming project**, and what is
**deferred** (generalize on the 2nd real usage). This briefs the future
Polytricks instance — see `TODO.md`.

| Item | Decision | Rationale |
|---|---|---|
| Runtime dependencies | Deferred — added per phase | Tracer-bullet, bottom-up; avoid speculative weight. The package ships with only what a real consumer demands (e.g. `dlt` in Phase 1, `prometheus-client` in Phase 3). |
| `prometheus-client` | SDK, runtime dep (Phase 3) | Promoted from dev once `obs/metrics.py` shipped, as planned. |
| Standard metric series (names + labels) | SDK, frozen surface | Shared Grafana dashboards across consumers; renaming/relabeling is forbidden. Series whose mechanism is unbuilt are declared at 0. |
| Metrics push transport | PushGateway built; GMP remote-write/OTLP deferred | The preferred transport needs a real GCP target to verify; the push-at-exit mechanism is generic, the transport is config. |
| Push failures | Swallowed (logged), never fail the run | Observability must not break ingestion. |
| HTTP client + circuit breaker | SDK, on `ctx.http` | Generic ingestion plumbing; the `fetch()` that uses it stays in the project. |
| Retry/backoff/UA list, breaker threshold/cooldown | SDK, config defaults | Mechanism generic, values betting-tuned and overridable (Polytricks can loosen them). |
| 429 handling | Recorded with breaker, not retried | Retrying a rate-limit signal worsens it; the breaker is the correct response. |
| `CircuitOpenError` on open breaker | SDK raises; source decides | The project's `fetch()` owns the loop, so it chooses to stop/partial-yield — the SDK doesn't impose a policy. |
| Breaker cross-run persistence | Deferred to Phase 5 (Redis) | In-memory per run is enough to validate the mechanism; surviving restarts needs the Redis store. |
| IP guard + proxy router | SDK, config-gated | Generic density→mode→proxy mechanism; thresholds are config defaults (betting-tuned). Disabled when `redis_url`/`proxy` unset → Polytricks keeps retry/jitter only. |
| Volatility trigger | SDK exposes a `force_proxy` hook; detection stays in betting | Forcing the proxy is generic; computing volatility (L2-norm of probability movement) is business logic. |
| `RedisCache` latest-state | SDK, provided not yet consumed | Generic snapshot store; betting wires it for latest odds in a later phase (recorded so it isn't re-invented). |
| Sliding window = fixed hourly buckets | SDK | Matches ARCHITECTURE §6.A key structure (`ratelimit:{source}:{bucket}`, 2× TTL); simpler than a sorted-set true window and sufficient for req/hr thresholds. |
| `Transform` slot + staging boundary | SDK | Generic two-archetype topology; the `transform()` logic (margin removal) stays in betting. The split is wiring, not a fork — a single `source+transform+sink` worker is unchanged. |
| Margin removal / odds normalization | Stays in betting | Pure business logic over the generic `Transform` slot. |
| Raw-landing transport | fsspec (`file://`/`gs://`); Pub/Sub deferred | GCS-raw covers replayable handoff and is verifiable locally; Pub/Sub needs GCP. |
| `stage` label across all series | SDK, frozen surface (added pre-deploy) | Distinguishes ingest vs transform on shared dashboards; added now while no dashboards are deployed, so not a breaking relabel. |
| Hot store: Delta-on-GCS + Redis, **not Postgres** | SDK; user decision | GCS-only keeps cost/ops minimal (no always-on Cloud SQL). Delta gives ACID merge on objects for cold idempotency; Redis covers latest-state. Postgres/BigQuery remain reachable via dlt `destination=` if a consumer ever needs SQL serving. |
| `deterministic_id` | SDK, generic | The hash is generic; which fields identify a record is business logic (betting passes bookmaker/match/market/time). |
| Idempotency model | Replay-of-raw, not re-fetch | Re-running a transform over immutable raw upserts (same ids); a fresh fetch is a legitimately new tick. Matches the staging/replay design. |
| `dlt` (load layer) | SDK, runtime dep | Generic schema-inference + parquet load behind `dlt_sink`; the canonical/business schema stays in the project. |
| Destination (`file://` vs `gs://`) | SDK, config-driven | One `dlt_sink`; local vs GCS is dlt config (`DESTINATION__FILESYSTEM__BUCKET_URL`), not a code fork. |
| `WorkerApp(source, sink)` signature | SDK, minimal | `transform=` / `settings=` are deferred to their phases (6 / 2) as non-breaking optional params, not added speculatively. |
| Betting odds schema / `bookmaker="betclic"` | Stays in betting | Business data model and constants live in the consumer, never the SDK. |
| `Settings` base (log level/format) | SDK, subclassed per project | Generic plumbing knobs only; project fields (dataset, table, prefix) live in the consumer's `WorkerSettings`. |
| GCP Secret Manager source | Deferred | env/`.env` covers co-dev; add the secrets source once a real deployment needs it (avoid speculative cloud coupling). |
| `RunContext.should_stop` | SDK | Generic cooperative-shutdown signal; the betting poll loop will consult it, but the mechanism is project-agnostic. |
| Single-work-unit run (granularity) | SDK, deferred (infra v2) | A worker can run for one source work unit per invocation (e.g. a single competition) instead of looping all, selected by config. Prereq for `data-pipeline-infra` v2 per-unit Cloud Tasks fan-out; v1 uses the whole-source loop. The unit is consumer-defined; the targeting mechanism is generic. |
| Scheduling hint (`next_run_seconds`) | SDK, deferred (infra v2) | An optional typed value a worker emits at end of run (`RunContext.request_next_run(...)` or returned from `run()`) so infra self-paces the next per-unit invocation (Cloud Tasks `schedule_time`). Narrow-waist business→infra contract: one clamped scalar, infra enforces `[min,max]`, the consumer computes it. Deferred like Pub/Sub — declare the seam, build the emitter when v2 lands. |
| Destination bucket URL | Stays dlt-native config | Kept as `DESTINATION__FILESYSTEM__BUCKET_URL` rather than mirrored into `Settings` — no duplicate source of truth. |

# data-pipeline-core — Build TODO

Tasks only. Strategy, phase goals, workflow and the decision log live in `DEVELOPMENT.md`.

## Phase 0 — Repo & tooling setup

- [x] `pyproject.toml` (uv + hatch build backend), `src/data_pipeline_core/` layout
- [x] Dev tooling: `ruff`, `mypy --strict`, `pytest`, `pre-commit`
- [x] CI skeleton (GitHub Actions): lint + type + test on push
- [x] `README.md`, `CHANGELOG.md` (start at `0.1.0-dev`), `.gitignore`
- [x] Test scaffolding: `conftest.py` with `fakeredis`, httpx mock, isolated Prometheus registry
- [x] **Betting:** init `proba-markets-analysis` Python env, wire the editable path dependency, confirm `import data_pipeline_core` works

## Phase 1 — Walking skeleton (the tracer bullet)

- [x] `storage/protocols.py` — `Source`, `Sink`, `Record`, `WriteResult` Protocols
- [x] `runtime/context.py` — minimal `RunContext` (run_id, logger, clock)
- [x] `runtime/app.py` — minimal `WorkerApp(source, sink).run()`: call `source.fetch(ctx)`, stream to `sink.write()`, return exit code
- [x] `storage/dlt_sink.py` — `dlt_sink(dataset, destination)` for filesystem/GCS parquet (lean on dlt for schema inference + load)
- [x] **Betting:** implement one real `Source` (one bookmaker, one market), `workers/main.py` = `WorkerApp(source, sink).run()`, observe Parquet land in GCS — validates the core contract _(verified locally against a `file://` bucket; `gs://` is the same code by config)_

## Phase 2 — Config & structured logging

- [x] `runtime/config.py` — `Settings` via `pydantic-settings` (env + GCP secrets) _(env/.env now; GCP Secret Manager deferred)_
- [x] Structured logging (`structlog`): JSON output, run_id/source context bound
- [x] `runtime/lifecycle.py` — SIGTERM handling, graceful shutdown (Cloud Run Jobs)
- [x] **Betting:** move the worker's config to `Settings`; logs are structured JSON

## Phase 3 — Observability

- [x] `obs/metrics.py` — standard series with stable labels: `worker_up`, `request_rate`, `http_status_total{code}`, `ingestion_lag_seconds`, `circuit_breaker_state{source}`, `proxy_usage_ratio`
- [x] `obs/metrics.py` — technical-dashboard series: `worker_runs_total{status}`, `records_written_total`, `bytes_written_total`
- [x] `storage/protocols.py` — `WriteResult` grows an optional `byte_count` (raw-landing reports it; dlt reports rows only)
- [x] `obs/gmp_push.py` — end-of-run push via remote-write/OTLP with PushGateway fallback _(PushGateway transport built; GMP remote-write deferred to a real GCP target)_
- [x] `obs/otlp_push.py` — OTLP/HTTP-JSON push (preferred path): final series written straight to the OTLP backend at exit, no PushGateway/scraper
- [x] `WorkerApp.run()` wires metrics + push automatically (OTLP when `metrics_otlp_url` is set, else PushGateway)
- [x] **Betting:** metrics appear in GMP/Grafana Cloud; first dashboard panel live _(verified locally with the `source` label and `worker_up=1`; the cloud panel is pending access)_

## Phase 4 — Resilience

- [x] `ingestion/http.py` — instrumented httpx client: retry, jitter, UA rotation
- [x] `ingestion/circuit_breaker.py` — per-source breaker, configurable halt (default 15 min) on repeated 429s _(cross-run persistence added in Fixes; in-memory when no `redis_url`)_
- [x] `WorkerApp` integrates breaker + http client into the run loop (exposed to the source as `ctx.http`)
- [x] **Betting:** worker survives an injected 429 storm; breaker state metric flips

## Phase 5 — IP guard & proxy

- [x] `ingestion/ip_guard.py` — Redis sliding-window counter, Safe/Warning/Aggressive mode switch, thresholds as config (defaults <300 / 300–500 / ≥500 req/hr)
- [x] `ingestion/proxy.py` — proxy SaaS middleware, activated by ip_guard or volatility trigger _(forced-routing hook; volatility detection stays in the project)_
- [x] `storage/redis_cache.py` — latest-state cache + counter backing
- [x] **Betting:** simulate >500 req/hr → traffic routes through proxy, `proxy_usage_ratio` rises
- [x] **Polytricks check:** proxy can be disabled by config, retry/jitter kept _(SDK-tested)_

## Phase 6 — Transform & staging decoupling

- [x] `storage/protocols.py` — add the `Transform` protocol (`transform(record, ctx) -> Iterable[Record]`)
- [x] `runtime/app.py` — optional `transform=` slot applied between `fetch()` and `write()` (in-process transform path)
- [x] `storage/staging.py` — raw-landing `Sink` + matching `Source` over the staging boundary (GCS raw bucket via fsspec; Pub/Sub variant deferred)
- [x] `obs/metrics.py` — standard series carry a `stage` label so ingest vs transform workers are distinguishable
- [x] **Betting (in-process):** add the normalization `Transform` (margin removal) to the worker; raw → curated in a single run
- [x] **Betting (decoupled):** split into an ingest worker and a transform worker; confirm raw is replayable _(verified via CLI: raw JSONL lands, transform replays it twice)_
- [x] **Polytricks check:** a single worker wired `source + transform + sink` works unchanged

## Phase 7 — Storage maturity

- [x] `storage/ids.py` — deterministic hash for dedup / idempotent writes
- [x] dlt sink: idempotent merge (hot state) alongside GCS (cold)
- [x] Idempotency: re-running a worker doesn't duplicate rows
- [x] **Betting:** odds upsert to Redis (optional) + cold Delta to GCS, both idempotent _(2 transform replays → curated stays at 3 rows; Redis keeps one snapshot per match)_

## Fixes — correctness defects (priority order; blocking v0.1.0)

- [x] `ingestion/impersonation.py` — declare `is_success` on the `Response` protocol
- [x] `ingestion/http.py` — count guard token, `request_count` and `proxied_count` per attempt, not per call
- [x] `ingestion/http.py` — record transport errors as `http_status_total{code="transport_error"}`
- [x] `ingestion/circuit_breaker.py` — Redis-backed state so the cooldown survives the process
- [x] `storage/ids.py` — hash lone surrogates instead of raising _(found by hypothesis; no id migration)_
- [x] **Betting:** `mypy --strict` clean against the fixed `Response`; injected 429 storm stays halted across two runs

## Phase 8 — Hardening & first release

- [x] Test coverage on SDK (unit + the fakeredis/httpx-mock integration paths)
- [x] `mypy --strict` clean; public API typed and exported from `__init__.py`
- [x] Docstrings + README usage example (the `WorkerApp(...).run()` pattern)
- [ ] Build & publish **v0.1.0** to private Artifact Registry _(no publish job in `ci.yml` yet — the only Phase 8 item still open)_

---

## Backlog — deferred follow-ups

- [ ] **Pub/Sub staging variant** — `raw_landing_sink` / `raw_landing_source` over Pub/Sub, alongside the fsspec JSONL handoff; deferred until a real GCP target exists
- [ ] **Always-on transform archetype** — long-running subscriber service (vs the one-shot Cloud Run Job); build only when a consumer needs continuous processing
- [ ] **Single-work-unit run (granularity)** — run for one source work unit per invocation, selected by config/env, instead of looping all units _(prereq for `data-pipeline-infra` v2)_
- [ ] **Scheduling hint (`next_run_seconds`)** — optional clamped scalar emitted at end of run so infra self-paces the next invocation per unit _(prereq for `data-pipeline-infra` v2)_

---

## Definition of done — cut v0.1.0 only when ALL hold

- [x] Betting runs entirely on the SDK, behavior parity demonstrated
- [x] Both worker archetypes exercised in betting: ingest (lands raw) + transform (parses/normalizes from raw)
- [x] Emitted metrics match the standard series (names + labels stable)
- [x] Both repos' test suites green
- [x] **Zero business leakage in the SDK** — no `if bookmaker`, no betting constants hard-coded (thresholds are config defaults only)

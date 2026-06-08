# data-pipeline-core — Build TODO

Reusable Python SDK for GCP ingestion pipelines. The plumbing is provided; a new
project only writes the business logic (the worker `fetch()`, optional
`transform()`) and the interface.

**Full spec:** `~/Workspace/data-pipeline-core/ARCHITECTURE.md` (§5 = package
layout, §6 = core contract, §11 = versioning rules). Co-dev workflow and
extraction notes live in `~/Workspace/polytricks/data-pipeline-core.md` (§6–§7).

---

## Context & strategy

- **Both projects are greenfield.** `proba-markets-analysis` has a detailed
  roadmap but no code yet. So this is *not* "extract from mature code" — it is
  **co-development**: build the SDK and the betting pipeline together, with
  betting as the **first consumer that drives the API**.
- **Tracer-bullet, bottom-up.** Don't design the API in the abstract. Get *one*
  real betting worker running end-to-end through the SDK first, then layer in
  resilience and observability. Let real friction shape each abstraction.
- **Single consumer = overfitting risk.** For every abstraction, sanity-check
  against Polytricks (daily, no proxy): "would it use this the same way?" If not
  → make it config (default = betting value) or leave it in the project. Keep a
  running **decision log** (see last section).
- **Co-dev setup (from §7):** sibling repos, `proba-markets-analysis` depends on
  the SDK via an editable local path; stays in `0.x` (breaking changes free)
  until v0.1.0 is cut.

  ```toml
  # proba-markets-analysis/pyproject.toml
  [tool.uv.sources]
  data-pipeline-core = { path = "../data-pipeline-core", editable = true }
  ```

Each phase below has **SDK work** (this repo) and a **Betting integration** step
that proves the SDK work against a real consumer. A phase isn't done until both
are green.

---

## Phase 0 — Repo & tooling setup

- [x] `pyproject.toml` (uv + hatch build backend), `src/data_pipeline_core/` layout
- [x] Dev tooling: `ruff`, `mypy --strict`, `pytest`, `pre-commit`
- [x] CI skeleton (GitHub Actions): lint + type + test on push
- [x] `README.md`, `CHANGELOG.md` (start at `0.1.0-dev`), `.gitignore`
- [x] Test scaffolding: `conftest.py` with `fakeredis`, httpx mock, isolated
      Prometheus registry
- [x] **Betting:** init `proba-markets-analysis` Python env, wire editable path
      dependency above, confirm `import data_pipeline_core` works

## Phase 1 — Walking skeleton (the tracer bullet)

Goal: one betting worker fetches odds and writes Parquet to GCS, end-to-end,
through the SDK. No resilience, minimal obs — just the spine.

- [ ] `storage/protocols.py` — `Source`, `Sink`, `Record`, `WriteResult` Protocols
- [ ] `runtime/context.py` — minimal `RunContext` (run_id, logger, clock)
- [ ] `runtime/app.py` — minimal `WorkerApp(source, sink).run()`: call
      `source.fetch(ctx)`, stream to `sink.write()`, return exit code
- [ ] `storage/dlt_sink.py` — `dlt_sink(dataset, destination)` for filesystem/GCS
      parquet (lean on dlt for schema inference + load)
- [ ] **Betting:** implement one real `Source` (one bookmaker, one market),
      `workers/main.py` = `WorkerApp(source, sink).run()`, observe Parquet land in
      GCS. **This is the milestone that validates the core contract.**

## Phase 2 — Config & structured logging

- [ ] `runtime/config.py` — `Settings` via `pydantic-settings` (env + GCP secrets)
- [ ] Structured logging (`structlog`): JSON output, run_id/source context bound
- [ ] `runtime/lifecycle.py` — SIGTERM handling, graceful shutdown (Cloud Run Jobs)
- [ ] **Betting:** move the worker's config to `Settings`; logs are structured JSON

## Phase 3 — Observability

- [ ] `obs/metrics.py` — standard series with stable labels:
      `worker_up`, `request_rate`, `http_status_total{code}`,
      `ingestion_lag_seconds`, `circuit_breaker_state{source}`, `proxy_usage_ratio`
- [ ] `obs/gmp_push.py` — push at end of run via remote-write/OTLP, PushGateway
      fallback (workers are short-lived → push, not scrape)
- [ ] `WorkerApp.run()` wires metrics + push automatically
- [ ] **Betting:** metrics appear in GMP/Grafana Cloud; first dashboard panel live

## Phase 4 — Resilience

- [ ] `ingestion/http.py` — instrumented httpx client: retry, jitter, UA rotation
- [ ] `ingestion/circuit_breaker.py` — per-source breaker, configurable halt
      (default 15 min) on repeated 429s
- [ ] `WorkerApp` integrates breaker + http client into the run loop
- [ ] **Betting:** worker survives an injected 429 storm; breaker state metric flips

## Phase 5 — IP guard & proxy (generic mechanism, betting-tuned defaults)

- [ ] `ingestion/ip_guard.py` — Redis sliding-window counter, mode switch
      Safe/Warning/Aggressive; **thresholds are config** (defaults: <300 / 300–500
      / ≥500 req/hr per ARCHITECTURE §6)
- [ ] `ingestion/proxy.py` — proxy SaaS middleware, activated by ip_guard or
      volatility trigger
- [ ] `storage/redis_cache.py` — latest-state cache + counter backing
- [ ] **Betting:** simulate >500 req/hr → traffic routes through proxy,
      `proxy_usage_ratio` rises; **Polytricks check:** can disable proxy, keep
      retry/jitter only

## Phase 6 — Transform & staging decoupling

Goal: support the two-archetype topology (ingest workers land raw → transform
workers parse/normalize) without forcing it — simple pipelines still transform
in-process in a single worker.

- [ ] `storage/protocols.py` — add the `Transform` protocol
      (`transform(record, ctx) -> Iterable[Record]`)
- [ ] `runtime/app.py` — add optional `transform=` slot; `run()` applies it
      between `fetch()` and `write()` (the in-process transform path)
- [ ] `storage/staging.py` — raw-landing `Sink` + matching `Source` over the
      staging boundary (GCS raw bucket and/or Pub/Sub)
- [ ] `obs/metrics.py` — standard series carry a `stage` label so ingest vs
      transform workers are distinguishable on the shared dashboards
- [ ] **Betting (in-process):** add the normalization `Transform` (margin
      removal) to the worker; raw → curated in a single run
- [ ] **Betting (decoupled):** split into an ingest worker
      (`Source` → `raw_landing_sink`) and a transform worker
      (`raw_landing_source` → `Transform` → curated `dlt_sink`); confirm raw is
      replayable (re-run transform without re-fetching)
- [ ] **Polytricks check:** a single worker wired `source + transform + sink`
      works unchanged — confirms the split is wiring, not a fork

## Phase 7 — Storage maturity

- [ ] `storage/ids.py` — deterministic hash for dedup / idempotent writes
- [ ] dlt sink: Postgres upsert destination (hot state) alongside GCS (cold)
- [ ] Idempotency: re-running a worker doesn't duplicate rows
- [ ] **Betting:** odds upsert to Cloud SQL + cold Parquet to GCS, both idempotent

## Phase 8 — Hardening & first release

- [ ] Test coverage on SDK (unit + the fakeredis/httpx-mock integration paths)
- [ ] `mypy --strict` clean; public API typed and exported from `__init__.py`
- [ ] Docstrings + README usage example (the `WorkerApp(...).run()` pattern)
- [ ] Build & publish **v0.1.0** to private Artifact Registry
- [ ] **Betting:** drop the editable path override, pin `data-pipeline-core==0.1.*`,
      CI tests against the published version

---

## Definition of done — cut v0.1.0 only when ALL hold

- [ ] Betting runs entirely on the SDK, behavior parity demonstrated
- [ ] Both worker archetypes exercised in betting: ingest (lands raw) + transform
      (parses/normalizes from raw)
- [ ] Emitted metrics match the standard series (names + labels stable)
- [ ] Both repos' test suites green
- [ ] **Zero business leakage in the SDK** — no `if bookmaker`, no betting
      constants hard-coded (thresholds are config defaults only)

Until all four hold, stay on editable / `0.x`. Publishing early in multi-repo
costs a publish/bump cycle on every adjustment.

---

## Decision log (keep updated — briefs the future Polytricks instance)

Record as you go: what went **into the SDK**, what stayed **in the project**, and
what is **deferred** (generalize on the 2nd real usage). Maintain in
`CHANGELOG.md` and mirror the headline here.

| Item | Decision | Rationale |
|---|---|---|
| _e.g. IP guard thresholds_ | SDK, as config defaults | mechanism generic, values betting-specific |
| _e.g. entity resolution (Marseille↔OM)_ | stays in betting | pure business logic |
| _e.g. raw-landing staging adapters_ | SDK | generic handoff; `transform()` logic stays in betting |

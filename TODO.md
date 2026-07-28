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

- [x] `storage/protocols.py` — `Source`, `Sink`, `Record`, `WriteResult` Protocols
- [x] `runtime/context.py` — minimal `RunContext` (run_id, logger, clock)
- [x] `runtime/app.py` — minimal `WorkerApp(source, sink).run()`: call
      `source.fetch(ctx)`, stream to `sink.write()`, return exit code
- [x] `storage/dlt_sink.py` — `dlt_sink(dataset, destination)` for filesystem/GCS
      parquet (lean on dlt for schema inference + load)
- [x] **Betting:** implement one real `Source` (one bookmaker, one market),
      `workers/main.py` = `WorkerApp(source, sink).run()`, observe Parquet land in
      GCS. **This is the milestone that validates the core contract.**
      _Verified locally against a `file://` bucket; `gs://` is the same code by
      config (`DESTINATION__FILESYSTEM__BUCKET_URL`)._

## Phase 2 — Config & structured logging

- [x] `runtime/config.py` — `Settings` via `pydantic-settings` (env + GCP secrets)
      _(env/.env now; GCP Secret Manager deferred to a later phase)_
- [x] Structured logging (`structlog`): JSON output, run_id/source context bound
- [x] `runtime/lifecycle.py` — SIGTERM handling, graceful shutdown (Cloud Run Jobs)
- [x] **Betting:** move the worker's config to `Settings`; logs are structured JSON

## Phase 3 — Observability

- [x] `obs/metrics.py` — standard series with stable labels:
      `worker_up`, `request_rate`, `http_status_total{code}`,
      `ingestion_lag_seconds`, `circuit_breaker_state{source}`, `proxy_usage_ratio`
- [x] `obs/metrics.py` — technical-dashboard series (runs/errors/storage):
      `worker_runs_total{status}` (runs & errors), `records_written_total`,
      `bytes_written_total` (throughput & storage footprint). `WriteResult` grows an
      optional `byte_count` (raw-landing reports it; dlt reports rows only).
      `WorkerApp.run()` emits all three. _The dashboard over these lives in
      `data-pipeline-infra` (Phase 7) — SDK owns the series, infra owns the render._
- [x] `obs/gmp_push.py` — push at end of run via remote-write/OTLP, PushGateway
      fallback (workers are short-lived → push, not scrape)
      _(PushGateway transport built; GMP remote-write/OTLP deferred to a real GCP
      target — config swap, not a code change)_
- [x] `obs/otlp_push.py` — **OTLP/HTTP push** (the preferred path): worker writes
      its final series straight to an OTLP backend at exit (Grafana Cloud's OTLP
      gateway), no PushGateway/scraper. `WorkerApp` uses it when `metrics_otlp_url`
      is set, else the PushGateway fallback. OTLP-JSON, no protobuf/compression;
      names sent as-is so the OTLP→Prometheus mapping preserves them. Infra wiring
      (endpoint + token secret) in `data-pipeline-infra` Phase 7. _(Replaced the
      Prometheus remote-write emitter — Grafana Cloud ingests via OTLP.)_
- [x] `WorkerApp.run()` wires metrics + push automatically
- [x] **Betting:** metrics appear in GMP/Grafana Cloud; first dashboard panel live
      _(verified locally: worker emits the full standard series with the `source`
      label and `worker_up=1`; the GMP/Grafana panel needs cloud access — pending)_

## Phase 4 — Resilience

- [x] `ingestion/http.py` — instrumented httpx client: retry, jitter, UA rotation
- [x] `ingestion/circuit_breaker.py` — per-source breaker, configurable halt
      (default 15 min) on repeated 429s _(in-memory per run; cross-run
      persistence is Phase 5)_
- [x] `WorkerApp` integrates breaker + http client into the run loop
      (exposed to the source as `ctx.http`)
- [x] **Betting:** worker survives an injected 429 storm; breaker state metric flips

## Phase 5 — IP guard & proxy (generic mechanism, betting-tuned defaults)

- [x] `ingestion/ip_guard.py` — Redis sliding-window counter, mode switch
      Safe/Warning/Aggressive; **thresholds are config** (defaults: <300 / 300–500
      / ≥500 req/hr per ARCHITECTURE §6)
- [x] `ingestion/proxy.py` — proxy SaaS middleware, activated by ip_guard or
      volatility trigger _(via a forced-routing hook; volatility detection stays
      in the project)_
- [x] `storage/redis_cache.py` — latest-state cache + counter backing
- [x] **Betting:** simulate >500 req/hr → traffic routes through proxy,
      `proxy_usage_ratio` rises; **Polytricks check:** can disable proxy, keep
      retry/jitter only _(proxy disabled by config → stays direct; SDK-tested)_

## Phase 6 — Transform & staging decoupling

Goal: support the two-archetype topology (ingest workers land raw → transform
workers parse/normalize) without forcing it — simple pipelines still transform
in-process in a single worker.

- [x] `storage/protocols.py` — add the `Transform` protocol
      (`transform(record, ctx) -> Iterable[Record]`)
- [x] `runtime/app.py` — add optional `transform=` slot; `run()` applies it
      between `fetch()` and `write()` (the in-process transform path)
- [x] `storage/staging.py` — raw-landing `Sink` + matching `Source` over the
      staging boundary (GCS raw bucket via fsspec; Pub/Sub variant deferred)
- [x] `obs/metrics.py` — standard series carry a `stage` label so ingest vs
      transform workers are distinguishable on the shared dashboards
- [x] **Betting (in-process):** add the normalization `Transform` (margin
      removal) to the worker; raw → curated in a single run
- [x] **Betting (decoupled):** split into an ingest worker
      (`Source` → `raw_landing_sink`) and a transform worker
      (`raw_landing_source` → `Transform` → curated `dlt_sink`); confirm raw is
      replayable (re-run transform without re-fetching) _(verified via CLI: raw
      JSONL lands, transform replays it twice)_
- [x] **Polytricks check:** a single worker wired `source + transform + sink`
      works unchanged — confirms the split is wiring, not a fork

## Phase 7 — Storage maturity

- [x] `storage/ids.py` — deterministic hash for dedup / idempotent writes
- [x] dlt sink: idempotent merge (hot state) alongside GCS (cold)
      _**scope change** (user decision): GCS-only with Redis as the optional hot
      tier — no Postgres/Cloud SQL. Cold idempotency is a Delta Lake merge on GCS
      (ACID upsert, no service); hot state is `redis_latest_sink`._
- [x] Idempotency: re-running a worker doesn't duplicate rows
- [x] **Betting:** odds upsert to Redis (optional) + cold Delta to GCS, both
      idempotent _(verified via CLI + tests: 2 transform replays → curated stays
      at 3 rows; Redis keeps one snapshot per match)_

## Phase 8 — Hardening & first release

- [ ] Test coverage on SDK (unit + the fakeredis/httpx-mock integration paths)
- [ ] `mypy --strict` clean; public API typed and exported from `__init__.py`
- [ ] Docstrings + README usage example (the `WorkerApp(...).run()` pattern)
- [ ] Build & publish **v0.1.0** to private Artifact Registry

---

## Backlog — deferred follow-ups (surfaced by betting Phase 1)

Tracked, not blocking v0.1.0. Promote when a real consumer genuinely needs them.

- [ ] **Pub/Sub staging variant** — `raw_landing_sink` / `raw_landing_source` over
      Pub/Sub (publish on ingest, subscribe on transform), alongside the current
      fsspec JSONL handoff. Betting wants event-driven fetch→transform; the
      file/GCS-raw handoff covers it meanwhile. Deferred until a real GCP target
      exists (already flagged in `storage/staging.py`).
- [ ] **Always-on transform archetype** — a long-running subscriber service (vs the
      one-shot Cloud Run Job) for "always-on" transform consumption. New runtime
      model; build only when a consumer needs continuous (not scheduled/triggered)
      processing.
- [ ] **Single-work-unit run (granularity)** — let a worker run for *one* source
      work unit per invocation (e.g. a single competition), selected by config/env,
      instead of looping all units. **Prereq for `data-pipeline-infra` v2**
      (per-unit Cloud Tasks fan-out — one self-paced chain per unit). Today's
      source loops all units (fine for v1's whole-source cron); v2 needs a
      "target one unit" path. Mechanism generic; the unit is consumer-defined.
- [ ] **Scheduling hint (`next_run_seconds`)** — an optional typed value a worker
      emits at end of run (e.g. `RunContext.request_next_run(delay_seconds)` or
      returned from `run()`) so infra self-paces the next invocation per unit
      (Cloud Tasks `schedule_time`). The narrow-waist business→infra contract: a
      single clamped scalar, infra enforces `[min,max]`, the consumer computes the
      value. **Prereq for `data-pipeline-infra` v2.** Deferred like Pub/Sub —
      declare the seam, build the emitter when v2 lands.

**Not SDK — belongs to `data-pipeline-infra`:**
- Raw-landing **retention** (e.g. betting's 7 days) → GCS bucket lifecycle rule;
  the retention period is a consumer config value.
- Transform **run frequency** (always-on / scheduled / fixed interval) → Cloud
  Scheduler (cron/interval) + Pub/Sub push subscriptions. The SDK worker stays a
  one-shot job; triggering is infra.

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
| IP guard thresholds | SDK, as config defaults | mechanism generic, values betting-specific |
| entity resolution (Marseille↔OM) | stays in betting | pure business logic |
| raw-landing staging adapters | SDK | generic handoff; `transform()` logic stays in betting |
| `py.typed` marker | SDK (shipped) | consumers need PEP 561 to see inline types; surfaced by betting `mypy --strict` |
| Pub/Sub staging variant | SDK, deferred | generic handoff, but no GCP target yet; fsspec JSONL covers dev/now |
| raw retention (7 days) | infra, not SDK | GCS bucket lifecycle policy; period is a consumer config value |
| transform run frequency | infra, not SDK | Cloud Scheduler / Pub/Sub push; the SDK worker stays one-shot |
| single-work-unit run (granularity) | SDK, deferred (infra v2) | per-unit cadence needs targeting one unit; v1 loops all. Mechanism generic; the unit (e.g. competition) is consumer-defined |
| scheduling hint (`next_run_seconds`) | SDK, deferred (infra v2) | narrow-waist business→infra contract; worker emits one clamped scalar, infra (Cloud Tasks) self-paces, consumer computes the value |
| browser-TLS impersonation (`curl_cffi`) | SDK, as config (`impersonate`) | generic anti-bot mechanism (JA3/JA4), like the proxy; profile value is consumer config |
| console sink for dev confirmation | stays in betting | trivial project helper; promote only on a 2nd real usage |
| technical-obs series (runs/errors/storage) | SDK | the worker is the only thing that knows it ran/errored/wrote N; part of the frozen surface, shared by all consumers |
| technical dashboard (Grafana JSON) | infra, not SDK | generic over the frozen series but deployed by infra's Grafana provider; SDK = what's measured, infra = where it's shown |
| `WriteResult.byte_count` | SDK, optional | sinks that can report volume cheaply do (raw landing); others leave `None` → `bytes_written_total` stays flat, no forced cost on every sink |
| OTLP push over PushGateway | SDK, preferred path | short-lived jobs → write to the backend at exit; a PushGateway needs an always-on box + scraper. PushGateway kept as fallback |
| OTLP/HTTP-JSON (vs Prometheus remote-write) | SDK | the chosen backend (Grafana Cloud) surfaces OTLP for custom metrics, not remote-write; OTLP-JSON is also zero-dep (no protobuf/snappy) — simpler than the remote-write emitter it replaced |

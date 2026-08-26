# DEVELOPMENT.md — how this build is run

Companion to `TODO.md`, which holds the task checklists only. This file holds the
strategy, the co-dev workflow, the phase goals, and the decision log.

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

Each phase in `TODO.md` has **SDK work** (this repo) and a **Betting
integration** step that proves the SDK work against a real consumer. A phase
isn't done until both are green.

---

## Phase goals

- **Phase 0 — Repo & tooling setup.** Get the package, tooling, CI and test
  scaffolding in place, and prove the editable dependency resolves from betting.
- **Phase 1 — Walking skeleton (the tracer bullet).** One betting worker fetches
  odds and writes Parquet to GCS, end-to-end, through the SDK. No resilience,
  minimal obs — just the spine.
- **Phase 2 — Config & structured logging.** Settings and JSON logs, so the
  worker stops carrying its own config and log plumbing.
- **Phase 3 — Observability.** Freeze the standard metric series and push them
  from short-lived jobs. The SDK owns *what* is measured; `data-pipeline-infra`
  (Phase 7) owns *where* it's rendered and the OTLP wiring (endpoint + token
  secret). OTLP names are sent as-is so the OTLP→Prometheus mapping preserves
  them.
- **Phase 4 — Resilience.** Instrumented HTTP client and per-source circuit
  breaker wired into the run loop.
- **Phase 5 — IP guard & proxy.** Generic mechanism, betting-tuned defaults:
  thresholds are config (defaults per ARCHITECTURE §6), and Polytricks must be
  able to disable the proxy and keep retry/jitter only.
- **Phase 6 — Transform & staging decoupling.** Support the two-archetype
  topology (ingest workers land raw → transform workers parse/normalize) without
  forcing it — simple pipelines still transform in-process in a single worker.
- **Phase 7 — Storage maturity.** Deterministic ids, idempotent writes, and the
  hot/cold split. _**Scope change** (user decision): GCS-only with Redis as the
  optional hot tier — no Postgres/Cloud SQL. Cold idempotency is a Delta Lake
  merge on GCS (ACID upsert, no service); hot state is `redis_latest_sink`._
- **Phase 8 — Hardening & first release.** Coverage, typing, docs, then cut and
  publish v0.1.0.

---

## Fix goals (review 2026-08-24)

Four correctness defects found reviewing the resilience layer. They sit before
Phase 8 in `TODO.md` because hardening a broken contract is wasted work. Priority
order below; fixes 2 and 3 both rewrite `HttpClient.request` / `read_until`, so do
them back to back, one commit each.

- **Fix 1 — `Response` omits `is_success`.** The protocol declares `status_code` /
  `content` / `text` / `json()` only, but `http.py` uses `response.is_success`. It
  typechecks internally because the concrete `httpx.Response | _CurlResponse` union
  leaks through the declared return type; a consumer sees only the protocol and
  gets `"Response" has no attribute "is_success"` under `mypy --strict`. We ship
  `py.typed`, so this is a broken published contract. Declare the property —  both
  backends already implement it, so no implementation changes. Widening only breaks
  hypothetical *implementers* of `Response`, of which there are none. Guard it with
  a test that binds the result to a `Response`-annotated local (mypy covers
  `tests/`, so it fails today).
- **Fix 2 — the retry loop miscounts real outbound requests.** `IpGuard.evaluate()`
  and `proxied_count` fire once per `request()` call; `request_count` increments per
  *attempt*. A request retried twice is three packets on the wire but one token
  consumed in Redis, so the IP guard under-counts the exact quantity it exists to
  bound, and `proxy_usage_ratio` reads 0.33 when every packet was proxied. Extract
  the per-attempt preamble (guard → mode → jitter → client choice) and call it
  inside the loop: one attempt = one token = one `proxied_count`. Re-evaluating per
  attempt is also the correct behaviour — rising density should escalate the mode
  mid-retry. The same helper replaces the near-identical duplicated preamble in
  `read_until`. Verify with 503→503→200 through a proxied, guard-backed client:
  `request_count`, `proxied_count` and the fakeredis window counter all at 3.
- **Fix 3 — transport errors are invisible.** `request_count` and
  `observe_http_status` only fire once a response exists, so a source timing out on
  every call exports `request_rate=0`, no `http_status_total` sample, and nothing
  but `worker_up=0` — the dashboard goes dark on precisely the incident it exists
  for. Count the attempt and record it under a sentinel label *value*,
  `http_status_total{code="transport_error"}`: `code` is already stringified, so
  this adds no series and renames nothing, and the §8 stability guardrail holds.
  Accept deliberately that `code=~"5.."` won't match it — it isn't an HTTP status.
  Preferred over a new `http_transport_errors_total` series, which would cost an
  `ARCHITECTURE.md` §8 amendment for no extra signal.
- **Fix 4 — breaker state dies with the process.** State is in-memory inside a
  one-shot Cloud Run Job, so the 900 s halt exits with the process and the next
  invocation starts closed and walks back into the 429 wall. Cross-run persistence
  was deferred from Phase 4 to Phase 5; Phase 5 shipped Redis and didn't pick it
  up. Back the breaker with the existing client when `redis_url` is set, mirroring
  `IpGuard`'s shape (optional client, injected clock, in-memory fallback when
  absent). Use the TTL *as* the cooldown — `SET breaker:{source} EX cooldown` — so
  expiry is the reset: no clock-skew handling, no cleanup, and `is_open` stops
  being a side-effecting property. The failure streak becomes `INCR` + `EXPIRE`
  under a second key, dropped by `record_success`. `__init__` gains an optional
  client, which is additive and the class isn't exported from `__init__.py`, so
  the contract is untouched. Accepted cost: one extra Redis round trip per request
  on top of the guard's — fold it into the guard's pipeline only if it measurably
  hurts. Verify with two breakers over one fakeredis: open the first, the second
  reads open; advance past the cooldown and it reads closed.

---

## Backlog policy

Backlog items in `TODO.md` are tracked, not blocking v0.1.0. Promote one when a
real consumer genuinely needs it (rule of two: generalize on the 2nd real usage).

**Not SDK — belongs to `data-pipeline-infra`:**

- Raw-landing **retention** (e.g. betting's 7 days) → GCS bucket lifecycle rule;
  the retention period is a consumer config value.
- Transform **run frequency** (always-on / scheduled / fixed interval) → Cloud
  Scheduler (cron/interval) + Pub/Sub push subscriptions. The SDK worker stays a
  one-shot job; triggering is infra.

---

## Release gate

Cut v0.1.0 only when every item under "Definition of done" in `TODO.md` holds.
Until then, stay on editable / `0.x`. Publishing early in multi-repo costs a
publish/bump cycle on every adjustment.

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

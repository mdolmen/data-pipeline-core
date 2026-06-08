# CLAUDE.md — data-pipeline-core

Reusable Python SDK for GCP ingestion pipelines. Read `ARCHITECTURE.md` for the
design and `TODO.md` for the sequenced build plan before making changes.

These are behavioral guidelines (derived from Karpathy's notes on LLM coding
pitfalls), specialized to this repo. They bias toward caution over speed; use
judgment on trivial tasks.

## 1. Think before coding

- State assumptions explicitly. If a requirement is ambiguous, ask — don't pick
  silently.
- If multiple interpretations exist, surface them. If a simpler approach exists,
  say so and push back when warranted.
- If something is unclear, stop and name what's confusing.

## 2. Simplicity first

- Minimum code that solves the problem. Nothing speculative.
- No features, abstractions, "flexibility", or error handling beyond what was
  asked or what the contract requires.
- If you write 200 lines and it could be 50, rewrite it. Ask: "would a senior
  engineer call this overcomplicated?"

## 3. Surgical changes

- Touch only what the task requires. Don't "improve" adjacent code, comments, or
  formatting; match existing style even if you'd do it differently.
- Clean up orphans **your** changes create (unused imports/vars). Don't delete
  pre-existing dead code — mention it instead.
- Every changed line should trace directly to the request.

## 4. Goal-driven execution

- Turn tasks into verifiable goals: "fix bug" → "write a failing test that
  reproduces it, then make it pass".
- For multi-step work, state a brief plan with a verify step each, then loop until
  green.

---

## Project-specific guardrails

These override the generic guidance where they conflict — they are why this repo
exists.

- **Only the generic goes in the SDK.** A mechanism is shared; its values are
  not. Anything referencing a bookmaker, odds, a canonical entity, a strategy, or
  a numeric business threshold does **not** belong here. The test: if you'd write
  `if bookmaker == ...` or hard-code a betting constant, stop — it stays in the
  consuming project. See `ARCHITECTURE.md` §12.
- **Config over forking.** Project-specific values become `Settings` defaults
  (default = the betting value), never hard-coded constants. Behaviour varies by
  configuration, not by editing the SDK (e.g. Polytricks disables the proxy).
- **Contract-driven.** The SDK owns the run loop; projects fill the `Source` /
  `Sink` Protocols. Keep the public contract (`Source`, `Sink`, `WorkerApp`,
  `RunContext`) small and stable. Changing it is a SemVer-major event.
- **Inclusion rule (rule of two).** Add something to the SDK only when it is
  *literally* identical across consumers. When in doubt, leave it in the project
  and promote it once a second real usage confirms the shape. Resist abstracting
  for a single consumer — overfitting to betting is the main risk.
- **Stable observability surface.** Don't rename or relabel the standard metric
  series (`ARCHITECTURE.md` §8) — Grafana dashboards depend on them.
- **Decision log.** When you decide something goes in the SDK vs stays in a
  project (or is deferred), record it in `CHANGELOG.md` per the table in
  `TODO.md`. This briefs the future Polytricks instance.

## Conventions

- Python 3.11+, strict type hints; `mypy --strict` and `ruff` must pass.
- Tests use `fakeredis`, httpx mocking, and an isolated Prometheus registry
  (`conftest.py`). New mechanisms ship with tests.
- Lean on `dlt` for the load layer; don't reimplement schema/load handling the
  library already provides.
- During co-development the SDK stays in `0.x`; breaking changes are fine until
  v0.1.0 is cut (see the spec §7 / `TODO.md` Definition of Done).

## Git

- Commit often. Each commit is self-contained: it builds, it makes sense on its own, and it does one thing. A feature spanning backend + frontend can still be one commit if the pieces only make sense together — but unrelated cleanups go in their own commit.
- Commit messages in English, following [Conventional Commits](https://www.conventionalcommits.org/) — `<type>(optional scope): description`.
- **Type**: one of `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`, `perf`. An optional scope names the area (e.g. `feat(ingestion):`). A breaking change adds `!` before the colon (`feat!:`) — any change to the public contract (`Source`, `Sink`, `WorkerApp`, `RunContext`) is breaking and is a SemVer-major event.
- **Subject**: single line, imperative mood, lower-case after the colon, no trailing period. Keep it tight.
- **Body** (only when needed): blank line after subject, then a short paragraph explaining the *why*. If there are multiple distinct points, use one bullet (`- `) per point instead of prose.
- No `Co-Authored-By` trailers unless explicitly requested.
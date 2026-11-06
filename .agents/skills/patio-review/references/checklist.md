# Patio review checklist

Work through the sections for the areas a PR touches. Items marked with a PR number come from findings in past
reviews.

## Every change

- [ ] The scope matches the PR description; no unrelated refactors or reformatting.
- [ ] Every new outbound HTTP call has an explicit timeout (#3).
- [ ] No secrets (API keys, tokens, auth headers) in logs, exceptions, or error responses.
- [ ] Failure logs carry context (URL, status code, response body) through `init_logger(__name__)`.
- [ ] No new runtime dependency without a reason. Everything in `pyproject.toml` `dependencies` ships in the
      image.
- [ ] Code works on Python 3.10, the minimum (`requires-python = ">=3.10"`).

## Configuration (`envs.py`, `config.py`, CLI flags)

- [ ] A new env var is defined in `envs.py` with a string default and explicit parsing, and read as `envs.X` at
      call time.
- [ ] A renamed env var keeps the old name as a fallback, and a test covers precedence when both are set (#3).
- [ ] README defaults match the code exactly, and new variables or flags appear in the README tables (#3).
- [ ] Boolean CLI flags use `store_true` or `BooleanOptionalAction`, never `type=bool`.
- [ ] A new topology type has a canonical constant in `SUPPORTED_TOPO_TYPES` and a case-insensitive factory
      branch.

## Topology (`src/patio/topo/**`)

- [ ] Every request passes `timeout=(envs.TOPO_CONNECT_TIMEOUT, <read timeout>)` (#3).
- [ ] `register()` is idempotent (the heartbeat repeats it), doesn't mutate the caller's `worker_info`, and
      validates input inside its `try` so failures return `False`.
- [ ] `unregister()` is bounded well below the pod grace period (default 30 s) and fails fast on 4xx (#6). It
      runs only after uvicorn's graceful shutdown, and only on uvicorn ≥ 0.29.
- [ ] The router address comes from `ROUTER_ROLE_NAME`/`ROUTER_PORT` (legacy `SGL_ROUTER_*` fallback) in RBG DNS
      format, and the worker address honors `POD_IP`.
- [ ] Missing required configuration raises a clear `RuntimeError` in `__init__` (startup aborts) rather than
      failing on every heartbeat.
- [ ] Error handling matches the existing clients: log, traceback, return `bool`.
- [ ] Nothing new builds on `topo/server/*`, `send_heartbeat`, or `get_topo_register_endpoint`.
- [ ] Tests assert the exact URL, payload, headers, and timeout, and cover non-2xx, exceptions, a patched
      `utils.retry`, and a singleton reset where applicable.
- [ ] A new router integration comes with a proposal and a `doc/` page.

## Engine and API (`engine/**`, `api/**`, `app.py`)

- [ ] Models extend `NoExtraBaseModel` and use pydantic v2 validators; no `pydantic.v1` import.
- [ ] Failures return `ErrorResponse` through the `response_error` helpers. Upstream status codes are passed as
      `int`, never wrapped in `HTTPStatus(...)`.
- [ ] `async def` paths use `httpx.AsyncClient` with explicit timeouts. Clients are closed or reused, never
      leaked per request. No `requests` or `time.sleep` in async code.
- [ ] Engine paths are correct: SGLang has no `/v1` prefix, vLLM uses `/v1/...`.
- [ ] A new engine also adds metric rules; otherwise startup fails whenever metric scraping is enabled (the
      default) and `INFERENCE_ENGINE_ENDPOINT` is set.
- [ ] A new router module is included in `app.py`, and the README "API Reference" and `doc/api_protocol.md` are
      updated.
- [ ] Middleware metric labels stay bounded (route template, not raw path) if the middleware is touched.
- [ ] Blocking work added before `uvicorn.run` delays Patio's `/health`; check the probe implications.

## Metrics (`metrics/**`)

- [ ] Collectors are registered on `patio_registry` only.
- [ ] Rule keys are `prometheus_client` family names (counters without `_total`), and each key equals the
      rule's `old_name`.
- [ ] Names use the `patio:` prefix. Two engines share a name only when semantics and units match.
- [ ] `collect()` and the rules never raise, and the scrape timeout stays small.
- [ ] A renamed or removed exposed metric is treated as breaking, and the README metrics table and
      `doc/metrics.md` are updated.
- [ ] Tests run Prometheus text through `EngineCollector` and `generate_latest`.

## Tests (`tests/**`)

- [ ] Names are patched at the lookup site; `importlib.reload(envs)` follows env changes; singletons are
      reset; `utils.retry` is patched; async code uses `AsyncMock`.
- [ ] No real network access and no avoidable sleeps.
- [ ] Failure paths are covered, not just the happy path.

## Docs and proposals

- [ ] README tables (CLI, env vars, metrics, API) and the matching `doc/*.md` page are updated; new pages are
      linked from `doc/README.md`.
- [ ] A proposal exists (`proposals/README.md`) for a new engine backend, a new router integration, a change to
      a public API, protocol, or configuration contract, or a change to lifecycle, security, or failure
      recovery. `proposal.yaml` has the correct `status`.
- [ ] If a listed known issue was fixed, its entry in `references/known-issues.md` is removed in the same PR.

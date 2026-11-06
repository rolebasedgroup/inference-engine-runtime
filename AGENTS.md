# AGENTS.md: Patio (inference-engine-runtime)

Guidance for AI coding agents and human contributors. Trust this file and the skills it links to.
Search the code only when something here is missing or wrong, and then fix this file in the same PR.
**The code is the source of truth.** `README.md` and `doc/` are out of date in places; see
`.agents/skills/patio-review/references/known-issues.md`.

## What Patio is

Patio (distribution `patio-runtime`, import package `patio`) is a small Python sidecar. It runs next to an
inference engine (SGLang or vLLM) in a [RoleBasedGroup (RBG)](https://github.com/sgl-project/rbg) pod.
A single process serves a FastAPI app on port `9091` and does three things:

- proxies LoRA load/unload requests to the engine's native API;
- scrapes the engine's Prometheus metrics, renames the mapped ones into a unified `patio:` namespace, and
  re-exposes them;
- registers the engine as a worker with a router or proxy (SGLang Router, vLLM proxy), re-registers it on a
  heartbeat, and calls the client's `unregister()` on SIGTERM/SIGINT (only on uvicorn ≥ 0.29; for the vLLM
  proxy it is a no-op).

## Repository map

| Path | Responsibility | Key symbols |
|------|----------------|-------------|
| `src/patio/app.py` | CLI entry point (`patio`), FastAPI app, HTTP-metrics middleware, startup, topology, heartbeat, signals | `main`, `app`, `run_topo_client`, `_init_metrics` |
| `src/patio/envs.py` | Env vars, read **once at import time** (topology clients also read `POD_IP` and some `RBG_*` directly) | `INFERENCE_ENGINE*`, `TOPO_*`, `ROUTER_*`, `GROUP_NAME`/`ROLE_NAME`/`ROLE_INDEX` (from `RBG_*`) |
| `src/patio/config.py` | Constants | `DEFAULT_PATIO_PORT`, `SUPPORTED_TOPO_TYPES`, `EXIT_EVENT` |
| `src/patio/logger.py` | Logging to stdout and `/tmp/patio/patio.log` | `configure_logging`, `init_logger` |
| `src/patio/api/` | Routers, pydantic models, error helpers | `server_router`, `lora_router`, `NoExtraBaseModel`, `ErrorResponse`, `response_error.*` |
| `src/patio/engine/` | Engine abstraction and the SGLang/vLLM LoRA proxies | `InferenceEngine`, `inference_engine()`, `SGLangInferenceEngine`, `VLLMInferenceEngine` |
| `src/patio/metrics/` | Patio registry, engine scraper, rename rules | `patio_registry`, `EngineCollector`, `RenameStandardRule`, `get_metric_standard_rules` |
| `src/patio/topo/` | Router/proxy registration clients, factory, retry helper | `GroupTopoClient`, `SGLangGroupTopoClient`, `VLLMProxyTopoClient`, `create_topo_client`, `utils.retry` |
| `tests/mock_tests/` | Offline unit tests (pytest, mocks only) | `test_<module>.py` |
| `tests/e2e/*.sh` | Manual scripts that need a real engine and GPU | not part of normal validation |
| `proposals/` | Design-proposal process and template | `proposals/README.md` |
| `doc/` | User documentation, partly stale | |

## Runtime model

1. `main()` parses CLI flags and configures logging.
2. `_init_metrics`: when engine-metric scraping is enabled (the default) and `INFERENCE_ENGINE_ENDPOINT` is set,
   loads the engine's metric rules (an engine without rules fails startup here) and registers an
   `EngineCollector` on `patio_registry`. The collector scrapes the engine every time `/metrics` is requested.
3. If `--instance-info '<json>'` is given, `run_topo_client` parses `{"topo_type": ..., "data": {...}}`.
   `topo_type` is optional and falls back to the `TOPO_TYPE` env var. It then builds the client with
   `create_topo_client`, **blocks** in `wait_engine_ready()` and `register()` (each retries up to 60 times, 3 s apart),
   installs SIGTERM/SIGINT handlers that call `unregister()`, and starts a daemon heartbeat thread that calls
   `register()` again every `HEARTBEAT_INTERVAL` seconds. If either call returns `False`, startup still
   continues; an exception exits the process with `sys.exit(1)`.
4. `uvicorn.run(app)`. Patio's own `/health` is served only from this point on.

RBG passes worker data as an argument, e.g.
`--instance-info={"data":{"port":8000,"worker_type":"prefill","bootstrap_port":34000}}`. The `TOPO_TYPE` and
`ROUTER_*` env vars come from the `ClusterEngineRuntimeProfile`. Details and diagrams are in
`.agents/skills/patio-dev/references/architecture.md`.

## Public contracts (compatibility-sensitive)

External controllers deploy Patio, so these surfaces are contracts. A change to one of them must stay
backward compatible or come with an explicit migration note, and README/`doc/` must be updated in the same
PR. New integrations, and changes to protocols or lifecycle, also need a proposal (`proposals/README.md`).

- **Inbound HTTP:** `GET /`, `GET /health`, `GET /metrics`, `POST /load_lora_adapter`, `POST /unload_lora_adapter`.
  FastAPI's `/docs` and `/openapi.json` are always served as well. Request models reject unknown fields with
  FastAPI's 422 `{"detail": [...]}`. LoRA failures return `ErrorResponse` `{code, type, message, object, param}`
  with HTTP status equal to `code`: `InvalidError` 400 for missing or unsupported engine configuration,
  `ServerError` for engine failures, 501 for unimplemented operations. The exception is a non-standard upstream
  status, which currently surfaces as a bare 500. Success returns a JSON string.
- **Env vars and CLI flags:** see `.agents/skills/patio-dev/references/configuration.md`. Deployed profiles still
  use the legacy `SGL_ROUTER_ROLE_NAME`/`SGL_ROUTER_PORT` and lowercase `TOPO_TYPE` values such as `sglang`.
- **Exposed metric names:** `patio:*` (`metrics/engine_metric_rules.py`), `http_requests_total`,
  `http_request_latency_seconds`.
- **Outbound engine APIs:** SGLang `POST /load_lora_adapter` and `/unload_lora_adapter`; vLLM
  `POST /v1/load_lora_adapter` and `/v1/unload_lora_adapter`; both `GET /health` and `GET /metrics`.
- **Router/proxy protocols:** SGLang Router `POST /workers` (expects 202 with `worker_id`) and
  `DELETE /workers/{worker_id}`; vLLM proxy `POST /instances/add`, which has no unregister call.
- **RBG addressing:** `{RBG_GROUP_NAME}-{role}-{index}.s-{RBG_GROUP_NAME}-{role}:{port}`. The registered worker
  address is `data.instance` (vLLM only), otherwise `{POD_IP}:{port}`, otherwise that RBG DNS name. Engine
  health checks go to `POD_IP` or `localhost`.
- **Container image:** `Dockerfile` installs the package; the entrypoint is `patio`.

## Build, test, run

Verified with Python 3.12. Python ≥ 3.10 is required (`requires-python` in `pyproject.toml`). There is no CI
workflow, linter, or formatter in the repo, so run the tests yourself before you finish.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                                               # all offline unit tests, about 5 s
pytest tests/mock_tests/test_sgl_topo_client.py -v   # one file
python tests/run_mock_tests.py                       # same suite; contributors quote it in PRs
patio --help                                         # or: python -m patio.app --port 9091
docker build -t inference-engine-runtime:latest .
```

The existing code is mostly Black-formatted (default settings). Formatting is not enforced, so match the
surrounding style and do not reformat code you did not change.

## Conventions

- **Env vars:** define each in `envs.py` with a string default and explicit parsing (`float(os.getenv("X", "3"))`).
  Read it as `envs.X` at call time so tests can `importlib.reload(envs)`; `from patio.envs import X` freezes the
  value. A rename keeps the old name as a fallback: `os.getenv("NEW") or os.getenv("OLD")`.
- **Outbound HTTP:** give every new call an explicit timeout. (The existing LoRA unload calls rely on httpx's
  5 s default.) Topology calls use `(envs.TOPO_CONNECT_TIMEOUT, <read timeout>)`. Use `httpx.AsyncClient` in
  `async def` paths; use `requests` with `topo.utils.retry` only in the synchronous startup and topology code.
  Never block the event loop.
- **Errors:** API paths return `ErrorResponse` built with the helpers in `api/response_error.py`. Topology clients
  catch exceptions, log them with context, call `traceback.print_exc()`, and return `bool`; they must not raise
  from `wait_engine_ready`/`register`/`unregister`. SGLang `register()` currently can (known issue K7).
- **Models:** subclass `NoExtraBaseModel` and use pydantic v2 APIs (`model_validator`, `field_validator`,
  `model_dump`). `pydantic.v1` decorators never run on v2 models.
- **Topology types:** canonical names live in `config.py` (`SGLang`, `vLLM`), and `topo/factory.py` matches them
  case-insensitively. Keep it that way, because deployed profiles use lowercase values.
- **Metrics:** register on `patio_registry`, never on the global `REGISTRY`; standardized names carry the
  `patio:` prefix; label values must stay bounded.
- **Lifecycle:** background threads are daemon threads. Anything on the SIGTERM path must finish well within
  the pod's termination grace period (Kubernetes default: 30 s).
- **Logging:** create `logger = init_logger(__name__)` at module level. Failure logs include the URL, status code,
  and response body.
- **Legacy, unused at runtime** (don't build on these without a proposal): `topo/server/*`, `create_topo_server`,
  `GroupTopoClient.send_heartbeat`, `envs.get_topo_register_endpoint`, `topo/utils.get_worker_endpoint` and
  `write_config_file`, `api/protocol.Topo*Request`, `InferenceEngine.ready()`.

## Definition of done

1. Tests in `tests/mock_tests/` cover the success path and the failure paths (non-2xx, exception or timeout,
   missing configuration), and `pytest` passes.
2. Contract changes are backward compatible or explicitly documented as breaking.
3. README tables (CLI flags, env vars, metrics, API) and the matching `doc/*.md` page are updated in the same PR.
4. A proposal in `proposals/` exists if the change adds an engine backend or a router integration, or changes a
   public API, protocol, configuration contract, lifecycle, or failure-recovery behavior.
5. Unrelated code is neither refactored nor reformatted.

## Skills and scoped rules

| Path | Use it when |
|------|-------------|
| `.agents/skills/patio-dev/SKILL.md` | Implementing changes: architecture, configuration reference, playbooks (topology client, inference engine, metric rules), testing patterns |
| `.agents/skills/patio-review/SKILL.md` | Reviewing a PR or diff: procedure, severity rubric, checklist, known issues |
| `.github/instructions/*.instructions.md` | Per-area rules that Copilot applies by path (topology, engine/API, metrics, tests) |

Agents without native skill discovery should open these files directly.

## Keeping this knowledge base current

When you change a convention, contract, or command, or fix a listed known issue, update the affected files in
the same PR: this file, the matching `.github/instructions/*.instructions.md`, and the relevant
`.agents/skills/**/references/*.md`. `CLAUDE.md` is a symlink to this file.

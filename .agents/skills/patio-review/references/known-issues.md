# Known issues and documentation drift

Every entry below was verified against the code: by reading it and, where marked "verified", by running it
with Python 3.12, pydantic 2.14, uvicorn 0.54, and prometheus-client 0.26. Line numbers were current when this
file was written; search for the quoted code if they have moved.

How to use this file:

- **Reviewers:** flag new code that copies one of these patterns. Don't re-flag pre-existing instances on lines a
  PR doesn't touch.
- **Authors:** don't build on these behaviors. If you fix one, delete its entry in the same PR.

## Code issues

| ID | Location | Problem and impact | Guidance |
|----|----------|--------------------|----------|
| K1 | `src/patio/engine/base.py:49`, `sglang_engine.py:30`, `vllm_engine.py:30` | `inference_engine()` builds a new engine for every LoRA request, and each engine creates an `httpx.AsyncClient` that is never closed. Connections and file descriptors leak with every call | Open the client per call (`async with`) or share one long-lived client |
| K2 | `sglang_engine.py:61,88`, `vllm_engine.py:61,89` | `HTTPStatus(value=response.status_code)` raises `ValueError` for non-standard upstream codes (499, 520, ...). The caller gets a bare `500 Internal Server Error` instead of an `ErrorResponse` (verified) | Pass the status code through as an `int` |
| K3 | `src/patio/api/protocol.py:6,32,55,74` | `pydantic.v1.root_validator` on pydantic v2 models never runs. `TopoInfoRequest()` ignores the `TOPO_TYPE` fallback and fails; `topo_type="bogus"`, an empty `worker_info`, and an empty `endpoint` are all accepted (verified). No route uses these models today | Use `model_validator`/`field_validator` |
| K4 | `src/patio/app.py:156` | `--scrape-engine-metrics` is declared with `type=bool`, so `false`, `False`, and `0` all mean `True`. Only an empty value disables scraping (verified) | Use `action="store_true"` or `BooleanOptionalAction` |
| K5 | `src/patio/app.py:30,144` | `--enable-fastapi-docs` is parsed but never read. `/docs` and `/openapi.json` are always served (verified) | Pass `docs_url`/`openapi_url` from the flag, if docs should be optional |
| K6 | `src/patio/app.py:43` | The middleware labels `http_requests_total`/`http_request_latency_seconds` with the raw path, so every distinct URL (404 scans included) creates a new time series. Label cardinality is unbounded (verified) | Label with the matched route template, or a fixed value for unmatched routes |
| K7 | `src/patio/topo/client/sgl_topo_client.py:127` | `register()` runs `del worker_info["port"]` outside its `try`, so a missing `port` raises `KeyError` instead of returning `False`, even though `get_worker_endpoint` defaults the port to 30000. At startup the process exits; in the heartbeat a warning is logged every interval (verified) | Validate inside the `try`; use `pop("port", None)` |
| K8 | `sgl_topo_client.py:105,154,187`, `vllm_topo_client.py:86,124` | Every topology call uses `utils.retry(retry_times=60, interval=3)`, so a single call can block for minutes. `SGLangGroupTopoClient.unregister()` on the SIGTERM path can outlast the pod grace period, leaving a stale worker registered at the router | Bound the shutdown path with a deadline and fail fast on 4xx (see `add-topo-client.md`) |
| K9 | `src/patio/app.py:113` | The result of `wait_engine_ready()` is ignored, so registration goes ahead even if the engine never became healthy | Decide explicitly: fail, or register anyway with a warning |
| K10 | `src/patio/app.py:115-116`, `pyproject.toml:15` | The SIGTERM/SIGINT handler that calls `unregister()` runs only on uvicorn ≥ 0.29, which restores and re-raises captured signals after graceful shutdown. `pyproject.toml` allows `uvicorn>=0.15.0`; on 0.15–0.28 uvicorn replaces the handler and `unregister()` never runs (verified against 0.15.0, 0.28.1, and 0.29.0 sources) | Raise the uvicorn floor, or hook shutdown through FastAPI lifespan |
| K11 | `src/patio/config.py:7` | `EXIT_EVENT` is never set, so the heartbeat thread runs until the process exits. That works only because the thread is a daemon | Don't rely on `EXIT_EVENT` for cleanup |
| K12 | `src/patio/topo/utils.py:13`, `topo/client/base_topo_client.py:12`, `sgl_topo_client.py:47,51,55` | Inconsistent env access: `from patio.envs import ...` freezes values at import time and ignores `reload(envs)` (verified), and `sgl_topo_client.get_worker_endpoint` reads `RBG_*` from `os.environ` directly | New code reads `envs.X` |
| K13 | `src/patio/envs.py:36`, `base_topo_client.py:40` | `HEARTBEAT_INTERVAL` defaults to the int `10` but is a `str` when set from the environment (verified). `app.py:121` casts it; the legacy `send_heartbeat` passes it uncast to `time.sleep` | Parse in `envs.py`: `int(os.getenv("HEARTBEAT_INTERVAL", "10"))` |
| K14 | `src/patio/engine/base.py:31,109` | `InferenceEngine.ready()` (unused) makes a blocking `requests.get` inside `async def` | Don't copy; use `httpx.AsyncClient` |
| K15 | `src/patio/topo/utils.py:63-66`, `src/patio/envs.py:46-50` | Legacy hostname helpers build `{group}-{role}-{index}.{group}-{role}` without the `s-` service prefix that the current clients use | Use `sgl_topo_client.get_rbg_endpoint` |
| K16 | `src/patio/config.py:5` | Unused `from tarfile import SUPPORTED_TYPES` | Remove during a cleanup |

## Documentation drift (the code is correct)

| ID | Location | README/doc says | Code does |
|----|----------|-----------------|-----------|
| D1 | `README.md:149-151` | Env vars `GROUP_NAME`, `ROLE_NAME`, `ROLE_INDEX` | Reads `RBG_GROUP_NAME`, `RBG_ROLE_NAME`, `RBG_ROLE_INDEX` (`envs.py:39-41`). The un-prefixed names are RBG's deprecated v1alpha1 names, and PR #4 proposes a fallback |
| D2 | `README.md:152` | `HEARTBEAT_INTERVAL` default `30` | Default `10` (`envs.py:36`) |
| D3 | `README.md:62` | `sglang:num_prompt_tokens_total` maps to `patio:input_tokens_total` | No such rule (`metrics/engine_metric_rules.py`) |
| D4 | `README.md:142` | `INFERENCE_ENGINE_ENDPOINT` default `http://localhost:8000` | No default (`envs.py:15`). Without it the LoRA routes return 400 and no engine metrics are scraped |
| D5 | `README.md:128-134` | `--enable-fastapi-docs` enables the docs; `--instance-info` isn't listed | The flag is ignored (K5), and `--instance-info` drives topology registration |
| D6 | `README.md` env table | Missing variables | `POD_IP`, `METRIC_SCRAPE_PATH`, and the legacy `SGL_ROUTER_ROLE_NAME`/`SGL_ROUTER_PORT` fallbacks are read |
| D7 | `doc/engine.md:71-78`, `doc/metrics.md:65` | Only SGLang endpoints and SGLang metric mappings | vLLM uses `/v1/load_lora_adapter` and `/v1/unload_lora_adapter` and has its own mapping table (`VLLM_METRIC_STANDARD_RULES`) |
| D8 | `doc/topo.md:1`, `doc/examples.md:18` | "Coming soon..." placeholder; `cd <project-dir>/python` | Topology is implemented (see `.agents/skills/patio-dev/references/architecture.md` §9); the package uses the `src/` layout |

# Patio configuration reference

This is the source of truth for configuration as implemented. `envs.py` reads every value **once at import
time**; the topology clients also read `POD_IP` and some `RBG_*` variables straight from `os.environ`. Where
README disagrees with this file, the code is right (see
`.agents/skills/patio-review/references/known-issues.md`).

## Environment variables

### Engine and metrics

| Variable | Code default | Type | Read by | Notes |
|----------|--------------|------|---------|-------|
| `INFERENCE_ENGINE` | `sglang` | str, case-insensitive | `engine/base.inference_engine`, `app._init_metrics` | `sglang` or `vllm`. Any other value makes the LoRA routes return 400, and startup fails when metric scraping is enabled and `INFERENCE_ENGINE_ENDPOINT` is set (no metric rules for it) |
| `INFERENCE_ENGINE_VERSION` | `v0.5.3` | str | `engine/base.inference_engine` | Informational only; no version check exists |
| `INFERENCE_ENGINE_ENDPOINT` | none | URL | `app._init_metrics`, `engine/base.inference_engine` | Optional for startup, required for LoRA operations and engine-metric scraping. Without it the LoRA routes return 400 and no engine metrics are scraped |
| `METRIC_SCRAPE_PATH` | `/metrics` | path | `app._init_metrics` | Combined with the endpoint via `urljoin`, so an absolute path replaces any path already in the endpoint |

### Topology (router/proxy registration)

| Variable | Code default | Type | Read by | Notes |
|----------|--------------|------|---------|-------|
| `TOPO_TYPE` | none | str | `app.run_topo_client` (the legacy `api/protocol` validators that reference it never run) | Used when `--instance-info` has no `topo_type`. `SGLang` or `vLLM`, matched case-insensitively (deployed profiles use `sglang`) |
| `ROUTER_ROLE_NAME` | none, falls back to `SGL_ROUTER_ROLE_NAME` | str | both topology clients | RBG role of the router/proxy; Patio always targets pod index 0 |
| `ROUTER_PORT` | none, falls back to `SGL_ROUTER_PORT` | str | both topology clients | Router/proxy port |
| `TOPO_CONNECT_TIMEOUT` | `3` | float seconds | both topology clients | Connect timeout for every topology request |
| `TOPO_HEALTH_CHECK_TIMEOUT` | `30` | float seconds | `wait_engine_ready` | Read timeout for the engine health check |
| `TOPO_REGISTER_TIMEOUT` | `10` | float seconds | `register`, `unregister` | Read timeout for router/proxy calls |
| `HEARTBEAT_INTERVAL` | `10` | int seconds | `app.run_topo_client` heartbeat | The default is the int `10`, but a value from the environment is a `str`; `app.py` wraps it in `int()` |
| `RBG_GROUP_NAME` | none | str | `envs.GROUP_NAME`; `os.environ` in `sgl_topo_client.get_worker_endpoint` | Injected by RBG. Required to build the router address |
| `RBG_ROLE_NAME` | none | str | `envs.ROLE_NAME`; `os.environ` in `sgl_topo_client` | Injected by RBG. Used for the worker's DNS name when `POD_IP` is unset |
| `RBG_ROLE_INDEX` | none | str | `envs.ROLE_INDEX`; `os.environ` in `sgl_topo_client` | Injected by RBG. Used for the worker's DNS name when `POD_IP` is unset |
| `POD_IP` | none | str | topology clients (`os.environ`) | Worker address and health-check host; usually set through the downward API. Without it, the health check targets `localhost` |

### Defined but unused at runtime

`PROMETHEUS_MULTIPROC_DIR`, `HEALTH_CHECK_INTERVAL`, `LWS_GROUP_SIZE`, and `TOPO_CONFIG_FILE` (read only by the
legacy `topo/utils.write_config_file`), plus `SCHEDULER_ROLE_NAME` and `TOPO_REGISTER_ENDPOINT` (read only by the
legacy `envs.get_topo_register_endpoint`).

## CLI flags (`patio`, `python -m patio.app`)

| Flag | Default | Behavior |
|------|---------|----------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `9091` (`config.DEFAULT_PATIO_PORT`) | Bind port |
| `--log-level` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `--instance-info` | `""` | JSON string `{"topo_type": "...", "data": {...}}`; `topo_type` is optional. An empty value disables topology registration |
| `--scrape-engine-metrics` | `True` | Declared with `type=bool`, so every non-empty value, `false` included, means `True`. Only an empty value (`--scrape-engine-metrics=`) disables scraping |
| `--enable-fastapi-docs` | `False` | Parsed but never used. `/docs` and `/openapi.json` are always served |

## Adding configuration

1. **Define it in `envs.py`** next to related settings, with a string default and explicit parsing:
   `MY_TIMEOUT = float(os.getenv("MY_TIMEOUT", "5"))`. Prefix integration-specific variables with the
   integration name (for example `MOTOR_*`), and reuse `ROUTER_ROLE_NAME`/`ROUTER_PORT` for router addresses.
2. **Read it as `envs.MY_TIMEOUT`** at call time. Avoid `from patio.envs import MY_TIMEOUT`, which freezes the
   value and defeats `importlib.reload(envs)` in tests.
3. **Renames keep a fallback:** `NEW = os.getenv("NEW") or os.getenv("OLD")`. Test the new name, the old name,
   and precedence when both are set; `tests/mock_tests/test_sgl_topo_client.py` has examples.
4. **CLI flags** go into `app.main()`'s argparse. Booleans use `action="store_true"` or
   `argparse.BooleanOptionalAction`, never `type=bool`.
5. **Document it in the same PR:** the README env-var or CLI table (name, description, a default that matches
   the code exactly) and this file. If it changes deployment behavior, mention it in the PR description so
   RBG profiles can be updated.

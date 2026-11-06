---
applyTo: "src/patio/topo/**"
---

# Topology clients (router/proxy registration)

A `GroupTopoClient` registers the local engine with a router or proxy at startup, registers it again on
every heartbeat (`HEARTBEAT_INTERVAL`), and has `unregister()` called from the SIGTERM/SIGINT handler (after
uvicorn's graceful shutdown, and only on uvicorn ≥ 0.29). Each router management protocol gets its own client
class. Playbook: `.agents/skills/patio-dev/references/add-topo-client.md`.

- Every HTTP request passes `timeout=(envs.TOPO_CONNECT_TIMEOUT, <read timeout>)`. The read timeout is
  `envs.TOPO_HEALTH_CHECK_TIMEOUT` for health checks and `envs.TOPO_REGISTER_TIMEOUT` for register/unregister.
- `register()` must be idempotent (the heartbeat calls it repeatedly) and must not mutate the caller's
  `worker_info`; copy it first. Validate inputs inside the `try` so that a failure returns `False`
  instead of raising.
- `unregister()` runs on the SIGTERM path (on uvicorn ≥ 0.29). Bound its total duration well below the pod
  termination grace period (Kubernetes default 30 s) and fail fast on 4xx. Do not use
  `utils.retry(retry_times=60, interval=3)` there.
- Keep the error contract of the existing clients: catch the exception, call `logger.error(...)` with the
  URL, status code, and body, call `traceback.print_exc()`, and return a `bool`.
- Read configuration as `envs.X` at call time, not via `from patio.envs import X`. The router address comes from
  `ROUTER_ROLE_NAME`/`ROUTER_PORT`; the legacy `SGL_ROUTER_*` fallback lives in `envs.py`. The registered worker
  address is an explicit `data.instance` (if the protocol supports one), otherwise `{POD_IP}:{port}`, otherwise
  RBG DNS `{group}-{role}-{index}.s-{group}-{role}:{port}`. Never register `localhost`. Engine health checks
  use `POD_IP` or `localhost`.
- For a new client, add a canonical constant to `config.SUPPORTED_TOPO_TYPES` and a case-insensitive branch in
  `topo/factory.py::create_topo_client`. Deployed profiles use lowercase `TOPO_TYPE` values.
- Raise a clear `RuntimeError` in `__init__` when required configuration is missing, so startup aborts
  instead of the error appearing on every heartbeat.
- `topo/server/*`, `GroupTopoClient.send_heartbeat`, `envs.get_topo_register_endpoint`, and
  `topo/utils.get_worker_endpoint`/`write_config_file` are legacy and unused at runtime. Don't build on them.
- Do not flag these deliberate choices: the log + traceback + `return False` pattern, the no-op
  `VLLMProxyTopoClient.unregister()`, and clients not re-checking `worker_info is None`
  (`app.run_topo_client` already validates it).

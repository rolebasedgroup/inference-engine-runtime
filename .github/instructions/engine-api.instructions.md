---
applyTo: "src/patio/engine/**,src/patio/api/**,src/patio/app.py"
---

# Engine proxies, HTTP API, and app lifecycle

- Request and response models subclass `NoExtraBaseModel` (`extra="forbid"`). Use pydantic v2 APIs
  (`model_validator`, `field_validator`, `model_dump`). Never use `pydantic.v1`, whose decorators never run on
  v2 models.
- Handled failures go back to the caller as an `ErrorResponse` built with `api/response_error.py`. The router uses
  `invalid_error` (400) when the engine can't be built from configuration. Engine methods use `server_error`
  (500, or the upstream status), and the base class uses `not_implemented_error` (501). Routers convert the
  result to `JSONResponse(status_code=response.code)`. The error shape is a public contract; keep it stable.
- Pass upstream status codes through as plain `int`. `HTTPStatus(value=...)` raises `ValueError` for
  non-standard codes such as 499 or 520, and the client then gets a bare 500.
- Code in `async def` must not block. Use `httpx.AsyncClient` with explicit timeouts, and no `requests` or
  `time.sleep`. Don't create an `httpx.AsyncClient` per request without closing it; `inference_engine()` does
  this today, and the pattern must not spread.
- Engine-native paths: SGLang uses `/load_lora_adapter` and `/unload_lora_adapter`; vLLM uses
  `/v1/load_lora_adapter` and `/v1/unload_lora_adapter`.
- A new engine needs a lowercase branch in `engine/base.py::inference_engine()` and metric rules in
  `metrics/engine_metric_rules.py::get_metric_standard_rules`. Without rules, startup fails whenever metric
  scraping is enabled (the default) and `INFERENCE_ENGINE_ENDPOINT` is set.
- A new endpoint needs a router module in `api/`, `app.include_router(...)` in `app.py`, and updates to the
  README "API Reference" section and `doc/api_protocol.md`.
- In `app.py`, anything that runs before `uvicorn.run` delays Patio's own `/health`; topology registration
  blocks there. The signal handlers installed by `run_topo_client` run only after uvicorn's graceful
  shutdown, and only on uvicorn ≥ 0.29. Background threads must be daemon threads.
- Boolean CLI flags use `action="store_true"` or `argparse.BooleanOptionalAction`, never `type=bool`.
  Document every new flag in the README CLI table.

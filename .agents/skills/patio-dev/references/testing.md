# Patio testing patterns

All unit tests are offline: pytest, `unittest.mock`, and `pytest-asyncio`. They live in
`tests/mock_tests/test_<module>.py` as plain test functions. Run them with `pytest` from the repository root
after `pip install -e ".[dev]"`. The `tests/e2e/*.sh` scripts need a running engine and a GPU and are not
part of normal validation.

## Patch where the name is looked up

```python
@patch("patio.topo.client.vllm_topo_client.requests.post")   # not "requests.post"
@patch("patio.api.lora_router.inference_engine")             # the router's imported name
@patch("patio.metrics.engine_collector.requests.Session")    # created in EngineCollector.__init__
@patch("patio.engine.base.envs")                             # replace the whole envs module for the factory
```

## Environment variables

`patio.envs` reads the environment once at import time, so set the variables and reload the module:

```python
from importlib import reload
from patio import envs


def test_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "8000")
    reload(envs)
    ...
```

- Code that reads `envs.X` sees the reloaded value. Modules that did `from patio.envs import X`
  (`topo/utils.py`, `topo/client/base_topo_client.py`) keep the import-time value, so patch their attribute
  instead.
- `monkeypatch` restores `os.environ` after the test, but the reloaded `envs` keeps the test values. If a later
  test needs defaults, it must set or delete what it relies on and reload.
- For renamed variables, test the new name, the legacy name, and precedence when both are set (see
  `test_get_sgl_router_endpoint_*` in `tests/mock_tests/test_sgl_topo_client.py`).

## Topology clients

- `SGLangGroupTopoClient` is a singleton: set `SGLangGroupTopoClient._instance = None` before constructing it.
- `utils.retry` sleeps at least 1 s between attempts. To test a single attempt, patch it at the client's lookup
  site:

  ```python
  with patch(
      "patio.topo.client.vllm_topo_client.utils.retry",
      side_effect=lambda func, **_: func(),
  ):
      assert client.register("", worker_info) is False
  ```

- Assert the exact outbound request, because it is the router contract:

  ```python
  mock_post.assert_called_once_with(
      "http://demo-router-0.s-demo-router:8000/workers",
      json={"worker_type": "prefill", "url": "http://10.0.0.8:30000"},
      headers={"Content-Type": "application/json"},
      timeout=(4.0, 12.0),
  )
  ```

## Async engines

No `asyncio_mode` is configured, so every async test needs `@pytest.mark.asyncio`.

```python
@pytest.mark.asyncio
async def test_load_lora_adapter_http_error(sglang_engine):
    response = MagicMock(status_code=500, text="Internal Server Error")
    with patch.object(sglang_engine, "client") as mock_client:
        mock_client.post = AsyncMock(return_value=response)
        result = await sglang_engine.load_lora_adapter(
            LoadLoraAdapterRequest(lora_name="a", lora_path="/p")
        )
    assert result.code == 500
```

## HTTP routes

```python
from fastapi.testclient import TestClient
from patio.app import app

client = TestClient(app)


@patch("patio.api.lora_router.inference_engine")
def test_load_lora_adapter_success(mock_inference_engine):
    engine = AsyncMock()
    engine.load_lora_adapter.return_value = "Success: ..."
    mock_inference_engine.return_value = engine
    response = client.post("/load_lora_adapter", json={"lora_name": "a", "lora_path": "/p"})
    assert response.status_code == 200
```

Importing `patio.app` doesn't start the server or call `configure_logging`; only `main()` does that.

## Metrics

Push Prometheus text through `EngineCollector` into a fresh `CollectorRegistry` and assert on the
`generate_latest` output. A complete template is in `references/add-engine-metrics.md`. Don't register test
collectors on the shared `patio_registry`.

## What every change should cover

- The success path, with exact outbound calls asserted.
- Non-2xx responses, raised exceptions and timeouts, and missing or invalid configuration.
- Idempotency for `register()`, and bounded behavior for `unregister()`.
- No real network calls and no unnecessary sleeps; the whole suite runs in about 5 s.

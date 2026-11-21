---
applyTo: "tests/**"
---

# Tests

- Unit tests live in `tests/mock_tests/test_<module>.py`, use pytest with mocks only (no network, no real
  engine), and run with `pytest`. The `tests/e2e/*.sh` scripts need a real engine and a GPU, so they are not
  part of normal validation.
- Patch a name where the code under test looks it up, for example
  `patch("patio.topo.client.vllm_topo_client.requests.post")`, `patch("patio.api.lora_router.inference_engine")`,
  or `patch("patio.metrics.engine_collector.requests.Session")`.
- `patio.envs` reads the environment at import time. Call `monkeypatch.setenv`/`delenv`, then
  `importlib.reload(envs)`. Modules that did `from patio.envs import X` don't see the reload, so patch their
  attribute instead. monkeypatch restores `os.environ` afterwards, but not the reloaded module.
- Reset singletons before constructing them: `SGLangGroupTopoClient._instance = None`.
- Avoid real retry sleeps, since `utils.retry` waits at least 1 s between attempts. Patch
  `patio.topo.client.<module>.utils.retry` with `side_effect=lambda func, **_: func()`.
- Async engine methods: mark tests with `@pytest.mark.asyncio` (no asyncio mode is configured, so the marker is
  required) and use `patch.object(engine, "client")` with `AsyncMock`. HTTP routes:
  `fastapi.testclient.TestClient(app)`.
- Assert outbound contracts exactly:
  `assert_called_once_with(url, json=..., headers=..., timeout=(...))`.
- Cover the failure paths: non-2xx responses, exceptions and timeouts, missing env vars, and the legacy env
  fallback together with its precedence.

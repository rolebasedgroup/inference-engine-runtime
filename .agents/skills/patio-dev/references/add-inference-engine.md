# Playbook: add an inference engine backend

Use this to support a new `INFERENCE_ENGINE` value. A new engine backend needs a proposal
(`proposals/README.md`). Background: `references/architecture.md` §5–§8.

## Files to touch

| File | Change |
|------|--------|
| `src/patio/engine/<name>_engine.py` | `InferenceEngine` subclass that proxies the engine's native LoRA API (skeleton below) |
| `src/patio/engine/base.py` | Lowercase branch in `inference_engine()` with a lazy import, like the existing ones |
| `src/patio/metrics/engine_metric_rules.py` | `<NAME>_METRIC_STANDARD_RULES` and a branch in `get_metric_standard_rules`. **Required:** without it, startup raises `ValueError` whenever metric scraping is enabled (the default) and `INFERENCE_ENGINE_ENDPOINT` is set |
| `tests/mock_tests/test_<name>_engine.py` | Engine tests (template below) |
| `tests/mock_tests/test_engine_base.py` | Factory case, following `test_inference_engine_vllm` |
| `tests/mock_tests/test_metrics.py` | Rule tests (see `references/add-engine-metrics.md`) |
| `README.md`, `doc/engine.md`, `doc/metrics.md` | Accepted `INFERENCE_ENGINE` values, engine endpoints, metric mapping table |

## Rules

- Use the engine's native paths and verify them against the engine's documentation for the supported version.
  SGLang uses `/load_lora_adapter`; vLLM uses `/v1/load_lora_adapter`.
- Set an explicit timeout on every call. The existing engines allow 30 minutes for a LoRA load (large
  adapters) and httpx's 5 s default for unload; choose deliberately.
- Map failures with `api/response_error.py`: request exceptions become `server_error(...)` (500); a non-2xx
  upstream status becomes `server_error(message=..., code=<int status>)`. Pass the int as is, because
  `HTTPStatus(value=...)` raises on non-standard codes.
- Don't add a long-lived `httpx.AsyncClient` per engine instance: `inference_engine()` builds a new engine on
  every request, so such clients leak. Open the client per call, as the skeleton does.
- Never log secrets such as API keys or tokens.
- Leave an operation unimplemented if the engine can't do it; the base class returns 501.

## Skeleton

```python
from typing import Optional, Union
from urllib.parse import urljoin

import httpx

from patio.api.protocol import (
    ErrorResponse,
    LoadLoraAdapterRequest,
    UnLoadLoraAdapterRequest,
)
from patio.api.response_error import server_error
from patio.engine.base import InferenceEngine
from patio.logger import init_logger

logger = init_logger(__name__)

LOAD_TIMEOUT_SECONDS = 30 * 60
UNLOAD_TIMEOUT_SECONDS = 60


class ExampleInferenceEngine(InferenceEngine):
    def __init__(
        self, name: str, version: str, endpoint: str, api_key: Optional[str] = None
    ):
        super().__init__(name, version, endpoint)
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else None

    async def _post(self, path: str, payload: dict, timeout: float) -> httpx.Response:
        async with httpx.AsyncClient(timeout=timeout) as client:
            return await client.post(
                urljoin(self.endpoint, path), json=payload, headers=self.headers
            )

    async def load_lora_adapter(
        self, request: LoadLoraAdapterRequest
    ) -> Union[ErrorResponse, str]:
        name = request.lora_name
        try:
            response = await self._post(
                "/v1/load_lora_adapter", request.model_dump(), LOAD_TIMEOUT_SECONDS
            )
        except Exception as e:
            logger.error(f"Failed to load LoRA adapter '{name}': {e}")
            return server_error(f"Failed to load LoRA adapter '{name}'")
        if response.status_code != 200:
            logger.error(
                f"Failed to load LoRA adapter '{name}', status_code: {response.status_code}, error: {response.text}"
            )
            return server_error(
                message=f"Failed to load LoRA adapter '{name}', message: {response.text}",
                code=response.status_code,
            )
        return f"Success: LoRA adapter '{name}' added successfully."

    async def unload_lora_adapter(
        self, request: UnLoadLoraAdapterRequest
    ) -> Union[ErrorResponse, str]:
        name = request.lora_name
        try:
            response = await self._post(
                "/v1/unload_lora_adapter", request.model_dump(), UNLOAD_TIMEOUT_SECONDS
            )
        except Exception as e:
            logger.error(f"Failed to unload LoRA adapter '{name}': {e}")
            return server_error(f"Failed to unload LoRA adapter '{name}'")
        if response.status_code != 200:
            logger.error(
                f"Failed to unload LoRA adapter '{name}', status_code: {response.status_code}, error: {response.text}"
            )
            return server_error(
                message=f"Failed to unload LoRA adapter '{name}', message: {response.text}",
                code=response.status_code,
            )
        return f"Success: LoRA adapter '{name}' unloaded successfully."
```

## Test template

```python
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from patio.api.protocol import LoadLoraAdapterRequest, UnLoadLoraAdapterRequest
from patio.engine.example_engine import ExampleInferenceEngine


@pytest.fixture
def engine():
    return ExampleInferenceEngine("example", "v1", "http://test-endpoint")


@pytest.mark.asyncio
async def test_load_lora_adapter_success(engine):
    response = MagicMock(status_code=200, text="ok")
    with patch.object(engine, "_post", AsyncMock(return_value=response)) as post:
        result = await engine.load_lora_adapter(
            LoadLoraAdapterRequest(lora_name="a", lora_path="/p")
        )

    assert result == "Success: LoRA adapter 'a' added successfully."
    post.assert_awaited_once_with(
        "/v1/load_lora_adapter", {"lora_name": "a", "lora_path": "/p"}, 30 * 60
    )


@pytest.mark.asyncio
async def test_load_lora_adapter_keeps_nonstandard_status(engine):
    response = MagicMock(status_code=520, text="upstream error")
    with patch.object(engine, "_post", AsyncMock(return_value=response)):
        result = await engine.load_lora_adapter(
            LoadLoraAdapterRequest(lora_name="a", lora_path="/p")
        )

    assert result.code == 520
    assert result.type == "ServerError"


@pytest.mark.asyncio
async def test_unload_lora_adapter_request_error(engine):
    error = httpx.ConnectError("connection refused")
    with patch.object(engine, "_post", AsyncMock(side_effect=error)):
        result = await engine.unload_lora_adapter(
            UnLoadLoraAdapterRequest(lora_name="a")
        )

    assert result.code == 500
```

Also add a factory test in `tests/mock_tests/test_engine_base.py` (patch `patio.engine.base.envs`, set
`INFERENCE_ENGINE` in mixed case), plus a route-level test through `TestClient(app)` if the endpoint behavior
changes.

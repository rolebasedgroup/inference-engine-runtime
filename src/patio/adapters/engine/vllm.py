"""vLLM engine probe."""

from __future__ import annotations

import os
from urllib.parse import urljoin

import httpx
from prometheus_client.parser import text_string_to_metric_families

from patio import envs
from patio.adapters.engine.base import EngineProbe


class VLLMEngineProbe(EngineProbe):
    """Single-shot vLLM engine readiness and quiescence checks."""

    def __init__(self, worker_info: dict):
        self.worker_info = worker_info

    @property
    def health_endpoint(self) -> str:
        port = self.worker_info.get("port", "8000")
        host = os.getenv("POD_IP") or "localhost"
        return f"http://{host}:{port}/health"

    async def ready(self) -> bool:
        async with httpx.AsyncClient(
            timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT)
        ) as client:
            response = await client.get(self.health_endpoint)
        return response.status_code == 200

    async def quiescent(self) -> bool:
        if not envs.INFERENCE_ENGINE_ENDPOINT:
            return False

        endpoint = urljoin(envs.INFERENCE_ENGINE_ENDPOINT, envs.METRIC_SCRAPE_PATH)
        async with httpx.AsyncClient(
            timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT)
        ) as client:
            response = await client.get(endpoint)
        if response.status_code != 200:
            return False

        metrics = {
            family.name: family
            for family in text_string_to_metric_families(response.text)
        }
        for name in ("vllm:num_requests_running", "vllm:num_requests_waiting"):
            family = metrics.get(name)
            if family is None:
                return False
            if any(sample.value != 0 for sample in family.samples):
                return False
        return True

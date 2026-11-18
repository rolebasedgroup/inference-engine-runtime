"""vLLM engine probe."""

from __future__ import annotations

import asyncio
from urllib.parse import urljoin

import requests
from prometheus_client.parser import text_string_to_metric_families

from patio import envs
from patio.adapters.engine.base import EngineProbe
from patio.topo.client.vllm_topo_client import VLLMProxyTopoClient


class VLLMEngineProbe(EngineProbe):
    """Adapter around the existing vLLM engine readiness check."""

    def __init__(self, client: VLLMProxyTopoClient, worker_info: dict):
        self.client = client
        self.worker_info = worker_info

    async def ready(self) -> bool:
        return await asyncio.to_thread(self.client.wait_engine_ready, self.worker_info)

    async def quiescent(self) -> bool:
        return await asyncio.to_thread(self._quiescent_sync)

    def _quiescent_sync(self) -> bool:
        if not envs.INFERENCE_ENGINE_ENDPOINT:
            return False

        endpoint = urljoin(envs.INFERENCE_ENGINE_ENDPOINT, envs.METRIC_SCRAPE_PATH)
        try:
            response = requests.get(
                endpoint,
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT),
            )
        except requests.RequestException:
            return False
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

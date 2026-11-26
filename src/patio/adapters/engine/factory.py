"""Engine probe factory."""

from __future__ import annotations

from patio.adapters.engine.base import EngineProbe
from patio.adapters.engine.sglang import SGLangEngineProbe
from patio.adapters.engine.vllm import VLLMEngineProbe
from patio.topo.factory import create_topo_client


def create_engine_probe(topo_type: str, worker_info: dict) -> EngineProbe:
    """Create an engine probe for the configured topology type."""

    normalized = topo_type.lower()
    if normalized == "sglang":
        return SGLangEngineProbe(worker_info)
    if normalized == "vllm":
        # The probe keeps the legacy client for compatibility; readiness is a
        # single bounded check and the event executor owns retries.
        client = create_topo_client(normalized, worker_info)
        return VLLMEngineProbe(client, worker_info)
    raise ValueError(f"unsupported topology type: {topo_type}")

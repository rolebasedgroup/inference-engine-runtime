"""Engine probe factory."""

from __future__ import annotations

from patio.adapters.engine.base import EngineProbe
from patio.adapters.engine.sglang import SGLangEngineProbe
from patio.adapters.engine.vllm import VLLMEngineProbe


def create_engine_probe(topo_type: str, worker_info: dict) -> EngineProbe:
    """Create an engine probe for the configured topology type."""

    normalized = topo_type.lower()
    if normalized == "sglang":
        return SGLangEngineProbe(worker_info)
    if normalized == "vllm":
        return VLLMEngineProbe(worker_info)
    raise ValueError(f"unsupported topology type: {topo_type}")

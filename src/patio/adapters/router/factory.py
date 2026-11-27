"""Router adapter factory."""

from __future__ import annotations

from patio.adapters.router.base import RouterAdapter
from patio.adapters.router.sglang import SGLangRouterAdapter
from patio.adapters.router.vllm import VLLMRouterAdapter


def create_router_adapter(topo_type: str, worker_info: dict) -> RouterAdapter:
    """Create a router adapter for the configured topology type."""

    normalized = topo_type.lower()
    if normalized == "sglang":
        return SGLangRouterAdapter(worker_info)
    if normalized == "vllm":
        return VLLMRouterAdapter(worker_info)
    raise ValueError(f"unsupported topology type: {topo_type}")

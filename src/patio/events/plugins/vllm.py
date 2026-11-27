"""vLLM event callbacks."""

from __future__ import annotations

from patio.adapters.engine.vllm import VLLMEngineProbe
from patio.adapters.router.vllm import VLLMRouterAdapter
from patio.events.registry import register_callback


def register_vllm_callbacks(
    router_adapter: VLLMRouterAdapter,
    engine_probe: VLLMEngineProbe,
    registry=None,
) -> None:
    """Register vLLM-specific callbacks, optionally in a local registry."""

    @register_callback("engine_wait_ready", registry=registry)
    async def engine_wait_ready() -> None:
        if not await engine_probe.ready():
            raise RuntimeError("vLLM engine is not ready")

    @register_callback("router_onboard", registry=registry)
    async def router_onboard() -> None:
        if not await router_adapter.add():
            raise RuntimeError("failed to register vLLM instance")

    @register_callback("router_offboard", registry=registry)
    async def router_offboard() -> None:
        if not await router_adapter.remove():
            raise RuntimeError("failed to unregister vLLM instance")

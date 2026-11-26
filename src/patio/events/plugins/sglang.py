"""SGLang event callbacks."""

from __future__ import annotations

from patio.adapters.engine.sglang import SGLangEngineProbe
from patio.adapters.router.sglang import SGLangRouterAdapter
from patio.events.registry import register_callback


def register_sglang_callbacks(
    router_adapter: SGLangRouterAdapter,
    engine_probe: SGLangEngineProbe,
    registry=None,
) -> None:
    """Register SGLang-specific callbacks, optionally in a local registry."""

    @register_callback("engine_wait_ready", registry=registry)
    async def engine_wait_ready() -> None:
        if not await engine_probe.ready():
            raise RuntimeError("SGLang engine is not ready")

    @register_callback("router_onboard", registry=registry)
    async def router_onboard() -> None:
        if not await router_adapter.add():
            raise RuntimeError("failed to register SGLang worker")

    @register_callback("router_offboard", registry=registry)
    async def router_offboard() -> None:
        if not await router_adapter.remove():
            raise RuntimeError("failed to unregister SGLang worker")

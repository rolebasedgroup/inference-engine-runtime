"""Assemble the event executor and engine-specific callbacks."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from patio import envs
from patio.adapters.engine.factory import create_engine_probe
from patio.adapters.router.factory import create_router_adapter
from patio.events.executor import EventExecutor, MemorySignalSource, SignalSource
from patio.events.loader import load_event_bindings
from patio.events.registry import callback_plugins, reset_callback_registry
from patio.events.plugins.sglang import register_sglang_callbacks
from patio.events.plugins.vllm import register_vllm_callbacks
from patio.lifecycle.signal_file import FileSignalSource
from patio.lifecycle.signal_label import KubernetesSignalLabelSource
from patio.logger import init_logger


class PatioApplication:
    """Run event callbacks for one Patio process."""

    def __init__(
        self,
        worker_info: dict[str, Any],
        topo_type: str,
        events_config: str | Path,
        signal_source: SignalSource | None = None,
        poll_interval: float | None = None,
        heartbeat_interval: float | None = None,
        retry_times: int | None = None,
        retry_interval: float | None = None,
    ):
        self.worker_info = worker_info
        self.topo_type = topo_type
        self.events_config = Path(events_config)
        self.signal_source = signal_source or self._default_signal_source()
        self.logger = init_logger(__name__)
        self.poll_interval = (
            poll_interval
            if poll_interval is not None
            else float(os.getenv("EVENT_POLL_INTERVAL", "1"))
        )
        self.heartbeat_interval = (
            heartbeat_interval
            if heartbeat_interval is not None
            else float(envs.HEARTBEAT_INTERVAL)
        )
        self.retry_times = (
            retry_times
            if retry_times is not None
            else int(os.getenv("EVENT_CALLBACK_RETRY_TIMES", "60"))
        )
        self.retry_interval = (
            retry_interval
            if retry_interval is not None
            else float(os.getenv("EVENT_CALLBACK_RETRY_INTERVAL", "3"))
        )

        self._stop_event = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None

        self.router_adapter = create_router_adapter(topo_type, worker_info)
        self.engine_probe = create_engine_probe(topo_type, worker_info)

        reset_callback_registry()
        if topo_type.lower() == "sglang":
            register_sglang_callbacks(self.router_adapter, self.engine_probe)
        elif topo_type.lower() == "vllm":
            register_vllm_callbacks(self.router_adapter, self.engine_probe)
        else:
            raise ValueError(f"unsupported topology type: {topo_type}")

        start_callbacks, end_callbacks, hook_bindings = load_event_bindings(
            self.events_config
        )
        # Keep the legacy heartbeat contract: when startup onboards this
        # worker, keep re-registering so a router restart does not silently
        # drop the worker while this process stays healthy.
        onboard_callback = callback_plugins.get("router_onboard")
        offboard_callback = callback_plugins.get("router_offboard")
        self._heartbeat_enabled = (
            self.heartbeat_interval > 0
            and onboard_callback is not None
            and any(callback is onboard_callback for callback in start_callbacks)
        )
        # Signals that hand this worker's membership decision back to the
        # controller must stop the heartbeat from re-registering.
        self._offboard_signals = frozenset(
            signal
            for signal, callback in hook_bindings
            if offboard_callback is not None and callback is offboard_callback
        )

        self.executor = EventExecutor(
            start_callbacks=start_callbacks,
            end_callbacks=end_callbacks,
            hook_bindings=hook_bindings,
            signal_source=self.signal_source,
            poll_interval=self.poll_interval,
            retry_times=self.retry_times,
            retry_interval=self.retry_interval,
            on_hook_processed=self._on_hook_processed,
        )

    def _default_signal_source(self) -> SignalSource:
        if os.getenv("LIFECYCLE_RI_NAME") and os.getenv("KUBERNETES_SERVICE_HOST"):
            # The signal source selects how signals are read. Clearing always
            # goes through the Kubernetes API: the downwardAPI file is
            # read-only, so a file source composes the API source for clear.
            mode = os.getenv("LIFECYCLE_SIGNAL_SOURCE", "kubernetes")
            if mode == "file":
                return FileSignalSource(clear_source=KubernetesSignalLabelSource())
            if mode != "kubernetes":
                raise ValueError(
                    f"unsupported LIFECYCLE_SIGNAL_SOURCE: {mode!r} "
                    "(supported: kubernetes, file)"
                )
            return KubernetesSignalLabelSource()
        return MemorySignalSource()

    async def start(self) -> None:
        """Run start callbacks and begin polling for hook signals."""

        await self.executor.run_start()
        self._stop_event.clear()
        self._loop_task = asyncio.create_task(
            self.executor.run_forever(self._stop_event),
            name="patio-event-executor",
        )
        if self._heartbeat_enabled:
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name="patio-router-heartbeat",
            )
            self.logger.info(
                "router heartbeat started",
                extra={"interval": self.heartbeat_interval},
            )

    async def _on_hook_processed(self, signal: str) -> None:
        if signal in self._offboard_signals:
            self._heartbeat_enabled = False
            self.logger.info(
                "router heartbeat disabled after offboard signal",
                extra={"signal": signal},
            )

    async def _heartbeat_loop(self) -> None:
        """Re-register with the router to recover from router state loss."""

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.heartbeat_interval
                )
                return
            except asyncio.TimeoutError:
                pass

            if not self._heartbeat_enabled:
                self.logger.info("router heartbeat stopped")
                return

            try:
                if not await self.router_adapter.add():
                    raise RuntimeError("router re-registration returned failure")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("router heartbeat failed: %s", exc)

    async def stop(self) -> None:
        """Stop polling and run end callbacks."""

        self._stop_event.set()
        for task, name in ((self._loop_task, "event executor"), (self._heartbeat_task, "router heartbeat")):
            if task is None:
                continue
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self.logger.exception("%s stopped with an error", name)
        self._loop_task = None
        self._heartbeat_task = None
        await self.executor.run_end()


def parse_worker_instance_info(worker_instance_info: str) -> tuple[dict[str, Any], str]:
    """Parse ``--instance-info`` and derive topology type."""

    try:
        worker_dict = json.loads(worker_instance_info)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid worker instance info JSON: {exc}") from exc

    if not isinstance(worker_dict, dict):
        raise ValueError("worker instance info must be a JSON object")

    topo_type = worker_dict.get("topo_type") or os.getenv("TOPO_TYPE")
    if not topo_type:
        raise ValueError(
            "topo_type must be set in instance info or the TOPO_TYPE environment variable"
        )

    worker_info = worker_dict.get("data")
    if not isinstance(worker_info, dict):
        raise ValueError('worker instance info must contain a "data" object')

    return worker_info, topo_type

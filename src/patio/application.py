"""Assemble the event executor and engine-specific callbacks."""

from __future__ import annotations

import asyncio
import functools
import json
import os
from pathlib import Path
from typing import Any

from patio import envs
from patio.adapters.engine.factory import create_engine_probe
from patio.adapters.router.factory import create_router_adapter
from patio.events.executor import EventExecutor, MemorySignalSource, SignalSource
from patio.events.loader import load_event_bindings
from patio.events.plugins.sglang import register_sglang_callbacks
from patio.events.plugins.vllm import register_vllm_callbacks
from patio.events.registry import callback_plugins
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
        signal_interval: float | None = None,
        end_retry_times: int | None = None,
        end_retry_interval: float | None = None,
        end_timeout: float | None = None,
    ):
        self.worker_info = worker_info
        self.topo_type = topo_type
        self.events_config = Path(events_config)
        self.logger = init_logger(__name__)
        self.poll_interval = (
            poll_interval
            if poll_interval is not None
            else envs.get_event_poll_interval()
        )
        self.heartbeat_interval = (
            heartbeat_interval
            if heartbeat_interval is not None
            else float(envs.HEARTBEAT_INTERVAL)
        )
        self.retry_times = (
            retry_times
            if retry_times is not None
            else envs.get_event_callback_retry_times()
        )
        self.retry_times = max(1, int(self.retry_times))
        self.retry_interval = (
            retry_interval
            if retry_interval is not None
            else envs.get_event_callback_retry_interval()
        )
        self.signal_interval = (
            signal_interval
            if signal_interval is not None
            else envs.get_event_signal_interval()
        )
        self.end_retry_times = (
            end_retry_times
            if end_retry_times is not None
            else envs.get_event_end_callback_retry_times()
        )
        self.end_retry_interval = (
            end_retry_interval
            if end_retry_interval is not None
            else envs.get_event_end_callback_retry_interval()
        )
        self.end_timeout = (
            end_timeout
            if end_timeout is not None
            else envs.get_event_end_callback_timeout()
        )

        self._stop_event = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        # Membership operations are serialized: an in-flight heartbeat
        # registration must finish before an offboard hook removes the worker.
        self._membership_lock = asyncio.Lock()
        self._heartbeat_suspended = False

        self.router_adapter = create_router_adapter(topo_type, worker_info)
        self.engine_probe = create_engine_probe(topo_type, worker_info)

        # Build an application-local registry. Callbacks registered by an
        # embedder through the public register_callback() API are preserved;
        # assembling an application must not erase global plugin state.
        plugins: dict[str, Any] = dict(callback_plugins)
        normalized_topo = topo_type.lower()
        if normalized_topo == "sglang":
            register_sglang_callbacks(self.router_adapter, self.engine_probe, plugins)
        elif normalized_topo == "vllm":
            register_vllm_callbacks(self.router_adapter, self.engine_probe, plugins)
        else:
            raise ValueError(f"unsupported topology type: {topo_type}")

        start_callbacks, end_callbacks, hook_bindings = load_event_bindings(
            self.events_config, plugins=plugins
        )
        self.signal_source = (
            signal_source
            if signal_source is not None
            else self._select_signal_source(hook_bindings)
        )

        # Keep the legacy heartbeat contract: when startup onboards this
        # worker, keep re-registering so a router restart does not silently
        # drop the worker while this process stays healthy.
        onboard_callback = plugins.get("router_onboard")
        offboard_callback = plugins.get("router_offboard")
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
        # A later onboard signal can resume a heartbeat suspended by offboard.
        self._onboard_signals = frozenset(
            signal
            for signal, callback in hook_bindings
            if onboard_callback is not None and callback is onboard_callback
        )

        self.executor = EventExecutor(
            start_callbacks=start_callbacks,
            end_callbacks=end_callbacks,
            hook_bindings=tuple(
                (signal, self._wrap_hook_callback(signal, callback))
                for signal, callback in hook_bindings
            ),
            signal_source=self.signal_source,
            poll_interval=self.poll_interval,
            retry_times=self.retry_times,
            retry_interval=self.retry_interval,
            signal_interval=self.signal_interval,
            end_retry_times=self.end_retry_times,
            end_retry_interval=self.end_retry_interval,
            end_timeout=self.end_timeout,
            on_hook_processed=self._on_hook_processed,
        )

    @staticmethod
    def _select_signal_source(hook_bindings) -> SignalSource:
        """Select the configured signal source and fail on missing config."""

        requested = os.getenv("LIFECYCLE_SIGNAL_SOURCE")
        mode = requested or "kubernetes"
        in_cluster = bool(os.getenv("KUBERNETES_SERVICE_HOST"))
        role_instance_name = os.getenv("LIFECYCLE_RI_NAME")

        # The signal source selects how signals are read. Clearing always
        # goes through the Kubernetes API: the downwardAPI file is read-only,
        # so a file source composes the API source for clear.
        if mode == "file":
            # FileSignalSource validates the Kubernetes clear-path settings
            # eagerly instead of silently disabling hooks.
            expected_signals = {signal for signal, _ in hook_bindings}
            return FileSignalSource(expected_signals=expected_signals)
        if mode != "kubernetes":
            raise ValueError(
                f"unsupported LIFECYCLE_SIGNAL_SOURCE: {mode!r} "
                "(supported: kubernetes, file)"
            )
        if in_cluster:
            if hook_bindings and not role_instance_name:
                raise ValueError(
                    "LIFECYCLE_RI_NAME is required when hook callbacks are "
                    "configured inside Kubernetes"
                )
            if not role_instance_name:
                return MemorySignalSource()
            return KubernetesSignalLabelSource()
        if requested is not None:
            raise ValueError(
                "LIFECYCLE_SIGNAL_SOURCE=kubernetes requires LIFECYCLE_RI_NAME "
                "and in-cluster Kubernetes API access"
            )
        if hook_bindings:
            init_logger(__name__).warning(
                "hook callbacks are configured but no Kubernetes signal source "
                "is available; hooks will not fire"
            )
        return MemorySignalSource()

    def _wrap_hook_callback(self, signal: str, callback):
        """Serialize offboard callbacks with heartbeat re-registration."""

        if signal not in self._offboard_signals:
            return callback

        @functools.wraps(callback)
        async def offboard_callback() -> None:
            async with self._membership_lock:
                # Suspend while holding the lock so an in-flight heartbeat
                # add() has completed and no new one can start.
                self._suspend_heartbeat()
                await callback()

        return offboard_callback

    def _suspend_heartbeat(self) -> None:
        if not self._heartbeat_enabled:
            return
        self._heartbeat_enabled = False
        self._heartbeat_suspended = True
        self.logger.info("router heartbeat suspended before offboard callback")

    def _resume_heartbeat(self) -> None:
        if not self._heartbeat_suspended:
            return
        self._heartbeat_enabled = True
        self._heartbeat_suspended = False
        self._ensure_heartbeat_task()
        self.logger.info("router heartbeat resumed after onboard signal")

    def _ensure_heartbeat_task(self) -> None:
        if self._stop_event.is_set() or not self._heartbeat_enabled:
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name="patio-router-heartbeat",
        )

    async def start(self) -> None:
        """Run start callbacks and begin polling for hook signals."""

        validate = getattr(self.signal_source, "validate", None)
        if validate is not None:
            await validate()
        await self.executor.run_start()
        self._stop_event.clear()
        if self.executor.hook_bindings:
            self._loop_task = asyncio.create_task(
                self.executor.run_forever(self._stop_event),
                name="patio-event-executor",
            )
        if self._heartbeat_enabled:
            self._ensure_heartbeat_task()
            self.logger.info(
                "router heartbeat started",
                extra={"interval": self.heartbeat_interval},
            )

    async def _on_hook_processed(self, signal: str) -> None:
        if signal in self._offboard_signals:
            # The heartbeat was already suspended before the callback ran.
            # Reassert the suspended state after a successful side effect,
            # including when a later signal clear fails.
            self._suspend_heartbeat()
            return
        if signal in self._onboard_signals:
            self._resume_heartbeat()

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

            try:
                # Recheck inside the lock: an offboard callback may have
                # suspended the heartbeat while this task waited for it.
                async with self._membership_lock:
                    if not self._heartbeat_enabled:
                        self.logger.info("router heartbeat stopped")
                        return
                    for attempt in range(1, self.retry_times + 1):
                        try:
                            if not await self.router_adapter.add():
                                raise RuntimeError(
                                    "router re-registration returned failure"
                                )
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            if attempt >= self.retry_times:
                                raise
                            self.logger.warning(
                                "router heartbeat failed, retrying",
                                extra={"attempt": attempt, "next_attempt_in": self.retry_interval},
                            )
                            await asyncio.sleep(self.retry_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("router heartbeat failed: %s", exc)

    async def stop(self) -> None:
        """Stop polling and run end callbacks."""

        self._stop_event.set()
        self._heartbeat_enabled = False
        tasks = (
            (self._loop_task, "event executor"),
            (self._heartbeat_task, "router heartbeat"),
        )
        for task, name in tasks:
            if task is None:
                continue
            # Shutdown must be bounded.  If a hook or heartbeat callback is
            # still in flight, cancel it so the end-callback budget is not
            # consumed by an operation that is already being terminated.
            task.cancel()
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

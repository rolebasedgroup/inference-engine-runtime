"""Serial event executor for start, end, and hook callbacks."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Protocol

from patio.logger import init_logger


EventCallback = Callable[[], Awaitable[None]]
HookBinding = tuple[str, EventCallback]


class SignalChangedError(RuntimeError):
    """The signal value changed before Patio could clear it."""


class SignalSource(Protocol):
    """A source of signals that should trigger hook callbacks."""

    async def signals(self) -> set[str]:
        """Return the signals currently present."""

    async def clear(self, signal: str) -> None:
        """Clear one signal after its callback has completed."""


class MemorySignalSource:
    """In-memory signal source, mainly useful for tests."""

    def __init__(self, initial: Iterable[str] | None = None):
        self._signals: set[str] = set(initial or ())

    async def signals(self) -> set[str]:
        return set(self._signals)

    async def clear(self, signal: str) -> None:
        self._signals.discard(signal)

    async def add(self, signal: str) -> None:
        self._signals.add(signal)


class EventExecutor:
    """Execute start, end, and hook callbacks in configuration order."""

    def __init__(
        self,
        start_callbacks: Sequence[EventCallback],
        end_callbacks: Sequence[EventCallback],
        hook_bindings: Sequence[HookBinding],
        signal_source: SignalSource,
        poll_interval: float = 1.0,
        retry_times: int = 60,
        retry_interval: float = 3.0,
        signal_interval: float = 60.0,
        end_retry_times: int | None = None,
        end_retry_interval: float | None = None,
        end_timeout: float | None = None,
        on_hook_processed: Callable[[str], Awaitable[None]] | None = None,
        logger=None,
    ):
        self.start_callbacks = tuple(start_callbacks)
        self.end_callbacks = tuple(end_callbacks)
        self.hook_bindings = tuple(hook_bindings)
        self.signal_source = signal_source
        self.poll_interval = poll_interval
        # Start and end callbacks talk to the engine and router while those
        # services may still be starting or shutting down. Keep the retry
        # semantics of the legacy topology client for transient failures.
        self.retry_times = max(1, int(retry_times))
        self.retry_interval = float(retry_interval)
        # End callbacks run after SIGTERM and must finish inside the Pod's
        # termination grace period. They get a separate, shorter budget so a
        # retry loop does not end in SIGKILL.
        self.end_retry_times = (
            max(1, int(end_retry_times))
            if end_retry_times is not None
            else self.retry_times
        )
        self.end_retry_interval = (
            float(end_retry_interval)
            if end_retry_interval is not None
            else self.retry_interval
        )
        # End callbacks must fit inside the Pod termination grace period. A
        # retry count alone is not a wall-clock budget, so bound the whole end
        # phase as well.
        self.end_timeout = (
            float(end_timeout)
            if end_timeout is not None
            else 20.0
        )
        # The downwardAPI label file lags behind RoleInstance label removal
        # by tens of seconds, so a cleared signal can be re-detected right
        # after its callback ran. Enforce a minimum interval between
        # executions of the same signal instead of relying on callback
        # idempotency alone.
        self.signal_interval = max(0.0, float(signal_interval))
        self._last_signal_run: dict[str, float] = {}
        self.on_hook_processed = on_hook_processed
        self.logger = logger or init_logger(__name__)

    async def _run_callbacks(
        self,
        callbacks: Sequence[EventCallback],
        event: str,
        retry_times: int,
        retry_interval: float,
    ) -> None:
        for callback in callbacks:
            for attempt in range(1, retry_times + 1):
                started = time.perf_counter()
                callback_name = getattr(callback, "__name__", repr(callback))
                self.logger.info(
                    "event callback started",
                    extra={
                        "event": event,
                        "callback": callback_name,
                        "attempt": attempt,
                    },
                )
                try:
                    await callback()
                except Exception:
                    if attempt >= retry_times:
                        self.logger.exception(
                            "event callback failed",
                            extra={
                                "event": event,
                        "callback": callback_name,
                                "attempt": attempt,
                                "duration": time.perf_counter() - started,
                            },
                        )
                        raise
                    self.logger.warning(
                        "event callback failed, retrying",
                        extra={
                            "event": event,
                        "callback": callback_name,
                            "attempt": attempt,
                            "next_attempt_in": retry_interval,
                        },
                    )
                    await asyncio.sleep(retry_interval)
                else:
                    self.logger.debug(
                        "event callback completed",
                        extra={
                            "event": event,
                        "callback": callback_name,
                            "attempt": attempt,
                            "duration": time.perf_counter() - started,
                        },
                    )
                    break

    async def run_start(self) -> None:
        """Run all start callbacks in order."""

        await self._run_callbacks(
            self.start_callbacks,
            "start",
            self.retry_times,
            self.retry_interval,
        )

    async def run_end(self) -> None:
        """Run all end callbacks in order."""

        try:
            await asyncio.wait_for(
                self._run_callbacks(
                    self.end_callbacks,
                    "end",
                    self.end_retry_times,
                    self.end_retry_interval,
                ),
                timeout=self.end_timeout,
            )
        except asyncio.TimeoutError:
            self.logger.error(
                "end callback timeout exceeded",
                extra={
                    "timeout": self.end_timeout,
                    "callbacks": [
                        getattr(callback, "__name__", repr(callback))
                        for callback in self.end_callbacks
                    ],
                },
            )

    def _signal_ready(self, signal: str, now: float) -> bool:
        last = self._last_signal_run.get(signal)
        return last is None or now - last >= self.signal_interval

    async def run_once(self) -> int:
        """Run all matching, cooldown-eligible hook callbacks.

        Returns the number of successfully processed signals.
        """

        signals = await self.signal_source.signals()
        now = time.perf_counter()
        present_signals = {
            signal for signal, _ in self.hook_bindings if signal in signals
        }
        matching_signals = {
            signal for signal in present_signals if self._signal_ready(signal, now)
        }
        suppressed = present_signals - matching_signals
        if suppressed:
            self.logger.debug(
                "hook signals suppressed by execution interval: signals=%s",
                ",".join(sorted(suppressed)),
                extra={
                    "signals": sorted(suppressed),
                    "signal_interval": self.signal_interval,
                },
            )
        if not matching_signals:
            return 0

        self.logger.info(
            "hook signals detected: signals=%s",
            ",".join(sorted(matching_signals)),
            extra={
                "signals": sorted(matching_signals),
                "signal_count": len(matching_signals),
            },
        )

        processed = 0
        for signal, callback in self.hook_bindings:
            if signal not in matching_signals:
                continue

            callback_name = getattr(callback, "__name__", repr(callback))
            started = time.perf_counter()
            self._last_signal_run[signal] = started
            self.logger.info(
                "hook callback started: signal=%s callback=%s",
                signal,
                callback_name,
                extra={"signal": signal, "callback": callback_name},
            )
            try:
                await callback()
            except Exception:
                self.logger.exception(
                    "hook callback failed: signal=%s callback=%s",
                    signal,
                    callback_name,
                    extra={
                        "signal": signal,
                        "callback": callback_name,
                        "duration": time.perf_counter() - started,
                    },
                )
                # Continue with the other matching signals. Returning here
                # would starve every binding configured after a persistently
                # failing signal.
                continue

            # Notify the application after the callback's side effect has
            # succeeded.  A later clear failure must not undo an onboard or
            # leave an offboard heartbeat suspended.
            if self.on_hook_processed is not None:
                try:
                    await self.on_hook_processed(signal)
                except Exception:
                    self.logger.exception(
                        "hook processed notification failed: signal=%s",
                        signal,
                        extra={"signal": signal},
                    )

            try:
                # Clearing is conditional on the processed signal value. In
                # file mode a re-read can be stale, so the source itself must
                # perform the value check atomically.
                await self.signal_source.clear(signal)
            except SignalChangedError:
                self.logger.warning(
                    "signal changed while callback ran, skipping clear: signal=%s",
                    signal,
                    extra={"signal": signal},
                )
            except Exception:
                self.logger.exception(
                    "failed to clear signal: signal=%s",
                    signal,
                    extra={"signal": signal},
                )
                continue

            self.logger.info(
                "hook signal cleared: signal=%s",
                signal,
                extra={"signal": signal},
            )

            self.logger.info(
                "hook callback completed: signal=%s callback=%s duration=%.3f",
                signal,
                callback_name,
                time.perf_counter() - started,
                extra={
                    "signal": signal,
                    "callback": callback_name,
                    "duration": time.perf_counter() - started,
                },
            )
            processed += 1

        return processed

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Poll the signal source until ``stop_event`` is set.

        One failed poll (for example a Kubernetes API timeout or DNS error)
        must not end the polling task: log it and retry after the interval.
        """

        while not stop_event.is_set():
            processed = 0
            try:
                processed = await self.run_once()
            except Exception:
                self.logger.exception("signal polling iteration failed")

            if processed == 0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval)
                except asyncio.TimeoutError:
                    pass

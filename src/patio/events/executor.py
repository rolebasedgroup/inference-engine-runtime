"""Serial event executor for start, end, and hook callbacks."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Protocol

from patio.logger import init_logger


EventCallback = Callable[[], Awaitable[None]]
HookBinding = tuple[str, EventCallback]


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
        self.on_hook_processed = on_hook_processed
        self.logger = logger or init_logger(__name__)

    async def _run_callbacks(self, callbacks: Sequence[EventCallback], event: str) -> None:
        for callback in callbacks:
            for attempt in range(1, self.retry_times + 1):
                started = time.perf_counter()
                try:
                    await callback()
                except Exception:
                    if attempt >= self.retry_times:
                        self.logger.exception(
                            "event callback failed",
                            extra={
                                "event": event,
                                "callback": getattr(callback, "__name__", repr(callback)),
                                "attempt": attempt,
                                "duration": time.perf_counter() - started,
                            },
                        )
                        raise
                    self.logger.warning(
                        "event callback failed, retrying",
                        extra={
                            "event": event,
                            "callback": getattr(callback, "__name__", repr(callback)),
                            "attempt": attempt,
                            "next_attempt_in": self.retry_interval,
                        },
                    )
                    await asyncio.sleep(self.retry_interval)
                else:
                    self.logger.debug(
                        "event callback completed",
                        extra={
                            "event": event,
                            "callback": getattr(callback, "__name__", repr(callback)),
                            "attempt": attempt,
                            "duration": time.perf_counter() - started,
                        },
                    )
                    break

    async def run_start(self) -> None:
        """Run all start callbacks in order."""

        await self._run_callbacks(self.start_callbacks, "start")

    async def run_end(self) -> None:
        """Run all end callbacks in order."""

        await self._run_callbacks(self.end_callbacks, "end")

    async def run_once(self) -> int:
        """Run at most one matching hook callback and clear its signal."""

        signals = await self.signal_source.signals()
        if not signals:
            return 0

        self.logger.info(
            "hook signals detected: signals=%s",
            ",".join(sorted(signals)),
            extra={"signals": sorted(signals), "signal_count": len(signals)},
        )

        matched = False
        for signal, callback in self.hook_bindings:
            if signal not in signals:
                continue

            matched = True
            callback_name = getattr(callback, "__name__", repr(callback))
            started = time.perf_counter()
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
                return 0

            try:
                # Re-read before clearing: the controller may have replaced the
                # label value while the callback was running. Only remove the
                # label when it still carries the processed signal.
                current_signals = await self.signal_source.signals()
                if signal in current_signals:
                    await self.signal_source.clear(signal)
                    self.logger.info(
                        "hook signal cleared: signal=%s",
                        signal,
                        extra={"signal": signal},
                    )
                else:
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
                return 0

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
            if self.on_hook_processed is not None:
                try:
                    await self.on_hook_processed(signal)
                except Exception:
                    self.logger.exception(
                        "hook processed notification failed: signal=%s",
                        signal,
                        extra={"signal": signal},
                    )
            return 1

        if not matched:
            self.logger.warning(
                "hook signals detected but no callback binding matched: signals=%s",
                ",".join(sorted(signals)),
                extra={"signals": sorted(signals)},
            )
        return 0

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

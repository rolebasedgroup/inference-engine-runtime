"""Validated event callback configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class HookBinding:
    """One signal-to-callback mapping."""

    signal: str
    callback: str


@dataclass(frozen=True)
class EventCallbacksConfig:
    """Validated, unresolved event configuration."""

    start: tuple[str, ...]
    end: tuple[str, ...]
    hook: tuple[HookBinding, ...]


class EventConfigError(ValueError):
    """Raised when event callback configuration is invalid."""


def parse_signal(value: Any) -> str:
    """Validate and normalize a ``key=value`` signal."""

    if not isinstance(value, str):
        raise EventConfigError(f"signal must be a string, got {type(value).__name__}")
    if value.count("=") != 1:
        raise EventConfigError(
            f"signal must contain exactly one '=': {value!r}"
        )
    key, signal_value = value.split("=", 1)
    key = key.strip()
    signal_value = signal_value.strip()
    if not key or not signal_value:
        raise EventConfigError(f"signal key and value must be non-empty: {value!r}")
    return f"{key}={signal_value}"


def _validate_name_list(value: Any, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise EventConfigError(f"event_callbacks.{field} must be a list of callback names")
    if any(not item.strip() for item in value):
        raise EventConfigError(f"event_callbacks.{field} contains an empty callback name")
    if len(set(value)) != len(value):
        raise EventConfigError(f"event_callbacks.{field} contains duplicate callback names")
    return tuple(value)


def _validate_hook_list(value: Any) -> tuple[HookBinding, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise EventConfigError("event_callbacks.hook must be a list")

    bindings: list[HookBinding] = []
    seen_signals: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise EventConfigError(f"event_callbacks.hook[{index}] must be an object")
        if set(item.keys()) != {"signal", "callback"}:
            raise EventConfigError(
                f"event_callbacks.hook[{index}] must contain only signal and callback"
            )

        signal = parse_signal(item["signal"])
        callback = item["callback"]
        if not isinstance(callback, str) or not callback.strip():
            raise EventConfigError(
                f"event_callbacks.hook[{index}].callback must be a non-empty string"
            )
        if signal in seen_signals:
            raise EventConfigError(f"duplicate signal binding: {signal}")

        seen_signals.add(signal)
        bindings.append(HookBinding(signal=signal, callback=callback))

    return tuple(bindings)


def parse_event_callbacks(data: Any) -> EventCallbacksConfig:
    """Parse and validate the ``event_callbacks`` mapping."""

    if data is None:
        return EventCallbacksConfig(start=(), end=(), hook=())
    if not isinstance(data, Mapping):
        raise EventConfigError("event_callbacks must be an object")
    if not set(data.keys()) <= {"start", "end", "hook"}:
        unknown = sorted(set(data.keys()) - {"start", "end", "hook"})
        raise EventConfigError(f"unknown event_callbacks fields: {unknown}")

    start = _validate_name_list(data.get("start"), "start")
    end = _validate_name_list(data.get("end"), "end")
    hook = _validate_hook_list(data.get("hook"))
    return EventCallbacksConfig(start=start, end=end, hook=hook)

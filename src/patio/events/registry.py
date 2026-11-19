"""Global callback registry for event plugins."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar


EventCallback = Callable[[], Awaitable[None]]

callback_plugins: dict[str, EventCallback] = {}

_T = TypeVar("_T", bound=EventCallback)


def register_callback(name: str) -> Callable[[_T], _T]:
    """Register an async callback under a unique name."""

    def decorator(callback: _T) -> _T:
        if name in callback_plugins:
            raise ValueError(f"Callback plugin already registered: {name}")
        callback_plugins[name] = callback
        return callback

    return decorator


def reset_callback_registry() -> None:
    """Clear the global registry. Intended for tests and process restarts."""

    callback_plugins.clear()

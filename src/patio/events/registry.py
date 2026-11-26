"""Global callback registry for event plugins."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import TypeVar


EventCallback = Callable[[], Awaitable[None]]

callback_plugins: dict[str, EventCallback] = {}

_T = TypeVar("_T", bound=EventCallback)


def register_callback(
    name: str, registry: MutableMapping[str, EventCallback] | None = None
) -> Callable[[_T], _T]:
    """Register an async callback under a unique name.

    When ``registry`` is provided the callback is registered locally instead of
    mutating the process-global plugin registry.
    """

    def decorator(callback: _T) -> _T:
        target = callback_plugins if registry is None else registry
        if name in target:
            raise ValueError(f"Callback plugin already registered: {name}")
        target[name] = callback
        return callback

    return decorator


def reset_callback_registry() -> None:
    """Clear the global registry. Intended for tests and process restarts."""

    callback_plugins.clear()

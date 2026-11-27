"""Event callback registry and executor."""

from patio.events.executor import EventExecutor
from patio.events.loader import load_event_bindings
from patio.events.models import HookBinding
from patio.events.registry import (
    EventCallback,
    callback_plugins,
    register_callback,
    reset_callback_registry,
)

__all__ = [
    "EventCallback",
    "EventExecutor",
    "HookBinding",
    "callback_plugins",
    "load_event_bindings",
    "register_callback",
    "reset_callback_registry",
]

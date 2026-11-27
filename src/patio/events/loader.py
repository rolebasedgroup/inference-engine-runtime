"""Load event callback configuration and resolve callback names."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

from patio.events.models import EventCallbacksConfig, parse_event_callbacks
from patio.events.registry import EventCallback, callback_plugins


ResolvedEventBindings = tuple[
    tuple[EventCallback, ...], tuple[EventCallback, ...], tuple[tuple[str, EventCallback], ...]
]


def resolve_event_callbacks(
    config: EventCallbacksConfig,
    plugins: Mapping[str, EventCallback] | None = None,
) -> ResolvedEventBindings:
    """Resolve callback names to function references."""

    registry = callback_plugins if plugins is None else dict(plugins)

    missing = [
        name
        for name in (*config.start, *config.end, *(binding.callback for binding in config.hook))
        if name not in registry
    ]
    if missing:
        raise ValueError(f"unknown callback plugins: {missing}")

    start_callbacks = tuple(registry[name] for name in config.start)
    end_callbacks = tuple(registry[name] for name in config.end)
    hook_bindings = tuple(
        (binding.signal, registry[binding.callback]) for binding in config.hook
    )
    return start_callbacks, end_callbacks, hook_bindings


def load_event_bindings(
    path: str | Path,
    plugins: Mapping[str, EventCallback] | None = None,
) -> ResolvedEventBindings:
    """Load and validate ``events.yaml`` and resolve callback names."""

    config_path = Path(path)
    try:
        raw: Any = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"event callback configuration not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid event callback YAML: {exc}") from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping) or set(raw.keys()) != {"event_callbacks"}:
        raise ValueError("event callback YAML must contain an event_callbacks object")

    config = parse_event_callbacks(raw["event_callbacks"])
    return resolve_event_callbacks(config, plugins)

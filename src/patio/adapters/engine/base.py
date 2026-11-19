"""Engine probe contract."""

from __future__ import annotations

from abc import ABC, abstractmethod


class EngineProbe(ABC):
    """Engine readiness and quiescence checks."""

    @abstractmethod
    async def ready(self) -> bool:
        """Return whether the engine is ready."""

    @abstractmethod
    async def quiescent(self) -> bool:
        """Return whether the engine has no in-flight or queued work."""

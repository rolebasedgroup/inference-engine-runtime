"""Router adapter contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum


class MembershipState(str, Enum):
    """Router membership state for one worker."""

    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


class RouterAdapter(ABC):
    """Protocol adapter for router membership operations."""

    @abstractmethod
    async def add(self) -> bool:
        """Register this worker with the router."""

    @abstractmethod
    async def remove(self) -> bool:
        """Remove this worker from the router."""

    @abstractmethod
    async def query(self) -> MembershipState:
        """Return the router's current membership state."""

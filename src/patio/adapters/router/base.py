"""Router adapter contract."""

from __future__ import annotations

import socket
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
        """Return a best-effort membership state for this worker."""


def is_dns_resolution_failure(exc: BaseException) -> bool:
    """Return whether the exception chain contains a DNS resolution failure.

    HTTP client error text differs between musl, glibc, and library versions,
    so inspect the underlying ``socket.gaierror`` instead of matching strings.
    """

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, socket.gaierror):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False

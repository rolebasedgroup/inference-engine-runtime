"""vLLM proxy adapter."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import requests

from patio import envs
from patio.adapters.router.base import (
    MembershipState,
    RouterAdapter,
)


class VLLMRouterAdapter(RouterAdapter):
    """Direct vLLM proxy protocol implementation."""

    def __init__(self, worker_info: dict[str, Any]):
        self.worker_info = worker_info

    @property
    def proxy_endpoint(self) -> str:
        group_name = envs.GROUP_NAME
        router_role = envs.ROUTER_ROLE_NAME
        router_port = envs.ROUTER_PORT
        if not group_name or not router_role or not router_port:
            raise RuntimeError(
                "RBG_GROUP_NAME, ROUTER_ROLE_NAME, and ROUTER_PORT are required"
            )
        return f"{group_name}-{router_role}-0.s-{group_name}-{router_role}:{router_port}"

    @property
    def instance(self) -> str:
        if self.worker_info.get("instance"):
            return str(self.worker_info["instance"])

        port = self.worker_info.get("port", "8000")
        pod_ip = os.getenv("POD_IP")
        if pod_ip:
            return f"{pod_ip}:{port}"

        group_name = envs.GROUP_NAME
        role_name = envs.ROLE_NAME
        role_index = envs.ROLE_INDEX
        if not group_name or not role_name or not role_index:
            raise RuntimeError(
                "instance, POD_IP, or RBG_GROUP_NAME, RBG_ROLE_NAME, and RBG_ROLE_INDEX are required"
            )
        return f"{group_name}-{role_name}-{role_index}.s-{group_name}-{role_name}:{port}"

    @property
    def worker_type(self) -> str:
        worker_type = self.worker_info.get("type") or self.worker_info.get("worker_type")
        if not worker_type:
            raise RuntimeError("worker type is required")
        return str(worker_type)

    def _payload(self) -> dict[str, Any]:
        # The official vLLM disaggregated proxy demo uses the singular
        # "instance" field, while the vLLM Ascend load-balance proxy uses the
        # plural "instances" field. Keep both fields until vLLM defines one
        # stable, cross-implementation membership contract.
        return {
            "type": self.worker_type,
            "instance": self.instance,
            "instances": [self.instance],
        }

    @staticmethod
    def _request_succeeded(response: requests.Response) -> bool:
        if not 200 <= response.status_code < 300:
            return False
        try:
            body = response.json()
        except ValueError:
            return True
        # vLLM Ascend reports some request errors as HTTP 200 JSON bodies.
        return not isinstance(body, dict) or "error" not in body

    async def add(self) -> bool:
        def register() -> bool:
            response = requests.post(
                f"http://{self.proxy_endpoint}/instances/add",
                json=self._payload(),
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
            )
            return self._request_succeeded(response)

        return await asyncio.to_thread(register)

    async def remove(self) -> bool:
        # vLLM's proxy examples do not expose a stable removal API. Preserve
        # the legacy no-op behavior until a supported contract exists.
        return True

    async def query(self) -> MembershipState:
        # The vLLM proxy does not currently expose a query API.
        return MembershipState.UNKNOWN

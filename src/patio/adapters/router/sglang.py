"""SGLang router adapter."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import requests
from urllib.parse import quote

from patio import envs
from patio.adapters.router.base import MembershipState, RouterAdapter


class SGLangRouterAdapter(RouterAdapter):
    """Direct SGLang router protocol implementation."""

    def __init__(self, worker_info: dict[str, Any]):
        self.worker_info = worker_info
        self.worker_id: str | None = None

    @property
    def router_endpoint(self) -> str:
        group_name = envs.GROUP_NAME
        router_role = envs.ROUTER_ROLE_NAME
        router_port = envs.ROUTER_PORT
        if not group_name or not router_role or not router_port:
            raise RuntimeError(
                "RBG_GROUP_NAME, ROUTER_ROLE_NAME, and ROUTER_PORT are required"
            )
        return f"{group_name}-{router_role}-0.s-{group_name}-{router_role}:{router_port}"

    @property
    def worker_endpoint(self) -> str:
        port = self.worker_info.get("port", "30000")
        pod_ip = os.getenv("POD_IP")
        if pod_ip:
            return f"{pod_ip}:{port}"

        group_name = envs.GROUP_NAME
        role_name = envs.ROLE_NAME
        role_index = envs.ROLE_INDEX
        if not group_name or not role_name or not role_index:
            raise RuntimeError(
                "POD_IP or RBG_GROUP_NAME, RBG_ROLE_NAME, and RBG_ROLE_INDEX are required"
            )
        return f"{group_name}-{role_name}-{role_index}.s-{group_name}-{role_name}:{port}"

    async def add(self) -> bool:
        payload = dict(self.worker_info)
        payload["url"] = f"http://{self.worker_endpoint}"
        payload.pop("port", None)

        def register() -> bool:
            response = requests.post(
                f"http://{self.router_endpoint}/workers",
                json=payload,
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
            )
            if response.status_code != 202:
                return False
            worker_id = response.json().get("worker_id")
            if not worker_id:
                return False
            self.worker_id = worker_id
            return True

        return await asyncio.to_thread(register)

    def _lookup_worker_id_sync(self) -> str | None:
        """Find this worker's id in the router by matching its URL.

        The worker id is opaque: routers up to 0.2.x use the worker URL, and
        0.3.0+ use a UUID. Matching by URL works for both.
        """

        response = requests.get(
            f"http://{self.router_endpoint}/workers",
            timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"failed to list router workers: HTTP {response.status_code}"
            )
        expected_url = f"http://{self.worker_endpoint}"
        for worker in response.json().get("workers", []):
            if not isinstance(worker, dict):
                continue
            if worker.get("url") == expected_url or worker.get("id") == expected_url:
                return str(worker.get("id") or worker.get("url"))
        return None

    async def remove(self) -> bool:
        def unregister() -> bool:
            # Do not rely on the registration performed by this process: the
            # events.yaml may omit start callbacks, or the process may have
            # restarted since onboarding. Ask the router for the current id.
            worker_id = self._lookup_worker_id_sync()
            if worker_id is None:
                # This worker URL is not registered; offboard is complete.
                return True
            encoded_id = quote(worker_id, safe="")
            response = requests.delete(
                f"http://{self.router_endpoint}/workers/{encoded_id}",
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
            )
            if response.status_code in (202, 404):
                # 404 means another actor already removed the worker.
                self.worker_id = None
                return True
            return False

        return await asyncio.to_thread(unregister)

    async def query(self) -> MembershipState:
        # The SGLang router does not currently expose a reliable query API.
        # Use the local registration state until the router adds one.
        return MembershipState.PRESENT if self.worker_id else MembershipState.ABSENT

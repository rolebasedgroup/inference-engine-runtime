"""Observe and clear RoleInstance ``signalLabel`` values."""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import Any

import aiohttp

from patio import envs
from patio.events.executor import SignalChangedError


class KubernetesSignalLabelSource:
    """
    Read and clear labels on one RoleInstance.

    The default path targets the RoleInstance custom resource used by RBG.
    Override the group, version, and plural through environment variables when
    the API contract changes.
    """

    def __init__(
        self,
        name: str | None = None,
        namespace: str | None = None,
        api_group: str | None = None,
        api_version: str | None = None,
        plural: str | None = None,
        api_server: str | None = None,
        token_path: str | None = None,
        ca_path: str | None = None,
        request_timeout: float | None = None,
    ):
        self.name = name or os.getenv("LIFECYCLE_RI_NAME")
        self.namespace = namespace or os.getenv("LIFECYCLE_RI_NAMESPACE") or _read_file(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        )
        self.api_group = api_group or os.getenv(
            "LIFECYCLE_RI_API_GROUP", "workloads.x-k8s.io"
        )
        self.api_version = api_version or os.getenv(
            "LIFECYCLE_RI_API_VERSION", "v1alpha2"
        )
        self.plural = plural or os.getenv("LIFECYCLE_RI_PLURAL", "roleinstances")
        self.api_server = api_server or _default_api_server()
        self.token_path = token_path or "/var/run/secrets/kubernetes.io/serviceaccount/token"
        self.ca_path = ca_path or "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        # Bound each API call so a stalled Kubernetes connection cannot pin the
        # event loop task and delay SIGTERM end callbacks indefinitely.
        self.request_timeout = aiohttp.ClientTimeout(
            total=(
                request_timeout
                if request_timeout is not None
                else float(os.getenv("LIFECYCLE_API_TIMEOUT", "10"))
            )
        )

        if not self.name or not self.namespace or not self.api_server:
            raise ValueError(
                "RoleInstance name, namespace, and Kubernetes API server are required"
            )

    def _url(self) -> str:
        return (
            f"{self.api_server}/apis/{self.api_group}/{self.api_version}"
            f"/namespaces/{self.namespace}/{self.plural}/{self.name}"
        )

    def _headers(self) -> dict[str, str]:
        token = _read_file(self.token_path)
        if not token:
            raise RuntimeError(f"Kubernetes ServiceAccount token not found: {self.token_path}")
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    def _ssl_context(self) -> ssl.SSLContext | bool:
        if envs.is_lifecycle_api_insecure():
            return False
        if not Path(self.ca_path).exists():
            raise FileNotFoundError(
                "Kubernetes CA certificate not found: "
                f"{self.ca_path}; set LIFECYCLE_API_INSECURE=true only for "
                "explicitly insecure deployments"
            )
        return ssl.create_default_context(cafile=self.ca_path)

    async def signals(self) -> set[str]:
        """Return all labels as ``key=value`` strings."""

        async with aiohttp.ClientSession() as session:
            async with session.get(
                self._url(),
                headers=self._headers(),
                ssl=self._ssl_context(),
                timeout=self.request_timeout,
            ) as response:
                response.raise_for_status()
                payload: dict[str, Any] = await response.json()

        labels = payload.get("metadata", {}).get("labels", {})
        if not isinstance(labels, dict):
            raise RuntimeError("RoleInstance metadata.labels is not an object")
        return {f"{key}={value}" for key, value in labels.items()}

    async def validate(self) -> None:
        """Validate credentials, RBAC, and API access at startup."""

        await self.signals()

    async def clear(self, signal: str) -> None:
        """Atomically remove one label using a JSON Patch test and remove."""

        if signal.count("=") != 1:
            raise ValueError(f"invalid signal: {signal!r}")
        key, value = signal.split("=", 1)

        # A merge patch removes the label without checking its value, so a
        # controller update between the callback and this request could be
        # discarded. The test operation makes removal conditional on the
        # signal value that Patio actually processed.
        label_path = f"/metadata/labels/{_escape_json_pointer(key)}"
        patch = [
            {"op": "test", "path": label_path, "value": value},
            {"op": "remove", "path": label_path},
        ]
        headers = self._headers()
        headers["Content-Type"] = "application/json-patch+json"

        async with aiohttp.ClientSession() as session:
            async with session.patch(
                self._url(),
                headers=headers,
                json=patch,
                ssl=self._ssl_context(),
                timeout=self.request_timeout,
            ) as response:
                if response.status == 422:
                    raise SignalChangedError(
                        f"signal changed before clear: {signal!r}"
                    ) from None
                response.raise_for_status()


def _read_file(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _escape_json_pointer(value: str) -> str:
    """Escape one label key for use inside a JSON Patch path."""

    return value.replace("~", "~0").replace("/", "~1")


def _default_api_server() -> str | None:
    host = os.getenv("KUBERNETES_SERVICE_HOST")
    port = os.getenv("KUBERNETES_SERVICE_PORT_HTTPS") or os.getenv(
        "KUBERNETES_SERVICE_PORT"
    )
    if not host or not port:
        return None
    return f"https://{host}:{port}"

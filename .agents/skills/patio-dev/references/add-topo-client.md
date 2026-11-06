# Playbook: add or change a topology (router/proxy) client

Use this to integrate a new router, proxy, or coordinator that Patio registers workers with (for example
MindIE Motor, PR #6), or to change an existing client. Background: `references/architecture.md` §9 and
RFC issue #7 (one router management protocol per `GroupTopoClient`).

## Before writing code

1. **Pin down the protocol**: register/unregister endpoints, payload, success status codes, how the worker is
   identified (generated ID or address), whether re-registration is idempotent, authentication, and any metadata
   the router needs (model name, DP ranks, and so on). Don't reuse an existing client just because the product
   names look alike.
2. **Write a proposal.** A new router integration is a cross-component integration, so `proposals/README.md`
   requires one.
3. **Decide the `TOPO_TYPE` value** (canonical spelling, matched case-insensitively) and the env vars. Reuse
   `ROUTER_ROLE_NAME`/`ROUTER_PORT` for the router address and prefix anything new with the integration
   name.

## Files to touch

| File | Change |
|------|--------|
| `src/patio/topo/client/<name>_topo_client.py` | New `GroupTopoClient` subclass (skeleton below) |
| `src/patio/config.py` | Canonical constant, added to `SUPPORTED_TOPO_TYPES` |
| `src/patio/topo/factory.py` | `elif topo_type.lower() == "<name>":` branch in `create_topo_client` |
| `src/patio/envs.py` | New `<NAME>_*` variables, if any (see `references/configuration.md`) |
| `tests/mock_tests/test_<name>_topo_client.py` | Unit tests (template below) |
| `tests/mock_tests/test_topo_factory.py` | Factory selection, including a lowercase value |
| `README.md` | "Distributed Topology Management" section and the env-var table |
| `doc/<name>_registration.md` and `doc/README.md` | Integration doc linked from the TOC |
| `proposals/NNNN-<title>/` | Proposal |

## Client contract

| Method | Called | Must |
|--------|--------|------|
| `__init__(worker_info)` | Once at startup, from `create_topo_client` | Resolve addresses through `envs.*`/`POD_IP` and raise `RuntimeError` with a clear message when required configuration is missing (startup then exits 1) |
| `wait_engine_ready(worker_info) -> bool` | Once at startup | `GET /health` on the local engine with `utils.retry`; return `False` instead of raising |
| `register(url, worker_info, file_path=None) -> bool` | At startup, then every `HEARTBEAT_INTERVAL` | Idempotent; work on a copy of `worker_info`; validate inside `try`; return `False` on failure. `url` and `file_path` are unused legacy parameters |
| `unregister() -> bool` | Once, from the SIGTERM/SIGINT handler, after uvicorn's graceful shutdown (only on uvicorn ≥ 0.29) | Bounded total time (well under 30 s), fail fast on 4xx. If the router has no unregister API, log it and return `True` |

Every request takes `timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT)` for health checks or
`(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT)` for router calls.

## Skeleton

Placeholder protocol: `POST /workers/add` and `POST /workers/remove`, both with `{"role", "address"}`. Replace it
with the real one.

```python
import os
import time
import traceback
from typing import Optional

import requests

from patio import envs
from patio.logger import init_logger
from patio.topo import utils
from patio.topo.client.base_topo_client import GroupTopoClient
from patio.topo.client.sgl_topo_client import get_rbg_endpoint

logger = init_logger(__name__)

# deadline + one request timeout (3 s connect + 10 s read by default) must stay well under the pod's
# terminationGracePeriodSeconds (Kubernetes default 30 s).
UNREGISTER_DEADLINE_SECONDS = 10


class ExampleTopoClient(GroupTopoClient):
    def __init__(self, worker_info: dict):
        if not (envs.GROUP_NAME and envs.ROUTER_ROLE_NAME and envs.ROUTER_PORT):
            raise RuntimeError(
                "RBG_GROUP_NAME, ROUTER_ROLE_NAME and ROUTER_PORT must be set"
            )
        self.router_endpoint = get_rbg_endpoint(
            envs.GROUP_NAME, envs.ROUTER_ROLE_NAME, "0", envs.ROUTER_PORT
        )
        port = worker_info.get("port", "8000")
        pod_ip = os.getenv("POD_IP")
        # Health checks stay inside the pod; the registered address must be reachable by the router.
        self.health_check_endpoint = f"{pod_ip or 'localhost'}:{port}"
        if worker_info.get("instance"):
            self.address = worker_info["instance"]
        elif pod_ip:
            self.address = f"{pod_ip}:{port}"
        elif envs.ROLE_NAME and envs.ROLE_INDEX:
            self.address = get_rbg_endpoint(
                envs.GROUP_NAME, envs.ROLE_NAME, envs.ROLE_INDEX, port
            )
        else:
            raise RuntimeError("POD_IP or RBG_ROLE_NAME and RBG_ROLE_INDEX must be set")
        self.role: Optional[str] = None

    def wait_engine_ready(self, worker_info: dict) -> bool:
        def f():
            url = f"http://{self.health_check_endpoint}/health"
            resp = requests.get(
                url, timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT)
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"health check failed, url: {url}, status_code: {resp.status_code}, content: {resp.text}"
                )

        try:
            utils.retry(f, retry_times=60, interval=3)
            return True
        except Exception as e:
            logger.error(f"failed to check if worker engine is ready: {e}")
            traceback.print_exc()
            return False

    def register(
        self, url: str, worker_info: dict, file_path: Optional[str] = None
    ) -> bool:
        try:
            role = worker_info.get("type") or worker_info.get("worker_type")
            if role is None:
                raise ValueError("worker type is not set")
            payload = {"role": role, "address": self.address}
            add_url = f"http://{self.router_endpoint}/workers/add"

            def f():
                resp = requests.post(
                    add_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
                )
                if not 200 <= resp.status_code < 300:
                    raise RuntimeError(
                        f"register failed, url: {add_url}, status_code: {resp.status_code}, content: {resp.text}"
                    )

            utils.retry(f, retry_times=60, interval=3)
            self.role = role
            logger.info(f"registered worker {self.address} as {role}")
            return True
        except Exception as e:
            logger.error(f"failed to register worker: {e}")
            traceback.print_exc()
            return False

    def unregister(self) -> bool:
        if self.role is None:
            logger.warning("worker was never registered, skipping unregister")
            return False
        remove_url = f"http://{self.router_endpoint}/workers/remove"
        payload = {"role": self.role, "address": self.address}
        deadline = time.monotonic() + UNREGISTER_DEADLINE_SECONDS
        try:
            while True:
                try:
                    resp = requests.post(
                        remove_url,
                        json=payload,
                        timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
                    )
                    if 200 <= resp.status_code < 300:
                        logger.info(f"unregistered worker {self.address}")
                        return True
                    if 400 <= resp.status_code < 500:
                        logger.error(
                            f"unregister rejected, url: {remove_url}, status_code: {resp.status_code}, content: {resp.text}"
                        )
                        return False
                    logger.warning(
                        f"unregister failed, status_code: {resp.status_code}, content: {resp.text}"
                    )
                except requests.RequestException as e:
                    logger.warning(f"unregister request failed: {e}")
                if time.monotonic() >= deadline:
                    logger.error(
                        f"unregister gave up after {UNREGISTER_DEADLINE_SECONDS}s"
                    )
                    return False
                time.sleep(1)
        except Exception as e:
            logger.error(f"failed to unregister worker: {e}")
            traceback.print_exc()
            return False
```

## Test template

```python
from importlib import reload
from unittest.mock import MagicMock, patch

from patio import envs
from patio.topo.client.example_topo_client import ExampleTopoClient

MODULE = "patio.topo.client.example_topo_client"


def set_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    monkeypatch.setenv("POD_IP", "10.0.0.8")
    monkeypatch.setenv("TOPO_CONNECT_TIMEOUT", "4")
    monkeypatch.setenv("TOPO_REGISTER_TIMEOUT", "12")
    reload(envs)


@patch(f"{MODULE}.requests.post")
def test_register_posts_payload(mock_post, monkeypatch):
    set_env(monkeypatch)
    mock_post.return_value = MagicMock(status_code=200, text="ok")
    worker_info = {"type": "prefill", "port": 8000}
    client = ExampleTopoClient(worker_info)

    assert client.register("", worker_info) is True
    mock_post.assert_called_once_with(
        "http://demo-router-0.s-demo-router:9000/workers/add",
        json={"role": "prefill", "address": "10.0.0.8:8000"},
        headers={"Content-Type": "application/json"},
        timeout=(4.0, 12.0),
    )
    assert worker_info == {"type": "prefill", "port": 8000}  # not mutated


def test_address_falls_back_to_rbg_dns_without_pod_ip(monkeypatch):
    set_env(monkeypatch)
    monkeypatch.delenv("POD_IP")
    monkeypatch.setenv("RBG_ROLE_NAME", "prefill")
    monkeypatch.setenv("RBG_ROLE_INDEX", "2")
    reload(envs)

    client = ExampleTopoClient({"type": "prefill", "port": 8000})

    assert client.address == "demo-prefill-2.s-demo-prefill:8000"
    assert client.health_check_endpoint == "localhost:8000"


@patch(f"{MODULE}.requests.post")
def test_register_returns_false_on_router_error(mock_post, monkeypatch):
    set_env(monkeypatch)
    mock_post.return_value = MagicMock(status_code=500, text="failed")
    client = ExampleTopoClient({"type": "prefill"})
    with patch(f"{MODULE}.utils.retry", side_effect=lambda func, **_: func()):
        assert client.register("", {"type": "prefill"}) is False


def test_register_returns_false_without_worker_type(monkeypatch):
    set_env(monkeypatch)
    client = ExampleTopoClient({})
    assert client.register("", {}) is False


@patch(f"{MODULE}.requests.post")
def test_unregister_fails_fast_on_4xx(mock_post, monkeypatch):
    set_env(monkeypatch)
    mock_post.return_value = MagicMock(status_code=404, text="unknown worker")
    client = ExampleTopoClient({"type": "prefill"})
    client.role = "prefill"
    assert client.unregister() is False
    assert mock_post.call_count == 1
```

Also cover: the `wait_engine_ready` URL and timeout tuple; missing env in `__init__` raising `RuntimeError`;
`unregister()` stopping at the deadline (patch `time.monotonic`/`time.sleep`); the legacy env fallback; and
factory selection with a lowercase `topo_type`.

## Review focus

The topology section of `.agents/skills/patio-review/references/checklist.md`. In past reviews the most
common findings were missing timeouts (#3), env renames without a fallback (#3), and an unbounded unregister
on SIGTERM (#6).

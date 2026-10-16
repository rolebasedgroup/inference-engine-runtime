# -*- coding: utf-8 -*-
# @Author: dunjin

"""
MotorTopoClient — register/unregister inference engine instances with a
standalone MindIE Motor Coordinator via its /instances/refresh management
protocol, using the coordinator-standalone "External Deployer" event schema
(motor.common.resources.http_msg_spec.ExternalInsEventMsg).

The External Deployer schema is deliberately minimal: a sidecar states only the
instance id / role / routable endpoints, plus the optional shared model name and
P/D dispatch plan, and Motor derives the rest (job name, status, DP size,
multi-endpoint routing) in ExternalInsEventMsg.to_internal(). It also defines
what a sidecar can express: roles prefill/decode only, engine_type "vllm" only,
and a single P/D dispatch plan per registration. That matches Motor's standalone
Coordinator, which routes P/D traffic only and whose CLI
(``python3 -m motor.coordinator.register``) exposes the same two roles.

MindIE Motor Coordinator is a routing/control-plane component of the SAME
abstraction class as the SGLang router and the vLLM proxy, so this client lives
under topo/client/ and implements the shared GroupTopoClient interface, exactly
like SGLangGroupTopoClient and VLLMProxyTopoClient.

Standalone Motor Coordinator (``python3 -m motor.coordinator.register``) has NO
active health-check loop of its own: instances registered through
``/instances/refresh`` stay in the available pool until a ``del`` event is sent.
Patio is a sidecar that shares the pod with the engine, so this client is the
right place to (a) verify the engine is alive before (re-)registering and
(b) deregister the instance once the engine becomes unhealthy — traffic stops
being routed to a dead endpoint. ``register()`` is called periodically by the
app heartbeat thread, so the health check runs on every heartbeat and a
failed engine is deregistered within one heartbeat interval.
"""

import os
import time
import traceback
import zlib
from typing import Optional

import requests

from patio import envs
from patio.logger import init_logger
from patio.topo.client.base_topo_client import GroupTopoClient
from patio.topo import utils

logger = init_logger(__name__)

# Motor CLI-compatible instance-id derivation constants
STANDALONE_ID_NAMESPACE = 0x40000000
STANDALONE_ID_MASK = 0x3FFFFFFF

# Role a sidecar may register, i.e. ExternalInstance.role as accepted by
# ExternalInsEventMsg.to_internal(). Motor's standalone Coordinator routes P/D
# traffic only: its CLI exposes the same two roles, and encode/union instances
# belong to the Controller-registered topology (InsEventMsg), not to a sidecar.
MOTOR_EXTERNAL_ROLES = ("prefill", "decode")

# P/D coordination plan, i.e. ExternalInsEventMsg.dispatch_capabilities. The
# external schema carries ONE plan (an enum), unlike the Controller-shaped
# InsEventMsg whose Instance.dispatch_capabilities is a list; Motor rejects any
# value outside _require_external_dispatch_plan (decode_colocation included).
MOTOR_EXTERNAL_DISPATCH_PLANS = ("prefill_handoff_decode", "concurrent_engine_sync")
MOTOR_DISPATCH_CAPABILITY = "prefill_handoff_decode"

# Engine family accepted by the External Deployer schema. Motor rejects anything
# else with "External Deployer currently supports engine_type='vllm' only"; the
# Controller-shaped schema is the one that also knows the sglang adapter.
MOTOR_EXTERNAL_ENGINE_TYPE = "vllm"

# Motor management API key header (see management_interfaces.md / register.py)
MOTOR_MGMT_API_KEY_HEADER = "X-Motor-Management-Key"

# Port assumed for a bare host without ":port" in worker_info.
DEFAULT_ENGINE_PORT = "8000"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def get_motor_endpoint() -> str:
    """Derive the Motor Coordinator management endpoint from RBG env vars.

    Uses the same headless-service DNS shape as the SGLang router / vLLM proxy
    endpoints (ROUTER_ROLE_NAME / ROUTER_PORT), keeping Motor a first-class
    routing component rather than a side-channel integration.
    """
    rbg_group_name = envs.GROUP_NAME
    if rbg_group_name is None:
        raise RuntimeError("RBG_GROUP_NAME is not set")

    router_role_name = envs.ROUTER_ROLE_NAME
    if router_role_name is None:
        raise RuntimeError("ROUTER_ROLE_NAME is not set")

    router_port = envs.ROUTER_PORT
    if router_port is None:
        raise RuntimeError("ROUTER_PORT is not set")

    return (
        f"{rbg_group_name}-{router_role_name}-0."
        f"s-{rbg_group_name}-{router_role_name}:{router_port}"
    )


def get_mgmt_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    api_key_file = envs.MOTOR_MGMT_API_KEY_FILE
    if api_key_file:
        try:
            with open(api_key_file, "r") as f:
                api_key = f.read().strip()
            if api_key:
                headers[MOTOR_MGMT_API_KEY_HEADER] = api_key
        except OSError as e:
            logger.warning("failed to read motor mgmt api key file %s: %s", api_key_file, e)
    return headers


def detect_worker_type(worker_info: dict) -> str:
    """Extract and validate the worker role from worker_info.

    A sidecar can only register a P/D instance: ExternalInstance.role accepts
    prefill/decode, and Motor answers a bare 400 ("unsupported external instance
    role") for anything else. Validating here turns that late, server-side
    rejection into an immediate, actionable error before any HTTP call is made.
    """
    worker_type = worker_info.get("type") or worker_info.get("worker_type")
    if worker_type is None:
        raise ValueError("worker type is not set; provide 'type' or 'worker_type' in instance info")
    role = str(worker_type).strip().lower()
    if role not in MOTOR_EXTERNAL_ROLES:
        raise ValueError(
            "invalid worker type {!r}; a Motor sidecar can only register {} instances "
            "(the External Deployer protocol has no encode/union role)".format(
                worker_type, "/".join(MOTOR_EXTERNAL_ROLES)
            )
        )
    return role


def get_dispatch_capability() -> str:
    """P/D dispatch plan advertised for this instance.

    Defaults to the vLLM HANDOFF plan, which is what Motor assumes for a vLLM
    instance registered through the External Deployer protocol. Override with
    MOTOR_DISPATCH_CAPABILITIES when the engines use the trigger/sync profile
    (concurrent_engine_sync) instead.

    The external schema carries a single plan, unlike the Controller-shaped
    InsEventMsg whose dispatch_capabilities is a list, so exactly one value is
    accepted here and it must be one Motor knows.
    """
    raw = os.getenv("MOTOR_DISPATCH_CAPABILITIES")
    if raw is None:
        return MOTOR_DISPATCH_CAPABILITY
    capabilities = [item.strip() for item in raw.split(",") if item.strip()]
    if not capabilities:
        # empty override == "let Motor assume the vLLM default" (HANDOFF)
        return MOTOR_DISPATCH_CAPABILITY
    if len(capabilities) != 1 or capabilities[0] not in MOTOR_EXTERNAL_DISPATCH_PLANS:
        raise ValueError(
            "invalid MOTOR_DISPATCH_CAPABILITIES {!r}; the External Deployer protocol "
            "carries exactly one plan from {}".format(
                raw, ", ".join(MOTOR_EXTERNAL_DISPATCH_PLANS)
            )
        )
    return capabilities[0]


def get_engine_type() -> str:
    """Engine family advertised to Motor.

    The External Deployer protocol only serves vllm: Motor rejects any other
    value in ExternalInsEventMsg.to_internal() with "External Deployer currently
    supports engine_type='vllm' only". Validated client-side so a misconfigured
    MOTOR_ENGINE_TYPE (e.g. sglang, which the Controller-shaped schema accepts)
    fails at startup instead of at registration.
    """
    engine_type = (os.getenv("MOTOR_ENGINE_TYPE") or MOTOR_EXTERNAL_ENGINE_TYPE).strip().lower()
    if engine_type != MOTOR_EXTERNAL_ENGINE_TYPE:
        raise ValueError(
            "invalid MOTOR_ENGINE_TYPE {!r}; a Motor sidecar can only register "
            "engine_type={!r} instances".format(engine_type, MOTOR_EXTERNAL_ENGINE_TYPE)
        )
    return engine_type


def get_worker_address(worker_info: dict) -> str:
    """Return the worker's routable address (host:port).

    Resolution order:
    1. 'instance' field (from --instance-info JSON)
    2. POD_IP env var + worker port
    3. RBG headless DNS (group-role-index.s-group-role:port)
    """
    if worker_info.get("instance"):
        return worker_info["instance"]

    port = str(worker_info.get("port", "8000"))
    pod_ip = os.getenv("POD_IP")
    if pod_ip:
        return "{}:{}".format(pod_ip, port)

    group_name = os.getenv("RBG_GROUP_NAME")
    role_name = os.getenv("RBG_ROLE_NAME")
    role_index = os.getenv("RBG_ROLE_INDEX")
    if group_name and role_name and role_index:
        return "{}-{}-{}.s-{}-{}:{}".format(
            group_name, role_name, role_index, group_name, role_name, port
        )

    raise RuntimeError(
        "cannot determine worker address: set 'instance' field, POD_IP, or RBG env vars"
    )


def split_address(address: str) -> tuple[str, str]:
    """Split a ``host:port`` address into (host, port).

    Handles bare host without port (port defaults to 8000, matching the
    worker_info default) as well as IPv4 / DNS host:port forms and bracketed
    IPv6 (``[2001:db8::1]:8200``) — mirrors
    ``motor.common.utils.net.split_address``, which parses the address Motor
    receives in ``ExternalEndpoint.address``.

    A bare, unbracketed IPv6 literal is returned as host-only: ``rpartition``
    would otherwise read its last hextet as the port.
    """
    address = address.strip()
    if address.startswith("["):
        host, sep, remainder = address[1:].partition("]")
        if not sep:
            return address, DEFAULT_ENGINE_PORT
        port = remainder[1:] if remainder.startswith(":") else ""
        return host, port if port.isdigit() else DEFAULT_ENGINE_PORT
    host, sep, port = address.rpartition(":")
    if not sep or not host or not port.isdigit() or ":" in host:
        return address, DEFAULT_ENGINE_PORT
    return host, port


def format_address(host: str, port: str) -> str:
    """Render ``host:port`` for ``ExternalEndpoint.address`` (IPv6 bracketed)."""
    return "[{}]:{}".format(host, port) if ":" in host else "{}:{}".format(host, port)


def derive_instance_id(role: str, address: str) -> int:
    """Deterministic instance ID matching Motor's CLI scheme.

    Motor CLI (register.py, derive_instance_id):
        identity = "role|ip:port"
        digest = crc32(identity) & 0x3FFFFFFF
        return 0x40000000 | digest

    A sidecar registers the very same ID for the same role/endpoint pair, so a
    CLI-registered and a sidecar-registered instance are the same instance.
    """
    identity = "{}|{}".format(role, address.strip())
    digest = zlib.crc32(identity.encode()) & STANDALONE_ID_MASK
    return STANDALONE_ID_NAMESPACE | digest


def build_body(event: str, role: str, address: str, instance_id: int,
               model_name: Optional[str] = None) -> dict:
    """Build an ExternalInsEventMsg request body for /instances/refresh.

    Schema (motor.common.resources.http_msg_spec.ExternalInsEventMsg, the
    coordinator-standalone "External Deployer" protocol):
        {"event": "set"|"add"|"del",
         "model_name": str (optional),
         "dispatch_capabilities": "<one plan>",
         "engine_type": "vllm",
         "instances": [{"id": <int >=1>, "role": "prefill"|"decode",
                        "endpoints": [{"id": <int >=0>, "address": "host:port"}]}]}

    The schema is ``extra="forbid"``: job name, status, DP size and
    multi-endpoint routing are NOT part of it — Motor derives them in
    ExternalInsEventMsg.to_internal(). A request may not mix this shape with the
    Controller-shaped InsEventMsg fields (``job_name``, ``endpoints`` as a dict).
    """
    host, port = split_address(address)
    body = {
        "event": event,
        "dispatch_capabilities": get_dispatch_capability(),
        "engine_type": get_engine_type(),
        "instances": [
            {
                "id": instance_id,
                "role": role,
                "endpoints": [{"id": 0, "address": format_address(host, port)}],
            }
        ],
    }
    # Omitting model_name lets Motor probe every reachable DP's /v1/models and
    # require them to agree; a multi-model engine must state it. Patio sends the
    # name it resolved at registration time so that deregistration does not
    # depend on a still-live engine (Motor skips its probe when the name is set).
    if model_name:
        body["model_name"] = model_name
    return body


def _build_health_check_endpoint(worker_info: dict) -> str:
    """Determine the local health-check address.

    Uses POD_IP if available, otherwise falls back to localhost.
    """
    port = str(worker_info.get("port", "8000"))
    pod_ip = os.getenv("POD_IP")
    return "{}:{}".format(pod_ip, port) if pod_ip else "localhost:{}".format(port)


# ---------------------------------------------------------------------------
# MotorTopoClient
# ---------------------------------------------------------------------------


class MotorTopoClient(GroupTopoClient):
    """Register / unregister / health-check an inference engine instance with
    a standalone MindIE Motor Coordinator.

    Motor Coordinator is a routing/control-plane component of the same class
    as the SGLang router and the vLLM proxy, so this is a GroupTopoClient just
    like its topo/client/ siblings.
    """

    def __init__(self, worker_info: dict):
        self.worker_info = worker_info
        self.mgmt_endpoint = get_motor_endpoint()
        self.mgmt_headers = get_mgmt_headers()
        self.role = detect_worker_type(worker_info)
        self.address = get_worker_address(worker_info)
        self.instance_id = derive_instance_id(self.role, self.address)
        self.health_check_endpoint = _build_health_check_endpoint(worker_info)

        self._model_name = envs.MOTOR_MODEL_NAME
        # Registration state mirror used by the heartbeat-driven health check:
        # False until a register() succeeds, reset to False after an unregister().
        self._registered = False
        logger.info(
            "MotorTopoClient: role=%s address=%s instance_id=%d endpoint=%s",
            self.role, self.address, self.instance_id, self.mgmt_endpoint,
        )

    def _resolve_model_name(self) -> Optional[str]:
        """Resolve the engine model name for Coordinator registration.

        Priority: MOTOR_MODEL_NAME env > probe the local engine /v1/models
        (OpenAI-compatible, same endpoint Motor itself falls back to). The
        resolved name is cached in self._model_name so unregister() still
        carries it after the engine goes away — Motor's own fallback probe
        would fail against a dead engine (del would 400).
        """
        if self._model_name:
            return self._model_name
        try:
            resp = requests.get(
                "http://{}/v1/models".format(self.health_check_endpoint),
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT),
            )
            if resp.status_code != 200:
                logger.warning(
                    "engine /v1/models returned status=%s, keep model_name unset",
                    resp.status_code,
                )
                return None
            data = resp.json().get("data") or []
            ids = [m["id"] for m in data if isinstance(m, dict) and m.get("id")]
            if len(ids) == 1:
                self._model_name = ids[0]
                logger.info("resolved engine model name from /v1/models: %s", self._model_name)
                return self._model_name
            logger.warning(
                "engine /v1/models returned %d model ids, expected exactly 1; "
                "keep model_name unset", len(ids),
            )
        except Exception as e:
            logger.warning("failed to resolve model name from engine /v1/models: %s", e)
        return None

    # -----------------------------------------------------------------------
    # GroupTopoClient interface
    # -----------------------------------------------------------------------

    def _engine_healthy(self, worker_info: dict) -> bool:
        """Probe the local engine /health endpoint once.

        Returns True only on HTTP 200. Used by wait_engine_ready (polling
        loop) and by register() (heartbeat-driven failure detection).
        """
        try:
            resp = requests.get(
                "http://" + self.health_check_endpoint + "/health",
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT),
            )
            return resp.status_code == 200
        except requests.RequestException as e:
            logger.debug("engine health probe failed: %s", e)
            return False

    def wait_engine_ready(self, worker_info: dict) -> bool:
        """Poll the engine /health endpoint until it returns 200."""
        def _check():
            if self._engine_healthy(worker_info):
                logger.info("Health check OK, inference engine is now ready.")
                return True
            raise RuntimeError(
                "health check failed, url: http://{}/health".format(self.health_check_endpoint)
            )

        try:
            utils.retry(_check, retry_times=60, interval=3)
            return True
        except Exception as e:
            logger.error("failed to wait for engine ready: %s", e)
            traceback.print_exc()
            return False

    def register(self, url: str, worker_info: dict,
                 file_path: Optional[str] = None) -> bool:
        """Register the engine instance with the Motor Coordinator.

        Posts to /instances/refresh with event=add. Idempotent when the
        instance-id already exists on the Coordinator.

        Health-gated: the app heartbeat thread calls register() periodically,
        so this method doubles as the failure detector — if the engine is no
        longer healthy we deregister the instance (event=del) instead of
        re-adding it, so the Coordinator stops routing traffic to a dead
        endpoint. A recovered engine re-registers on the next healthy call.
        """
        if not self._engine_healthy(worker_info):
            logger.warning(
                "engine unhealthy at %s, deregistering instance %d (%s) from Motor Coordinator",
                self.health_check_endpoint, self.instance_id, self.role,
            )
            if self._registered:
                self.unregister()
            return False

        # Engine is guaranteed alive here (health gate above), so
        # resolve/cache the model name now — unregister() will reuse it after
        # the engine is gone.
        self._resolve_model_name()
        body = build_body(
            event="add",
            role=self.role,
            address=self.address,
            instance_id=self.instance_id,
            model_name=self._model_name,
        )

        def _do_register():
            mgmt_url = "http://{}/instances/refresh".format(self.mgmt_endpoint)
            logger.info(
                "registering instance to Motor Coordinator: %s  body=%s",
                mgmt_url, body,
            )
            resp = requests.post(
                mgmt_url,
                json=body,
                headers=self.mgmt_headers,
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
            )
            if not 200 <= resp.status_code < 300:
                raise RuntimeError(
                    "register failed, url: {}, status_code: {}, content: {}".format(
                        mgmt_url, resp.status_code, resp.text
                    )
                )
            logger.info("registered instance %d (%s) successfully", self.instance_id, self.role)

        try:
            utils.retry(_do_register, retry_times=60, interval=3)
            self._registered = True
            return True
        except Exception as e:
            logger.error("failed to register instance: %s", e)
            traceback.print_exc()
            return False

    def unregister(self):
        """Deregister the engine instance from the Motor Coordinator.

        Posts to /instances/refresh with event=del.
        Carries the cached model name (resolved at register time): Motor
        requires model_name for del too, and its fallback /v1/models probe
        would fail against the already-dead engine.

        Bounded by MOTOR_UNREGISTER_DEADLINE instead of the 60x3s registration
        budget: this runs inside the SIGTERM/SIGINT handler (app.py), and the
        equivalent vllm-router operation (POST /remove_worker) is immediate, so
        deregistration must never hold up pod termination. Only transport errors
        and 5xx are retried; a 4xx answer cannot start succeeding, so it stops
        the loop at once. A 404 means the Coordinator has no such instance
        (e.g. it restarted) — the desired end state, reported as success.
        """
        if not self._registered:
            logger.debug("instance %d is not registered, skipping unregister", self.instance_id)
            return True

        body = build_body(
            event="del",
            role=self.role,
            address=self.address,
            instance_id=self.instance_id,
            model_name=self._model_name,
        )
        mgmt_url = "http://{}/instances/refresh".format(self.mgmt_endpoint)
        deadline = time.monotonic() + envs.MOTOR_UNREGISTER_DEADLINE

        while True:
            try:
                logger.info(
                    "unregistering instance from Motor Coordinator: %s  body=%s",
                    mgmt_url, body,
                )
                resp = requests.post(
                    mgmt_url,
                    json=body,
                    headers=self.mgmt_headers,
                    timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.MOTOR_UNREGISTER_TIMEOUT),
                )
                if 200 <= resp.status_code < 300:
                    logger.info(
                        "unregistered instance %d (%s) successfully", self.instance_id, self.role
                    )
                    self._registered = False
                    return True
                if resp.status_code == 404:
                    logger.info(
                        "instance %d is not registered on the Coordinator (404), nothing to deregister",
                        self.instance_id,
                    )
                    self._registered = False
                    return True
                if 400 <= resp.status_code < 500:
                    # Malformed body / identity mismatch / rejected API key:
                    # retrying cannot fix it, so fail fast instead of sleeping.
                    logger.error(
                        "unregister rejected permanently, url: %s, status_code: %s, content: %s",
                        mgmt_url, resp.status_code, resp.text,
                    )
                    return False
                raise RuntimeError(
                    "unregister failed, url: {}, status_code: {}, content: {}".format(
                        mgmt_url, resp.status_code, resp.text
                    )
                )
            except Exception as e:
                last_error = e

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.error(
                    "failed to unregister instance %d within %.1fs: %s",
                    self.instance_id, envs.MOTOR_UNREGISTER_DEADLINE, last_error,
                )
                traceback.print_exception(last_error)
                return False
            interval = min(envs.MOTOR_UNREGISTER_INTERVAL, remaining)
            logger.warning(
                "unregister attempt failed (%s), retrying in %.1fs", last_error, interval
            )
            time.sleep(interval)

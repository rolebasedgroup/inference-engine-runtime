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

One sidecar == one P/D instance == one *endpoint group*. A production instance is
never a single endpoint: a DeepSeek-V4-Flash instance on one 16-NPU Atlas 800I A3
node is dp4 x tp4 = 4 prefill endpoints (one vLLM API server per DP rank) or
dp16 x tp1 = 16 decode endpoints. Motor derives ``dp_size = len(endpoints)`` and
builds the multi-endpoint routing table from that list
(``ExternalInsEventMsg.to_internal()``), so a sidecar advertising only its first
endpoint registers a dp_size=1 instance: the Coordinator routes every request of
the instance to one DP rank (1/4 of a prefill node, 1/16 of a decode node) and
reports the wrong DP size. This client therefore always reports the complete
group (``get_worker_endpoints``): an explicit ``endpoints`` list, an explicit
``ports`` list, or ``parallel_config`` + ``device_num`` + ``base_port`` from
which ports are derived exactly like Motor's own ``_generate_endpoint_ports``.
The original single ``instance``/``port`` contract is still accepted and means a
one-endpoint instance.

MindIE Motor Coordinator is a routing/control-plane component of the SAME
abstraction class as the SGLang router and the vLLM proxy, so this client lives
under topo/client/ and implements the shared GroupTopoClient interface, exactly
like SGLangGroupTopoClient and VLLMProxyTopoClient.

Standalone Motor Coordinator (``python3 -m motor.coordinator.register``) has NO
active health-check loop of its own: instances registered through
``/instances/refresh`` stay in the available pool until a ``del`` event is sent.
Patio is a sidecar that shares the pod with the engine, so this client is the
right place to (a) verify the engine is alive on EVERY DP endpoint before
(re-)registering and (b) deregister the instance once any endpoint becomes
unhealthy — traffic stops being routed to a dead endpoint. ``register()`` is
called periodically by the app heartbeat thread, so the health check runs on
every heartbeat and a failed engine is deregistered within one heartbeat
interval. The gate covers the whole endpoint group because Motor re-probes the
group on ``add`` and rejects the instance when a single DP is unreachable.
"""

import os
import time
import traceback
import zlib
from concurrent.futures import ThreadPoolExecutor
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

# Motor's own port convention for the DP endpoints of one instance
# (motor/config/node_manager.py: endpoint_config.base_port default 10000, and
# service_ports = [base_port + i * port_step], port_step = 2). Used only when a
# deployment hands Patio a DP size instead of an explicit endpoint list.
DEFAULT_BASE_PORT = 10000
DEFAULT_PORT_STEP = 2


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


# ---------------------------------------------------------------------------
# the endpoint group of one instance
# ---------------------------------------------------------------------------


def normalize_endpoint(item) -> str:
    """Normalize one endpoint entry to a routable ``host:port`` address.

    Accepts ``"host:port"`` strings and mappings carrying ``address`` (Motor's
    own ExternalEndpoint field) or an ``ip``/``host`` + ``port``/``business_port``
    pair, so a deployment can pass through whatever its inventory already holds.
    """
    if isinstance(item, dict):
        address = item.get("address") or item.get("instance")
        if address:
            return str(address).strip()
        host = item.get("ip") or item.get("host")
        port = item.get("port") or item.get("business_port") or DEFAULT_ENGINE_PORT
        if not host:
            raise ValueError(
                "endpoint entry {!r} carries neither 'address' nor 'ip'/'host'".format(item)
            )
        return format_address(str(host), str(port))
    address = str(item).strip()
    if not address:
        raise ValueError("empty endpoint address in the instance endpoint group")
    return address


def sort_endpoints(addresses) -> list[str]:
    """Return a group in Motor's canonical order, rejecting duplicates.

    Motor's CLI (register.py, ``parse_endpoint_group``) sorts a group by
    ``(ip, int(port))`` so the same physical group always derives the same
    instance id regardless of input order, and Motor itself rejects an instance
    carrying two identical ``(host, port)`` endpoints. Doing both here keeps the
    instance id stable across restarts and turns a duplicated address into an
    immediate, actionable error instead of a 400 after the request.
    """
    ordered: list[tuple[tuple[str, int], str]] = []
    seen: set[tuple[str, int]] = set()
    for address in addresses:
        host, port = split_address(normalize_endpoint(address))
        key = (host, int(port))
        if key in seen:
            raise ValueError(
                "duplicate endpoint address {}:{} in the instance endpoint group".format(host, port)
            )
        seen.add(key)
        ordered.append((key, format_address(host, port)))
    ordered.sort(key=lambda item: item[0])
    return [address for _, address in ordered]


def derive_endpoint_ports(dp_size: int, local_world_size: int, device_num: int,
                          base_port: int = DEFAULT_BASE_PORT,
                          port_step: int = DEFAULT_PORT_STEP) -> list[str]:
    """Derive an instance's DP endpoint ports exactly like Motor does.

    Mirror of ``motor/config/node_manager.py::_generate_endpoint_ports``::

        endpoint_num = min(dp_size, device_num // local_world_size)
        service_ports = [base_port + i * port_step for i in range(endpoint_num)]

    where ``local_world_size`` is the number of devices one DP rank occupies
    (``tp_size * pp_size * pcp_size``) and ``device_num`` the NPUs of the node.
    """
    if dp_size < 1:
        raise ValueError("dp_size must be >= 1, got {}".format(dp_size))
    if local_world_size < 1:
        raise ValueError("local_world_size must be >= 1, got {}".format(local_world_size))
    if device_num < local_world_size:
        raise ValueError(
            "device_num {} is smaller than one DP rank's local_world_size {}".format(
                device_num, local_world_size
            )
        )
    endpoint_num = min(dp_size, device_num // local_world_size)
    return [str(base_port + index * port_step) for index in range(endpoint_num)]


def get_worker_endpoints(worker_info: dict) -> list[str]:
    """Return EVERY routable DP endpoint of the instance this sidecar belongs to.

    Resolution order (first match wins):

    1. ``worker_info["endpoints"]`` — the authoritative, complete group; entries
       are ``"host:port"`` strings or mappings (see :func:`normalize_endpoint`).
       It may span pods (Motor keys endpoints by ip), so a group assembled by the
       platform can be handed to a single sidecar.
    2. ``MOTOR_INSTANCE_ENDPOINTS`` — comma separated ``host:port`` list, for
       deployments that can only inject environment variables.
    3. ``worker_info["ports"]`` — the DP ports of this pod's host.
    4. ``worker_info["endpoint_num"]`` (or one derived from ``parallel_config`` /
       ``device_num`` through :func:`derive_endpoint_ports`) plus
       ``base_port``/``port_step`` — ports generated with Motor's convention.
    5. the single ``instance``/``port``/RBG-DNS address: a one-endpoint instance.
    """
    explicit = worker_info.get("endpoints")
    if explicit is None:
        env_endpoints = os.getenv("MOTOR_INSTANCE_ENDPOINTS")
        if env_endpoints and env_endpoints.strip():
            explicit = [entry for entry in env_endpoints.split(",") if entry.strip()]
    if explicit is not None:
        if not isinstance(explicit, (list, tuple)) or not explicit:
            raise ValueError(
                "worker_info['endpoints'] must be a non-empty list of host:port entries"
            )
        return sort_endpoints(list(explicit))

    host, _ = split_address(get_worker_address(worker_info))
    base_port = worker_info.get("base_port") or os.getenv("MOTOR_BASE_PORT") or DEFAULT_BASE_PORT
    port_step = worker_info.get("port_step") or os.getenv("MOTOR_PORT_STEP") or DEFAULT_PORT_STEP

    ports = worker_info.get("ports")
    if ports is None:
        endpoint_num = worker_info.get("endpoint_num") or os.getenv("MOTOR_ENDPOINT_NUM")
        if endpoint_num is None:
            parallel_config = worker_info.get("parallel_config") or {}
            dp_size = parallel_config.get("dp_size", worker_info.get("data_parallel_size"))
            device_num = worker_info.get("device_num", worker_info.get("npu_num"))
            if dp_size is not None and device_num is not None:
                local_world_size = parallel_config.get("local_world_size")
                if local_world_size is None:
                    local_world_size = (
                        int(parallel_config.get("tp_size", 1))
                        * int(parallel_config.get("pp_size", 1))
                        * int(parallel_config.get("pcp_size", 1))
                    )
                ports = derive_endpoint_ports(
                    int(dp_size), int(local_world_size), int(device_num),
                    int(base_port), int(port_step),
                )
        elif int(endpoint_num) > 0:
            ports = [
                str(int(base_port) + index * int(port_step)) for index in range(int(endpoint_num))
            ]

    if ports:
        return sort_endpoints([format_address(host, str(port)) for port in ports])
    return [get_worker_address(worker_info)]


def derive_instance_id(role: str, endpoints) -> int:
    """Deterministic instance ID matching Motor's CLI scheme.

    Motor CLI (``register.py``, ``derive_instance_id``)::

        identity = "<role>|<ip:port>|<ip:port>|..."   # sorted by (ip, int(port))
        digest = crc32(identity) & 0x3FFFFFFF
        return 0x40000000 | digest

    The identity covers the COMPLETE endpoint group, not just its first endpoint,
    so a multi-endpoint instance derives the same id Motor's CLI derives for that
    group. A one-endpoint instance keeps the historical ``role|host:port`` id.
    """
    addresses = sort_endpoints([endpoints] if isinstance(endpoints, str) else list(endpoints))
    identity = "|".join([role, *addresses])
    digest = zlib.crc32(identity.encode()) & STANDALONE_ID_MASK
    return STANDALONE_ID_NAMESPACE | digest


def build_body(event: str, role: str, endpoints, instance_id: int,
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

    ``endpoints`` is the instance's COMPLETE group (a single address, or an
    iterable of addresses); ``endpoint.id`` is the DP rank, so a group of N
    endpoints is emitted as ids 0..N-1 in canonical order. Motor keys them by ip
    (an instance may therefore span pods) and derives
    ``dp_size = len(endpoints)`` in ExternalInsEventMsg.to_internal().

    The schema is ``extra="forbid"``: job name, status, DP size and
    multi-endpoint routing are NOT part of it. A request may not mix this shape
    with the Controller-shaped InsEventMsg fields (``job_name``, ``endpoints``
    as a dict).
    """
    addresses = sort_endpoints([endpoints] if isinstance(endpoints, str) else list(endpoints))
    body = {
        "event": event,
        "dispatch_capabilities": get_dispatch_capability(),
        "engine_type": get_engine_type(),
        "instances": [
            {
                "id": instance_id,
                "role": role,
                "endpoints": [
                    {"id": index, "address": address}
                    for index, address in enumerate(addresses)
                ],
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
        # Complete DP endpoint group of this instance (never just one endpoint:
        # Motor derives dp_size from the group size — see get_worker_endpoints).
        self.endpoints = get_worker_endpoints(worker_info)
        # ``address`` is kept for compatibility; the registration payload and the
        # health gate both operate on the COMPLETE group.
        self.address = self.endpoints[0]
        # The gate probes every registered endpoint: Motor re-probes the whole
        # group on add and rejects the instance when one DP is unreachable, so
        # probing only the local rank would let a partially dead group through.
        self.health_check_endpoints = list(self.endpoints)
        self.health_check_endpoint = self.health_check_endpoints[0]
        self.instance_id = derive_instance_id(self.role, self.endpoints)

        self._model_name = envs.MOTOR_MODEL_NAME
        # Registration state mirror used by the heartbeat-driven health check:
        # False until a register() succeeds, reset to False after an unregister().
        self._registered = False
        logger.info(
            "MotorTopoClient: role=%s instance_id=%d dp_endpoints=%d [%s] coordinator=%s",
            self.role, self.instance_id, len(self.endpoints),
            ", ".join(self.endpoints), self.mgmt_endpoint,
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

    @staticmethod
    def _probe_endpoint_health(address: str) -> tuple[str, bool, str]:
        """GET ``http://<address>/health`` once; return (address, healthy, reason)."""
        try:
            resp = requests.get(
                "http://" + address + "/health",
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT),
            )
            if resp.status_code == 200:
                return address, True, ""
            return address, False, "status {}".format(resp.status_code)
        except requests.RequestException as e:
            return address, False, str(e)

    def _engine_healthy(self, worker_info: dict) -> bool:
        """Probe EVERY DP endpoint's /health endpoint; True only if all pass.

        One sidecar owns one P/D instance, i.e. one DP endpoint group. Motor
        re-probes the whole group on add (model discovery/verification) and
        rejects the entire instance when a single DP is unreachable, so the gate
        must cover the group — a local-only probe would register a group Motor
        is about to reject. Probes run concurrently to keep the heartbeat
        bounded when a node hosts many DPs (up to 16).
        """
        targets = self.health_check_endpoints
        if len(targets) == 1:
            results = [self._probe_endpoint_health(targets[0])]
        else:
            max_workers = min(8, len(targets))
            with ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="motor-health"
            ) as executor:
                results = list(executor.map(self._probe_endpoint_health, targets))
        failures = [
            (address, reason) for address, healthy, reason in results if not healthy
        ]
        if failures:
            logger.warning(
                "engine health gate failed for %d/%d DP endpoint(s): %s",
                len(failures), len(targets),
                "; ".join("{} ({})".format(address, reason) for address, reason in failures),
            )
            return False
        return True

    def wait_engine_ready(self, worker_info: dict) -> bool:
        """Poll the engine /health endpoint until it returns 200."""
        def _check():
            if self._engine_healthy(worker_info):
                logger.info("Health check OK, inference engine is now ready.")
                return True
            raise RuntimeError(
                "health check failed, urls: {}".format(
                    ", ".join("http://{}/health".format(endpoint)
                              for endpoint in self.health_check_endpoints)
                )
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

        The body carries this instance's COMPLETE DP endpoint group: Motor
        derives dp_size from it and builds the multi-endpoint routing table, so
        advertising a single endpoint would pin the whole instance to one DP
        rank.

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
            endpoints=self.endpoints,
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
            if 400 <= resp.status_code < 500:
                # Deterministic rejection (e.g. 409 when the same instance id is
                # already registered with a different shape, or 400 for a
                # payload Motor refuses). Retrying cannot help, and this runs in
                # the 10s heartbeat: returning instead of raising keeps a
                # permanent 4xx from blocking the heartbeat thread for the whole
                # 60 x 3s registration budget on every single beat.
                logger.error(
                    "Motor Coordinator rejected registration of instance %d (%s), "
                    "status_code=%s, content=%s; not retrying",
                    self.instance_id, self.role, resp.status_code, resp.text,
                )
                return False
            if not 200 <= resp.status_code < 300:
                raise RuntimeError(
                    "register failed, url: {}, status_code: {}, content: {}".format(
                        mgmt_url, resp.status_code, resp.text
                    )
                )
            logger.info(
                "registered instance %d (%s) with %d DP endpoint(s) successfully",
                self.instance_id, self.role, len(self.endpoints),
            )
            return True

        try:
            self._registered = bool(utils.retry(_do_register, retry_times=60, interval=3))
            return self._registered
        except Exception as e:
            logger.error("failed to register instance: %s", e)
            traceback.print_exc()
            return False

    def unregister(self):
        """Deregister the engine instance from the Motor Coordinator.

        Posts to /instances/refresh with event=del.
        Carries the same COMPLETE endpoint group that register() advertised:
        Motor identifies a deletion by role plus the physical endpoint set
        (motor/coordinator/domain/instance_manager.py, _same_delete_identity) and
        rejects a partial group with a 409 InstanceIdConflictError.
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
            endpoints=self.endpoints,
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

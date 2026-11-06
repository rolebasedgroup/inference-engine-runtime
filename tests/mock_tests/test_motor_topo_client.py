# -*- coding: utf-8 -*-
"""
Tests for the MindIE Motor topo client (MotorTopoClient).
"""
from importlib import reload
from unittest.mock import MagicMock, patch

import time

from patio import envs
from patio.topo.client.motor_topo_client import (
    MotorTopoClient,
    build_body,
    derive_endpoint_ports,
    derive_instance_id,
    detect_worker_type,
    format_address,
    get_dispatch_capability,
    get_engine_type,
    get_motor_endpoint,
    get_worker_address,
    get_worker_endpoints,
    normalize_endpoint,
    sort_endpoints,
    split_address,
    STANDALONE_ID_NAMESPACE,
    STANDALONE_ID_MASK,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _patch_retry():
    """Bypass the 60-attempt retry loop in MotorTopoClient methods."""
    return patch(
        "patio.topo.client.motor_topo_client.utils.retry",
        side_effect=lambda func, **_: func(),
    )


def _set_motor_env(monkeypatch, group="demo", router_role="router",
                   router_port="9000", api_key_file=None):
    monkeypatch.setenv("RBG_GROUP_NAME", group)
    monkeypatch.setenv("ROUTER_ROLE_NAME", router_role)
    monkeypatch.setenv("ROUTER_PORT", router_port)
    if api_key_file:
        monkeypatch.setenv("MOTOR_MGMT_API_KEY_FILE", api_key_file)


def _make_client(monkeypatch, worker_info=None, pod_ip="10.0.0.1"):
    monkeypatch.setenv("POD_IP", pod_ip)
    reload(envs)
    return MotorTopoClient(worker_info or {"type": "prefill", "port": 8000})


# ---------------------------------------------------------------------------
# derive_instance_id
# ---------------------------------------------------------------------------

def test_derive_instance_id_prefill():
    """Derives a deterministic instance id within the namespace range."""
    instance_id = derive_instance_id("prefill", "10.0.0.1:8000")
    assert instance_id >= STANDALONE_ID_NAMESPACE
    assert instance_id <= STANDALONE_ID_NAMESPACE | STANDALONE_ID_MASK


def test_derive_instance_id_decode():
    instance_id = derive_instance_id("decode", "10.0.0.2:8000")
    assert instance_id >= STANDALONE_ID_NAMESPACE


def test_derive_instance_id_different_roles_yield_different_ids():
    prefill_id = derive_instance_id("prefill", "10.0.0.1:8000")
    decode_id = derive_instance_id("decode", "10.0.0.1:8000")
    assert prefill_id != decode_id


def test_derive_instance_id_matches_motor_cli():
    """Same derivation as motor/coordinator/register.py (role|ip:port)."""
    instance_id = derive_instance_id("prefill", "10.10.0.11:8000")
    assert instance_id == STANDALONE_ID_NAMESPACE | (
        __import__("zlib").crc32(b"prefill|10.10.0.11:8000") & STANDALONE_ID_MASK
    )


# ---------------------------------------------------------------------------
# get_motor_endpoint (RBG DNS derivation, same shape as sgl/vllm clients)
# ---------------------------------------------------------------------------

def test_get_motor_endpoint(monkeypatch):
    _set_motor_env(monkeypatch, group="demo", router_role="router", router_port="9000")
    reload(envs)

    endpoint = get_motor_endpoint()
    assert endpoint == "demo-router-0.s-demo-router:9000"


def test_get_motor_endpoint_raises_when_group_missing(monkeypatch):
    monkeypatch.delenv("RBG_GROUP_NAME", raising=False)
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    reload(envs)

    import pytest
    with pytest.raises(RuntimeError, match="RBG_GROUP_NAME is not set"):
        get_motor_endpoint()


def test_get_motor_endpoint_raises_when_router_role_missing(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.delenv("ROUTER_ROLE_NAME", raising=False)
    monkeypatch.setenv("ROUTER_PORT", "9000")
    reload(envs)

    import pytest
    with pytest.raises(RuntimeError, match="ROUTER_ROLE_NAME is not set"):
        get_motor_endpoint()


# ---------------------------------------------------------------------------
# address helpers
# ---------------------------------------------------------------------------

def test_split_address_host_port():
    assert split_address("10.0.0.1:8000") == ("10.0.0.1", "8000")


def test_split_address_dns_name():
    assert split_address("demo-prefill-0.s-demo-prefill:9000") == (
        "demo-prefill-0.s-demo-prefill", "9000")


def test_split_address_bare_host_defaults_port():
    assert split_address("10.0.0.1") == ("10.0.0.1", "8000")


def test_split_address_bracketed_ipv6():
    assert split_address("[2001:db8::1]:8200") == ("2001:db8::1", "8200")


def test_split_address_bare_ipv6_is_host_only():
    """rpartition would read the last hextet as the port; keep it whole."""
    assert split_address("2001:db8::1") == ("2001:db8::1", "8000")


def test_get_worker_address_prefers_explicit_instance(monkeypatch):
    monkeypatch.setenv("POD_IP", "10.0.0.8")
    assert get_worker_address({"instance": "custom-svc:8500", "port": 8500}) == "custom-svc:8500"


def test_get_worker_address_uses_pod_ip(monkeypatch):
    monkeypatch.setenv("POD_IP", "10.0.0.8")
    assert get_worker_address({"port": 8102}) == "10.0.0.8:8102"


def test_get_worker_address_uses_rbg_dns(monkeypatch):
    monkeypatch.delenv("POD_IP", raising=False)
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("RBG_ROLE_NAME", "decode")
    monkeypatch.setenv("RBG_ROLE_INDEX", "2")
    assert get_worker_address({"port": 8102}) == "demo-decode-2.s-demo-decode:8102"


def test_get_worker_address_raises_without_any_source(monkeypatch):
    monkeypatch.delenv("POD_IP", raising=False)
    monkeypatch.delenv("RBG_GROUP_NAME", raising=False)
    monkeypatch.delenv("RBG_ROLE_NAME", raising=False)
    monkeypatch.delenv("RBG_ROLE_INDEX", raising=False)

    import pytest
    with pytest.raises(RuntimeError, match="cannot determine worker address"):
        get_worker_address({"port": 8102})


# ---------------------------------------------------------------------------
# build_body (ExternalInsEventMsg: coordinator-standalone External Deployer)
# ---------------------------------------------------------------------------

def test_build_body_add():
    body = build_body(event="add", role="prefill", endpoints="10.0.0.1:8000",
                      instance_id=1073741825, model_name="Qwen-7B")
    # ExternalInsEventMsg: event + optional knobs + instances[]
    assert set(body.keys()) == {"event", "model_name", "dispatch_capabilities",
                                "engine_type", "instances"}
    assert body["event"] == "add"
    assert body["model_name"] == "Qwen-7B"
    assert body["engine_type"] == "vllm"
    # a single plan, not a list: the external schema carries one enum value
    assert body["dispatch_capabilities"] == "prefill_handoff_decode"
    assert len(body["instances"]) == 1
    inst = body["instances"][0]
    assert set(inst.keys()) == {"id", "role", "endpoints"}
    assert inst["id"] == 1073741825
    assert inst["role"] == "prefill"
    # endpoints are a flat [{id, address}] list; Motor derives ip/port, job
    # name, status and DP size in ExternalInsEventMsg.to_internal()
    assert inst["endpoints"] == [{"id": 0, "address": "10.0.0.1:8000"}]


def test_build_body_omits_model_name_when_unresolved():
    """model_name is optional in the external schema: Motor then probes every
    reachable DP /v1/models and requires them to agree. An unresolved name must
    be left out rather than sent as a placeholder that cannot match a model."""
    body = build_body(event="del", role="decode", endpoints="10.0.0.2:8000",
                      instance_id=1073741826)
    assert body["event"] == "del"
    assert "model_name" not in body


def test_build_body_never_mixes_controller_shape_fields():
    """Motor classifies a payload by shape: Controller-shaped fields on an
    instance (job_name, dict endpoints, status, ...) would route the event to
    the InsEventMsg schema instead of the external one."""
    inst = build_body("add", "prefill", "10.0.0.1:8000", 1, "m")["instances"][0]
    for controller_only in ("job_name", "model_name", "engine_type",
                            "dispatch_capabilities", "status",
                            "enable_multi_endpoints"):
        assert controller_only not in inst


def test_build_body_brackets_ipv6_address():
    body = build_body(event="add", role="prefill", endpoints="[2001:db8::1]:8200",
                      instance_id=7)
    assert body["instances"][0]["endpoints"] == [{"id": 0, "address": "[2001:db8::1]:8200"}]


# ---------------------------------------------------------------------------
# multi-endpoint instances (the production P/D shape)
#
# Motor's External Deployer schema takes a LIST of endpoints per instance and
# derives dp_size = len(endpoints) plus the multi-endpoint routing table in
# ExternalInsEventMsg.to_internal(). A DeepSeek-V4-Flash instance on one 16-NPU
# Atlas 800I A3 node is dp4 x tp4 = 4 prefill endpoints (ports 7100, 7102, 7104,
# 7106) or dp16 x tp1 = 16 decode endpoints (7100 .. 7130 step 2), because the
# official launch script starts one vLLM API server per DP rank
# (vllm-ascend DeepSeek-V4-Flash tutorial, launch_online_dp.py).
# ---------------------------------------------------------------------------

# Instance ids the same groups derive with Motor's own CLI
# (python3 -c "from motor.coordinator.register import derive_instance_id; ..."),
# i.e. crc32("role|ip:port|ip:port|...") | 0x40000000:
#   prefill 10.0.0.1:{7100,7102,7104,7106} -> 1147345019
#   decode  10.0.0.2:{7100..7130 step 2}    -> 1872492629
MOTOR_CLI_PREFILL_4EP_ID = 1147345019
MOTOR_CLI_DECODE_16EP_ID = 1872492629

A3_PREFILL_PORTS = [7100, 7102, 7104, 7106]
A3_DECODE_PORTS = [7100 + 2 * index for index in range(16)]


def test_build_body_reports_every_dp_endpoint_of_a_prefill_instance():
    """One endpoint per DP rank, ids 0..N-1: Motor turns this into dp_size=4."""
    addresses = ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS]
    body = build_body(event="add", role="prefill", endpoints=addresses,
                      instance_id=MOTOR_CLI_PREFILL_4EP_ID, model_name="dsv4")
    endpoints = body["instances"][0]["endpoints"]
    assert endpoints == [
        {"id": index, "address": address} for index, address in enumerate(addresses)
    ]
    assert len(endpoints) == 4


def test_build_body_reports_every_dp_endpoint_of_a_decode_instance():
    addresses = ["10.0.0.2:%d" % port for port in A3_DECODE_PORTS]
    body = build_body(event="add", role="decode", endpoints=addresses,
                      instance_id=MOTOR_CLI_DECODE_16EP_ID, model_name="dsv4")
    endpoints = body["instances"][0]["endpoints"]
    assert [entry["address"] for entry in endpoints] == addresses
    assert [entry["id"] for entry in endpoints] == list(range(16))


def test_build_body_is_order_independent():
    """Motor's CLI sorts a group by (ip, int(port)) before deriving the id, so
    the same physical group must produce the same body whatever order the
    deployment reports it in."""
    addresses = ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS]
    shuffled = [addresses[2], addresses[0], addresses[3], addresses[1]]
    assert build_body("add", "prefill", shuffled, 1) == build_body(
        "add", "prefill", addresses, 1
    )
    assert derive_instance_id("prefill", shuffled) == derive_instance_id(
        "prefill", addresses
    )


def test_build_body_accepts_dict_endpoint_entries():
    """A deployment may pass its own inventory records instead of bare strings."""
    body = build_body(
        event="add",
        role="decode",
        endpoints=[
            {"ip": "10.0.0.2", "business_port": "7100"},
            {"address": "10.0.0.2:7102"},
        ],
        instance_id=1,
    )
    assert body["instances"][0]["endpoints"] == [
        {"id": 0, "address": "10.0.0.2:7100"},
        {"id": 1, "address": "10.0.0.2:7102"},
    ]


def test_build_body_rejects_duplicate_endpoints():
    """Motor rejects an instance with two identical (host, port) endpoints with
    a 400; fail locally with an actionable message instead."""
    import pytest
    with pytest.raises(ValueError, match="duplicate endpoint address 10.0.0.1:8000"):
        build_body("add", "prefill", ["10.0.0.1:8000", "10.0.0.1:8000"], 1)


def test_derive_instance_id_multi_endpoint_matches_motor_cli():
    """The id covers the whole group, not just the first endpoint, so a
    multi-endpoint instance is the instance Motor's CLI would register."""
    prefill = derive_instance_id(
        "prefill", ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS]
    )
    decode = derive_instance_id(
        "decode", ["10.0.0.2:%d" % port for port in A3_DECODE_PORTS]
    )
    assert prefill == MOTOR_CLI_PREFILL_4EP_ID
    assert decode == MOTOR_CLI_DECODE_16EP_ID
    # ...and it differs from the single-endpoint id of the same first endpoint,
    # which is exactly the bug this fixes: a one-endpoint registration is not
    # the same instance as the dp_size=4 group.
    assert prefill != derive_instance_id("prefill", "10.0.0.1:7100")
    assert decode != derive_instance_id("decode", "10.0.0.2:7100")


def test_derive_endpoint_ports_matches_motor_node_manager():
    """Mirror of motor/config/node_manager.py::_generate_endpoint_ports:
    endpoint_num = min(dp_size, device_num // local_world_size), ports step 2."""
    # DeepSeek-V4-Flash A3 典配 (Motor user_config.json: 16 NPUs per pod)
    assert derive_endpoint_ports(4, 4, 16) == ["10000", "10002", "10004", "10006"]
    assert derive_endpoint_ports(16, 1, 16) == [
        str(10000 + 2 * index) for index in range(16)
    ]
    # dp larger than the node's DP capacity: capped by device_num // local
    assert derive_endpoint_ports(8, 4, 16) == ["10000", "10002", "10004", "10006"]
    # base_port / port_step override
    assert derive_endpoint_ports(2, 8, 16, base_port=9000, port_step=1) == ["9000", "9001"]


def test_derive_endpoint_ports_rejects_impossible_configurations():
    import pytest
    with pytest.raises(ValueError, match="dp_size must be >= 1"):
        derive_endpoint_ports(0, 1, 16)
    with pytest.raises(ValueError, match="local_world_size must be >= 1"):
        derive_endpoint_ports(4, 0, 16)
    with pytest.raises(ValueError, match="smaller than one DP rank"):
        derive_endpoint_ports(4, 16, 8)


def test_get_worker_endpoints_explicit_group(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)
    endpoints = get_worker_endpoints(
        {"type": "prefill", "endpoints": ["10.0.0.1:7104", "10.0.0.1:7100",
                                          "10.0.0.1:7102", "10.0.0.1:7106"]}
    )
    assert endpoints == ["10.0.0.1:7100", "10.0.0.1:7102",
                         "10.0.0.1:7104", "10.0.0.1:7106"]


def test_get_worker_endpoints_group_may_span_pods(monkeypatch):
    """Motor keys endpoints by ip, so one instance may cover several pods
    (Motor's A2 典配 spans 2 pods per instance; SGLang hybrid uses nnodes=2)."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)
    assert get_worker_endpoints(
        {"type": "decode", "endpoints": ["10.0.0.9:7100", "10.0.0.1:7100"]}
    ) == ["10.0.0.1:7100", "10.0.0.9:7100"]


def test_get_worker_endpoints_env_fallback(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("MOTOR_INSTANCE_ENDPOINTS", "10.0.0.1:7102, 10.0.0.1:7100")
    reload(envs)
    assert get_worker_endpoints({"type": "decode"}) == [
        "10.0.0.1:7100", "10.0.0.1:7102"
    ]


def test_get_worker_endpoints_from_ports(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)
    assert get_worker_endpoints({"type": "prefill", "ports": [7102, 7100]}) == [
        "10.0.0.1:7100", "10.0.0.1:7102"
    ]


def test_get_worker_endpoints_from_parallel_config(monkeypatch):
    """dp_size + device_num + local_world_size are enough to derive the ports
    with Motor's own convention (used when the platform only injects NPU/DP
    topology)."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)
    endpoints = get_worker_endpoints({
        "type": "decode",
        "parallel_config": {"dp_size": 16, "tp_size": 1},
        "device_num": 16,
        "base_port": 7100,
    })
    assert endpoints == ["10.0.0.1:%d" % port for port in A3_DECODE_PORTS]


def test_get_worker_endpoints_single_endpoint_stays_compatible(monkeypatch):
    """The original contract (one instance/port per sidecar) still works and
    yields a one-endpoint instance."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)
    assert get_worker_endpoints({"type": "prefill", "port": 8000}) == ["10.0.0.1:8000"]
    assert get_worker_endpoints(
        {"type": "prefill", "instance": "custom-svc:8500"}
    ) == ["custom-svc:8500"]


def test_get_worker_endpoints_rejects_empty_group(monkeypatch):
    import pytest
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)
    with pytest.raises(ValueError, match="non-empty list"):
        get_worker_endpoints({"type": "prefill", "endpoints": []})


def test_normalize_and_sort_endpoints_helpers():
    assert normalize_endpoint("10.0.0.1:8000") == "10.0.0.1:8000"
    assert normalize_endpoint({"address": "[2001:db8::1]:8200"}) == "[2001:db8::1]:8200"
    assert normalize_endpoint({"host": "svc-a", "port": 8200}) == "svc-a:8200"
    assert normalize_endpoint({"ip": "10.0.0.1"}) == "10.0.0.1:8000"
    # same key as Motor's CLI: (ip, int(port)) — ip is compared as a STRING, so
    # 10.0.0.10 sorts before 10.0.0.2 exactly as it does in register.py
    assert sort_endpoints(["10.0.0.10:8000", "10.0.0.2:8000", "10.0.0.2:9000"]) == [
        "10.0.0.10:8000", "10.0.0.2:8000", "10.0.0.2:9000"
    ]


def test_format_address():
    assert format_address("10.0.0.1", "8000") == "10.0.0.1:8000"
    assert format_address("demo-prefill-0.s-demo", "9000") == "demo-prefill-0.s-demo:9000"
    assert format_address("2001:db8::1", "8200") == "[2001:db8::1]:8200"


# ---------------------------------------------------------------------------
# MotorTopoClient instantiation
# ---------------------------------------------------------------------------

def test_init_prefill(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    client = MotorTopoClient(
        {"type": "prefill", "port": 8000}
    )
    assert client.role == "prefill"
    assert client.address == "10.0.0.1:8000"
    assert client.instance_id >= STANDALONE_ID_NAMESPACE
    assert client._registered is False


def test_init_decode(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.2")
    reload(envs)

    client = MotorTopoClient(
        {"worker_type": "decode", "port": 8001}
    )
    assert client.role == "decode"
    assert client.instance_id >= STANDALONE_ID_NAMESPACE


def test_init_uses_explicit_instance(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    client = MotorTopoClient(
        {"type": "prefill", "instance": "custom-svc:8500", "port": 8500}
    )
    assert client.address == "custom-svc:8500"


def test_init_multi_endpoint_instance(monkeypatch):
    """A dp4 x tp4 prefill sidecar: the instance id covers the whole endpoint
    group, and the health probe stays local (POD_IP) on the first DP port."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    client = MotorTopoClient({
        "type": "prefill",
        "endpoints": ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS],
    })
    assert client.endpoints == ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS]
    assert client.address == "10.0.0.1:7100"
    assert client.instance_id == MOTOR_CLI_PREFILL_4EP_ID
    assert client.health_check_endpoint == "10.0.0.1:7100"
    assert client._registered is False


def test_init_raises_without_worker_type(monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    import pytest
    with pytest.raises(ValueError, match="worker type is not set"):
        MotorTopoClient({"port": 8000})


def test_init_raises_without_rbg_group(monkeypatch):
    monkeypatch.delenv("RBG_GROUP_NAME", raising=False)
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    import pytest
    with pytest.raises(RuntimeError, match="RBG_GROUP_NAME is not set"):
        MotorTopoClient({"type": "prefill", "port": 8000})


# ---------------------------------------------------------------------------
# wait_engine_ready
# ---------------------------------------------------------------------------

@patch("patio.topo.client.motor_topo_client.requests.get")
def test_wait_engine_ready_ok(mock_get, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("TOPO_CONNECT_TIMEOUT", "4")
    monkeypatch.setenv("TOPO_HEALTH_CHECK_TIMEOUT", "35")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    mock_get.return_value = response

    with _patch_retry():
        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.wait_engine_ready({"port": 8000})
        mock_get.assert_called_once_with(
            "http://10.0.0.1:8000/health",
            timeout=(4.0, 35.0),
        )


@patch("patio.topo.client.motor_topo_client.requests.get")
def test_wait_engine_ready_fails_on_non_200(mock_get, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    response = MagicMock()
    response.status_code = 503
    response.text = "not ready"
    mock_get.return_value = response

    with _patch_retry():
        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.wait_engine_ready({"port": 8000}) is False


@patch("patio.topo.client.motor_topo_client.requests.get")
def test_engine_healthy_probes_every_dp_endpoint(mock_get, monkeypatch):
    """The gate covers the whole DP group: Motor rejects the instance when any
    DP is unreachable, so probing only the local rank is not enough."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    probed = []

    def _get(url, timeout=None):
        probed.append(url)
        response = MagicMock()
        response.status_code = 200
        return response

    mock_get.side_effect = _get

    endpoints = ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS]
    client = MotorTopoClient({"type": "prefill", "endpoints": endpoints})
    assert client._engine_healthy({}) is True
    assert sorted(probed) == sorted(
        "http://%s/health" % endpoint for endpoint in endpoints
    )


@patch("patio.topo.client.motor_topo_client.requests.get")
def test_engine_healthy_fails_when_one_dp_endpoint_is_down(mock_get, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    def _get(url, timeout=None):
        response = MagicMock()
        response.status_code = 503 if url == "http://10.0.0.1:7104/health" else 200
        return response

    mock_get.side_effect = _get

    client = MotorTopoClient({
        "type": "prefill",
        "endpoints": ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS],
    })
    assert client._engine_healthy({}) is False


@patch("patio.topo.client.motor_topo_client.requests.post")
@patch("patio.topo.client.motor_topo_client.requests.get")
def test_register_skips_add_when_one_dp_endpoint_is_down(mock_get, mock_post, monkeypatch):
    """A partially dead group is not registered at all: Motor would reject it."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    def _get(url, timeout=None):
        response = MagicMock()
        response.status_code = 503 if url == "http://10.0.0.1:7104/health" else 200
        return response

    mock_get.side_effect = _get

    client = MotorTopoClient({
        "type": "prefill",
        "endpoints": ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS],
    })
    assert client.register("", {"type": "prefill"}) is False
    mock_post.assert_not_called()
    assert client._registered is False


# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------

@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_posts_instances_refresh(mock_post, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("TOPO_CONNECT_TIMEOUT", "4")
    monkeypatch.setenv("TOPO_REGISTER_TIMEOUT", "12")
    monkeypatch.setenv("MOTOR_MODEL_NAME", "test-model")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        health = MagicMock()
        health.status_code = 200
        mock_get.return_value = health

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.register("", {"type": "prefill", "port": 8000})
        assert client._registered is True

        # Verify the POST went to the right URL with the right body
        call_args = mock_post.call_args
        assert call_args is not None
        url = call_args[0][0] if call_args[0] else call_args[1]["url"]
        assert "/instances/refresh" in url
        assert "demo-router-0.s-demo-router:9000" in url
        # ExternalInsEventMsg body: event + optional knobs + instances[]
        body = call_args[1]["json"]
        assert body["event"] == "add"
        assert body["model_name"] == "test-model"
        assert body["instances"][0]["id"] == client.instance_id
        assert body["instances"][0]["role"] == "prefill"
        assert body["instances"][0]["endpoints"] == [
            {"id": 0, "address": "10.0.0.1:8000"}
        ]
        # env-configured model name means no /v1/models probe
        model_urls = [c[0][0] for c in mock_get.call_args_list if "/v1/models" in c[0][0]]
        assert model_urls == []


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_resolves_model_name_from_engine(mock_post, monkeypatch):
    """register() probes the engine /v1/models when MOTOR_MODEL_NAME is unset,
    caches the name, and sends it in the request body."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        def _side_effect(url, *args, **kwargs):
            if "/health" in url:
                resp = MagicMock()
                resp.status_code = 200
                return resp
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"data": [{"id": "Qwen2.5-7B"}]}
            return resp
        mock_get.side_effect = _side_effect

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.register("", {"type": "prefill", "port": 8000})

        # engine /v1/models probed once (health check is a separate /health call)
        model_urls = [
            c[0][0] for c in mock_get.call_args_list if "/v1/models" in c[0][0]
        ]
        assert len(model_urls) == 1
        assert model_urls[0] == "http://10.0.0.1:8000/v1/models"

        body = mock_post.call_args[1]["json"]
        assert body["model_name"] == "Qwen2.5-7B"
        # cached for unregister after the engine dies
        assert client._model_name == "Qwen2.5-7B"


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_omits_model_name_when_probe_fails(mock_post, monkeypatch):
    """If the /v1/models probe fails, register() leaves model_name out: Motor
    probes the reachable DPs itself. A placeholder name could not match any real
    model, and Motor rejects a name the engines do not serve."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        def _side_effect(url, *args, **kwargs):
            resp = MagicMock()
            resp.status_code = 200 if "/health" in url else 404
            return resp
        mock_get.side_effect = _side_effect

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.register("", {"type": "prefill", "port": 8000})

        body = mock_post.call_args[1]["json"]
        assert "model_name" not in body
        assert client._model_name is None


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_returns_false_on_error(mock_post, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    response = MagicMock()
    response.status_code = 500
    response.text = "bad"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        health = MagicMock()
        health.status_code = 200
        mock_get.return_value = health

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.register("", {"type": "prefill", "port": 8000}) is False
        assert client._registered is False


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_posts_every_dp_endpoint_of_the_instance(mock_post, monkeypatch):
    """The registered body carries the whole group: Motor derives
    dp_size = len(endpoints) and routes each request to a DP rank, so a
    one-endpoint body would pin the instance to 1/16 of the decode node."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.2")
    monkeypatch.setenv("MOTOR_MODEL_NAME", "dsv4")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        health = MagicMock()
        health.status_code = 200
        mock_get.return_value = health

        worker_info = {
            "type": "decode",
            "endpoints": ["10.0.0.2:%d" % port for port in A3_DECODE_PORTS],
        }
        client = MotorTopoClient(worker_info)
        assert client.register("", worker_info)

        instance = mock_post.call_args[1]["json"]["instances"][0]
        assert instance["id"] == MOTOR_CLI_DECODE_16EP_ID
        assert instance["role"] == "decode"
        assert len(instance["endpoints"]) == 16
        assert [entry["id"] for entry in instance["endpoints"]] == list(range(16))
        # health probe stays local (POD_IP), on the first DP port
        assert client.health_check_endpoint == "10.0.0.2:7100"


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_does_not_retry_deterministic_4xx(mock_post, monkeypatch):
    """A 409 (this instance id is already registered with a different shape) or a
    400 can never start succeeding. register() runs on the 10s heartbeat, so it
    must fail immediately instead of burning the 60 x 3s retry budget on every
    beat."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("MOTOR_MODEL_NAME", "dsv4")
    reload(envs)

    response = MagicMock()
    response.status_code = 409
    response.text = "instance ID 1147345019 does not match the registered instance"
    mock_post.return_value = response

    with patch("patio.topo.client.motor_topo_client.requests.get") as mock_get, patch(
        "patio.topo.client.motor_topo_client.time.sleep", return_value=None
    ):
        health = MagicMock()
        health.status_code = 200
        mock_get.return_value = health

        # utils.retry is deliberately NOT stubbed: a regression that retried the
        # 4xx would issue 60 POSTs here (fast, because sleep is stubbed).
        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.register("", {"type": "prefill", "port": 8000}) is False
        assert mock_post.call_count == 1
        assert client._registered is False


# ---------------------------------------------------------------------------
# heartbeat-driven failure detection: unhealthy engine -> deregister
# ---------------------------------------------------------------------------

@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_skips_when_engine_unhealthy_and_not_registered(mock_post, monkeypatch):
    """An unhealthy engine must not be (re-)registered."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        health = MagicMock()
        health.status_code = 503
        mock_get.return_value = health

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        assert client.register("", {"type": "prefill", "port": 8000}) is False
        mock_post.assert_not_called()


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_register_deregisters_when_engine_becomes_unhealthy(mock_post, monkeypatch):
    """Multi-DP failure scenario: once the endpoint goes down, the heartbeat
    thread's register() call must send event=del so the Coordinator stops
    routing traffic to this (P) instance."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("MOTOR_MODEL_NAME", "test-model")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        def _side_effect(url, *args, **kwargs):
            resp = MagicMock()
            # /health returns 200 first (healthy register), then 503 (engine died)
            resp.status_code = 200 if "/health" in url and mock_get.call_count <= 1 else 503
            return resp
        mock_get.side_effect = _side_effect

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        # first heartbeat: engine healthy -> add
        assert client.register("", {"type": "prefill", "port": 8000}) is True
        assert client._registered is True
        events = [c[1]["json"]["event"] for c in mock_post.call_args_list]
        assert events == ["add"]

        # next heartbeat: engine dead -> must del (deregister)
        assert client.register("", {"type": "prefill", "port": 8000}) is False
        assert client._registered is False
        events = [c[1]["json"]["event"] for c in mock_post.call_args_list]
        assert events == ["add", "del"]

        # del carries the cached model name (engine is dead, no re-probe)
        del_body = mock_post.call_args_list[-1][1]["json"]
        assert del_body["model_name"] == "test-model"


# ---------------------------------------------------------------------------
# unregister
# ---------------------------------------------------------------------------

@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_posts_instance_del(mock_post, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("MOTOR_MODEL_NAME", "test-model")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry():
        client = MotorTopoClient({"type": "prefill", "port": 8000})
        client._registered = True
        assert client.unregister()
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        body = call_kwargs["json"]
        assert body["event"] == "del"
        # Motor needs a resolvable model_name for del too; carried from env/cache
        # so that its own /v1/models probe is skipped
        assert body["model_name"] == "test-model"
        assert client._registered is False


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_carries_the_registered_endpoint_group(mock_post, monkeypatch):
    """Motor matches a deletion by role + the physical endpoint set
    (_same_delete_identity) and answers a partial group with a 409, so del must
    repeat every endpoint that add advertised."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    monkeypatch.setenv("MOTOR_MODEL_NAME", "dsv4")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry():
        client = MotorTopoClient({
            "type": "prefill",
            "endpoints": ["10.0.0.1:%d" % port for port in A3_PREFILL_PORTS],
        })
        client._registered = True
        assert client.unregister()
        assert client._registered is False
        instance = mock_post.call_args[1]["json"]["instances"][0]
        assert instance["id"] == MOTOR_CLI_PREFILL_4EP_ID
        assert [entry["address"] for entry in instance["endpoints"]] == [
            "10.0.0.1:%d" % port for port in A3_PREFILL_PORTS
        ]


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_skips_when_not_registered(mock_post, monkeypatch):
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    client = MotorTopoClient({"type": "prefill", "port": 8000})
    assert client.unregister() is True
    mock_post.assert_not_called()


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_carries_cached_model_name_after_engine_death(mock_post, monkeypatch):
    """unregister() reuses the name resolved at register time even though the
    engine is now dead (Motor's own fallback probe would 400 against it)."""
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("POD_IP", "10.0.0.1")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    with _patch_retry(), patch(
        "patio.topo.client.motor_topo_client.requests.get"
    ) as mock_get:
        def _side_effect(url, *args, **kwargs):
            if "/health" in url:
                resp = MagicMock()
                resp.status_code = 200
                return resp
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"data": [{"id": "Qwen2.5-7B"}]}
            return resp
        mock_get.side_effect = _side_effect

        client = MotorTopoClient({"type": "prefill", "port": 8000})
        # register() resolves and caches the model name (engine alive then)
        assert client.register("", {"type": "prefill", "port": 8000})
        mock_get.reset_mock()
        # engine dies; unregister must NOT probe /v1/models again
        assert client.unregister()
        mock_get.assert_not_called()

        body = mock_post.call_args[1]["json"]
        assert body["event"] == "del"
        assert body["model_name"] == "Qwen2.5-7B"


# ---------------------------------------------------------------------------
# unregister: shutdown-safe behaviour (bounded, fail-fast)
#
# unregister() runs inside the SIGTERM/SIGINT handler, so it must not inherit
# the 60x3s registration retry budget (that would delay pod termination by
# minutes). These tests pin the bounded/fail-fast contract.
# ---------------------------------------------------------------------------


def _make_registered_client(monkeypatch):
    """A MotorTopoClient that believes it is registered (as after a successful add).

    Deadline/interval are shrunk so the bounded-retry tests stay fast.
    """
    _set_motor_env(monkeypatch)
    monkeypatch.setenv("MOTOR_MODEL_NAME", "test-model")
    client = _make_client(monkeypatch)
    monkeypatch.setattr(envs, "MOTOR_UNREGISTER_DEADLINE", 0.3)
    monkeypatch.setattr(envs, "MOTOR_UNREGISTER_INTERVAL", 0.01)
    monkeypatch.setattr(envs, "MOTOR_UNREGISTER_TIMEOUT", 1.0)
    client._model_name = "test-model"
    client._registered = True
    return client


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_treats_404_as_already_deregistered(mock_post, monkeypatch):
    """A Coordinator that no longer knows the instance (restart) means the
    desired end state is already reached: report success, do not retry."""
    client = _make_registered_client(monkeypatch)

    response = MagicMock()
    response.status_code = 404
    response.text = "not found"
    mock_post.return_value = response

    assert client.unregister() is True
    assert mock_post.call_count == 1
    assert client._registered is False


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_fails_fast_on_4xx(mock_post, monkeypatch):
    """4xx cannot start succeeding by retrying (bad body / identity mismatch /
    rejected API key): stop immediately instead of sleeping out the deadline."""
    client = _make_registered_client(monkeypatch)

    response = MagicMock()
    response.status_code = 400
    response.text = "invalid request format"
    mock_post.return_value = response

    assert client.unregister() is False
    assert mock_post.call_count == 1
    # Still registered on the Coordinator as far as we know -> mirror stays True.
    assert client._registered is True


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_retries_5xx_then_succeeds(mock_post, monkeypatch):
    """5xx is transient: retry within the deadline and report success."""
    client = _make_registered_client(monkeypatch)

    transient = MagicMock()
    transient.status_code = 503
    transient.text = "unavailable"
    success = MagicMock()
    success.status_code = 200
    success.text = "ok"
    mock_post.side_effect = [transient, success]

    assert client.unregister() is True
    assert mock_post.call_count == 2
    assert client._registered is False


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_is_bounded_by_deadline_when_coordinator_unreachable(mock_post, monkeypatch):
    """With the Coordinator gone, unregister() must give up after the deadline
    instead of blocking SIGTERM for the 60x3s registration budget."""
    client = _make_registered_client(monkeypatch)
    mock_post.side_effect = RuntimeError("connection refused")

    started = time.monotonic()
    assert client.unregister() is False
    elapsed = time.monotonic() - started

    assert elapsed < 2.0, f"unregister blocked for {elapsed:.1f}s, deadline was 0.3s"
    assert mock_post.call_count >= 1
    # Untruthful to claim we left the pool -> mirror stays True, and the state is
    # re-derivable: a later healthy heartbeat re-adds the instance.
    assert client._registered is True


@patch("patio.topo.client.motor_topo_client.requests.post")
def test_unregister_recovery_after_earlier_failure(mock_post, monkeypatch):
    """A failed deregistration must not wedge the client: once the Coordinator is
    back, the next unregister() (or heartbeat register) succeeds normally."""
    client = _make_registered_client(monkeypatch)
    mock_post.side_effect = RuntimeError("connection refused")
    assert client.unregister() is False

    success = MagicMock()
    success.status_code = 200
    success.text = "ok"
    mock_post.side_effect = None
    mock_post.return_value = success

    assert client.unregister() is True
    assert client._registered is False
    # every attempt was a `del` for the same instance
    assert {c[1]["json"]["event"] for c in mock_post.call_args_list} == {"del"}


# ---------------------------------------------------------------------------
# payload knobs the External Deployer protocol constrains
# ---------------------------------------------------------------------------

def test_detect_worker_type_normalizes_case_and_whitespace():
    assert detect_worker_type({"type": "  Prefill "}) == "prefill"
    assert detect_worker_type({"worker_type": "DECODE"}) == "decode"


def test_detect_worker_type_rejects_roles_a_sidecar_cannot_register():
    """ExternalInstance.role only accepts prefill/decode, so Motor answers a bare
    400 ("unsupported external instance role") for the Controller-only roles.
    Reject them before any HTTP call instead."""
    for bad_role in ("encode", "union", "both", "P", "D", "Prefill-Decode", "router"):
        try:
            detect_worker_type({"type": bad_role})
        except ValueError as exc:
            assert "invalid worker type" in str(exc)
        else:
            raise AssertionError(f"role {bad_role!r} should have been rejected")


def test_detect_worker_type_still_requires_a_role():
    try:
        detect_worker_type({})
    except ValueError as exc:
        assert "worker type is not set" in str(exc)
    else:
        raise AssertionError("missing type must raise")


def test_get_engine_type_defaults_to_vllm(monkeypatch):
    monkeypatch.delenv("MOTOR_ENGINE_TYPE", raising=False)
    assert get_engine_type() == "vllm"
    assert build_body("add", "prefill", "10.0.0.1:8000", 1, "m")["engine_type"] == "vllm"


def test_get_engine_type_rejects_engines_the_external_protocol_cannot_serve(monkeypatch):
    """The External Deployer protocol is vllm-only ("currently supports
    engine_type='vllm' only"); sglang belongs to the Controller-shaped schema."""
    for engine in (" SGLang ", "tgi"):
        monkeypatch.setenv("MOTOR_ENGINE_TYPE", engine)
        try:
            get_engine_type()
        except ValueError as exc:
            assert "invalid MOTOR_ENGINE_TYPE" in str(exc)
        else:
            raise AssertionError(f"engine {engine!r} must be rejected")


def test_dispatch_capability_defaults_to_vllm_handoff(monkeypatch):
    monkeypatch.delenv("MOTOR_DISPATCH_CAPABILITIES", raising=False)
    assert get_dispatch_capability() == "prefill_handoff_decode"
    body = build_body("add", "decode", "10.0.0.2:8000", 2, "m")
    assert body["dispatch_capabilities"] == "prefill_handoff_decode"


def test_dispatch_capability_override(monkeypatch):
    monkeypatch.setenv("MOTOR_DISPATCH_CAPABILITIES", " concurrent_engine_sync ")
    assert get_dispatch_capability() == "concurrent_engine_sync"

    # empty override == "let Motor assume the vLLM default" (HANDOFF)
    for empty in ("  ", ""):
        monkeypatch.setenv("MOTOR_DISPATCH_CAPABILITIES", empty)
        assert get_dispatch_capability() == "prefill_handoff_decode"


def test_dispatch_capability_rejects_multiple_or_unknown_plans(monkeypatch):
    """The external schema carries exactly one plan from a closed set: a list
    (Controller shape) or a plan Motor does not implement is rejected server-side."""
    for raw in ("concurrent_engine_sync, decode_colocation", "decode_colocation"):
        monkeypatch.setenv("MOTOR_DISPATCH_CAPABILITIES", raw)
        try:
            get_dispatch_capability()
        except ValueError as exc:
            assert "MOTOR_DISPATCH_CAPABILITIES" in str(exc)
        else:
            raise AssertionError(f"MOTOR_DISPATCH_CAPABILITIES={raw!r} should have been rejected")


def test_build_body_is_self_consistent_for_deregistration():
    """del must carry the same identity as add: Motor matches the instance by
    role/endpoint identity, and needs model_name only to skip its own probe."""
    add = build_body("add", "prefill", "10.0.0.1:8000", 42, "m")
    delete = build_body("del", "prefill", "10.0.0.1:8000", 42, "m")
    assert {k: v for k, v in add.items() if k != "event"} == {
        k: v for k, v in delete.items() if k != "event"
    }

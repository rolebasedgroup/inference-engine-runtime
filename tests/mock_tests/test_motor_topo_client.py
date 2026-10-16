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
    derive_instance_id,
    detect_worker_type,
    format_address,
    get_dispatch_capability,
    get_engine_type,
    get_motor_endpoint,
    get_worker_address,
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
    body = build_body(event="add", role="prefill", address="10.0.0.1:8000",
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
    body = build_body(event="del", role="decode", address="10.0.0.2:8000",
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
    body = build_body(event="add", role="prefill", address="[2001:db8::1]:8200",
                      instance_id=7)
    assert body["instances"][0]["endpoints"] == [{"id": 0, "address": "[2001:db8::1]:8200"}]


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

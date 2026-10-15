# -*- coding: utf-8 -*-
"""
Tests for the vLLM proxy topology client.
"""

from importlib import reload
from unittest.mock import MagicMock, patch

from patio import envs
from patio.topo.client.vllm_topo_client import (
    VLLMProxyTopoClient,
    get_vllm_proxy_endpoint,
    get_worker_instance,
)


def test_get_vllm_proxy_endpoint_uses_generic_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    reload(envs)

    endpoint = get_vllm_proxy_endpoint({})

    assert endpoint == "demo-proxy-0.s-demo-proxy:9000"


def test_get_vllm_proxy_endpoint_uses_legacy_sgl_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.delenv("ROUTER_ROLE_NAME", raising=False)
    monkeypatch.delenv("ROUTER_PORT", raising=False)
    monkeypatch.setenv("SGL_ROUTER_ROLE_NAME", "legacy-proxy")
    monkeypatch.setenv("SGL_ROUTER_PORT", "9001")
    reload(envs)

    endpoint = get_vllm_proxy_endpoint({})

    assert endpoint == "demo-legacy-proxy-0.s-demo-legacy-proxy:9001"


def test_get_vllm_proxy_endpoint_prefers_generic_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    monkeypatch.setenv("SGL_ROUTER_ROLE_NAME", "legacy-proxy")
    monkeypatch.setenv("SGL_ROUTER_PORT", "9001")
    reload(envs)

    endpoint = get_vllm_proxy_endpoint({})

    assert endpoint == "demo-proxy-0.s-demo-proxy:9000"


def test_get_worker_instance_prefers_explicit_instance(monkeypatch):
    monkeypatch.setenv("POD_IP", "10.0.0.8")

    instance = get_worker_instance({"instance": "localhost:8102", "port": 8102})

    assert instance == "localhost:8102"


def test_get_worker_instance_uses_pod_ip_and_port(monkeypatch):
    monkeypatch.setenv("POD_IP", "10.0.0.8")

    instance = get_worker_instance({"port": 8102})

    assert instance == "10.0.0.8:8102"


@patch("patio.topo.client.vllm_topo_client.requests.get")
def test_wait_engine_ready_uses_health_check_timeout(mock_get, monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    monkeypatch.setenv("POD_IP", "10.0.0.8")
    monkeypatch.setenv("TOPO_CONNECT_TIMEOUT", "4")
    monkeypatch.setenv("TOPO_HEALTH_CHECK_TIMEOUT", "35")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    mock_get.return_value = response

    client = VLLMProxyTopoClient({"port": 8102})

    assert client.wait_engine_ready({"port": 8102})
    mock_get.assert_called_once_with(
        "http://10.0.0.8:8102/health",
        timeout=(4.0, 35.0),
    )


@patch("patio.topo.client.vllm_topo_client.requests.post")
def test_register_posts_instances_add_payload(mock_post, monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    monkeypatch.setenv("TOPO_CONNECT_TIMEOUT", "4")
    monkeypatch.setenv("TOPO_REGISTER_TIMEOUT", "12")
    reload(envs)

    response = MagicMock()
    response.status_code = 200
    response.text = "ok"
    mock_post.return_value = response

    client = VLLMProxyTopoClient({"type": "prefill", "instance": "localhost:8102"})

    assert client.register("", {"type": "prefill", "instance": "localhost:8102"})
    mock_post.assert_called_once_with(
        "http://demo-proxy-0.s-demo-proxy:9000/instances/add",
        json={"type": "prefill", "instance": "localhost:8102"},
        headers={"Content-Type": "application/json"},
        timeout=(4.0, 12.0),
    )


@patch("patio.topo.client.vllm_topo_client.requests.post")
def test_register_returns_false_for_proxy_error(mock_post, monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    reload(envs)

    response = MagicMock()
    response.status_code = 500
    response.text = "failed"
    mock_post.return_value = response

    client = VLLMProxyTopoClient({"type": "prefill", "instance": "localhost:8102"})

    with patch(
        "patio.topo.client.vllm_topo_client.utils.retry",
        side_effect=lambda func, **_: func(),
    ):
        assert (
            client.register("", {"type": "prefill", "instance": "localhost:8102"})
            is False
        )


def test_unregister_is_noop(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    reload(envs)

    client = VLLMProxyTopoClient({"type": "prefill", "instance": "localhost:8102"})

    assert client.unregister() is True

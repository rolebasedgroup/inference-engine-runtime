# -*- coding: utf-8 -*-
"""
Tests for the SGLang topology client helpers.
"""

from importlib import reload
from unittest.mock import MagicMock, patch

from patio import envs
from patio.topo.client.sgl_topo_client import (
    SGLangGroupTopoClient,
    get_sgl_router_endpoint,
)


def create_sgl_client(
    monkeypatch,
    connect_timeout="3",
    health_check_timeout="30",
    register_timeout="10",
):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "8000")
    monkeypatch.setenv("POD_IP", "10.0.0.8")
    monkeypatch.setenv("TOPO_CONNECT_TIMEOUT", connect_timeout)
    monkeypatch.setenv("TOPO_HEALTH_CHECK_TIMEOUT", health_check_timeout)
    monkeypatch.setenv("TOPO_REGISTER_TIMEOUT", register_timeout)
    reload(envs)
    SGLangGroupTopoClient._instance = None
    return SGLangGroupTopoClient({"port": 30000})


def test_get_sgl_router_endpoint_uses_generic_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "8000")
    reload(envs)

    endpoint = get_sgl_router_endpoint({})

    assert endpoint == "demo-router-0.s-demo-router:8000"


def test_get_sgl_router_endpoint_uses_legacy_sgl_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.delenv("ROUTER_ROLE_NAME", raising=False)
    monkeypatch.delenv("ROUTER_PORT", raising=False)
    monkeypatch.setenv("SGL_ROUTER_ROLE_NAME", "legacy-router")
    monkeypatch.setenv("SGL_ROUTER_PORT", "8001")
    reload(envs)

    endpoint = get_sgl_router_endpoint({})

    assert endpoint == "demo-legacy-router-0.s-demo-legacy-router:8001"


def test_get_sgl_router_endpoint_prefers_generic_router_env(monkeypatch):
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "8000")
    monkeypatch.setenv("SGL_ROUTER_ROLE_NAME", "legacy-router")
    monkeypatch.setenv("SGL_ROUTER_PORT", "8001")
    reload(envs)

    endpoint = get_sgl_router_endpoint({})

    assert endpoint == "demo-router-0.s-demo-router:8000"


@patch("patio.topo.client.sgl_topo_client.requests.get")
def test_wait_engine_ready_uses_health_check_timeout(mock_get, monkeypatch):
    response = MagicMock()
    response.status_code = 200
    mock_get.return_value = response
    client = create_sgl_client(
        monkeypatch,
        connect_timeout="4",
        health_check_timeout="35",
    )

    assert client.wait_engine_ready({"port": 30000})
    mock_get.assert_called_once_with(
        "http://10.0.0.8:30000/health",
        timeout=(4.0, 35.0),
    )


@patch("patio.topo.client.sgl_topo_client.requests.post")
def test_register_uses_register_timeout(mock_post, monkeypatch):
    response = MagicMock()
    response.status_code = 202
    response.json.return_value = {"worker_id": "worker-1"}
    mock_post.return_value = response
    client = create_sgl_client(
        monkeypatch,
        connect_timeout="4",
        register_timeout="12",
    )

    assert client.register("", {"port": 30000, "worker_type": "prefill"})
    mock_post.assert_called_once_with(
        "http://demo-router-0.s-demo-router:8000/workers",
        json={"worker_type": "prefill", "url": "http://10.0.0.8:30000"},
        headers={"Content-Type": "application/json"},
        timeout=(4.0, 12.0),
    )


@patch("patio.topo.client.sgl_topo_client.requests.delete")
def test_unregister_uses_register_timeout(mock_delete, monkeypatch):
    response = MagicMock()
    response.status_code = 202
    mock_delete.return_value = response
    client = create_sgl_client(
        monkeypatch,
        connect_timeout="4",
        register_timeout="12",
    )
    client.worker_id = "worker-1"

    assert client.unregister()
    mock_delete.assert_called_once_with(
        "http://demo-router-0.s-demo-router:8000/workers/worker-1",
        timeout=(4.0, 12.0),
    )

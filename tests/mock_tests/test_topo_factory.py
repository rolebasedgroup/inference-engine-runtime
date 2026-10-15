# -*- coding: utf-8 -*-
"""
Tests for the topology factory.
"""

from importlib import reload

import pytest

from patio import envs
from patio.topo.factory import create_topo_client, create_topo_server
from patio.topo.client.sgl_topo_client import SGLangGroupTopoClient
from patio.topo.client.vllm_topo_client import VLLMProxyTopoClient
from patio.topo.server.sgl_topo_server import SGLangGroupTopoServer


def test_create_topo_client_sglang(monkeypatch):
    """Test that create_topo_client returns SGLang client for sglang type."""
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "router")
    monkeypatch.setenv("ROUTER_PORT", "8000")
    monkeypatch.setenv("POD_IP", "10.0.0.8")
    reload(envs)
    SGLangGroupTopoClient._instance = None

    client = create_topo_client("sglang", {"port": 8000})
    assert isinstance(client, SGLangGroupTopoClient)


def test_create_topo_client_vllm(monkeypatch):
    """Test that create_topo_client returns vLLM proxy client for vllm type."""
    monkeypatch.setenv("RBG_GROUP_NAME", "demo")
    monkeypatch.setenv("ROUTER_ROLE_NAME", "proxy")
    monkeypatch.setenv("ROUTER_PORT", "9000")
    reload(envs)

    client = create_topo_client(
        "vllm", {"type": "prefill", "instance": "localhost:8102"}
    )
    assert isinstance(client, VLLMProxyTopoClient)


def test_create_topo_client_invalid():
    """Test that create_topo_client raises ValueError for invalid type."""
    with pytest.raises(ValueError, match="Invalid topo type: invalid"):
        create_topo_client("invalid", {})


def test_create_topo_server_sglang():
    """Test that create_topo_server returns SGLang server for sglang type."""
    server = create_topo_server("sglang")
    assert isinstance(server, SGLangGroupTopoServer)


def test_create_topo_server_invalid():
    """Test that create_topo_server raises ValueError for invalid type."""
    with pytest.raises(ValueError, match="Invalid topo type: invalid"):
        create_topo_server("invalid")

# -*- coding: utf-8 -*-
# @Author: zibai.gj

# -*- coding: utf-8 -*-
# @Author: zibai.gj
# @Time  : 2025-03-07

import os

from patio.config import DEFAULT_PATIO_PORT

# Inference Engine Config
INFERENCE_ENGINE = os.getenv("INFERENCE_ENGINE", "sglang")
INFERENCE_ENGINE_VERSION = os.getenv("INFERENCE_ENGINE_VERSION", "v0.5.3")
INFERENCE_ENGINE_ENDPOINT = os.getenv("INFERENCE_ENGINE_ENDPOINT")

# Prometheus
PROMETHEUS_MULTIPROC_DIR = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/patio/metrics/")

# Standard Inference Engine Metrics
METRIC_SCRAPE_PATH = os.getenv("METRIC_SCRAPE_PATH", "/metrics")

# Topo
TOPO_TYPE = os.getenv("TOPO_TYPE")
TOPO_CONFIG_FILE = os.getenv("TOPO_CONFIG_FILE", "/etc/patio/instance-config.yaml")
ROUTER_ROLE_NAME = os.getenv("ROUTER_ROLE_NAME") or os.getenv("SGL_ROUTER_ROLE_NAME")
ROUTER_PORT = os.getenv("ROUTER_PORT") or os.getenv("SGL_ROUTER_PORT")
TOPO_CONNECT_TIMEOUT = float(os.getenv("TOPO_CONNECT_TIMEOUT", "3"))
TOPO_HEALTH_CHECK_TIMEOUT = float(os.getenv("TOPO_HEALTH_CHECK_TIMEOUT", "30"))
TOPO_REGISTER_TIMEOUT = float(os.getenv("TOPO_REGISTER_TIMEOUT", "10"))
SCHEDULER_ROLE_NAME = os.getenv("SCHEDULER_ROLE_NAME")
_topo_register_endpoint = os.getenv("TOPO_REGISTER_ENDPOINT")

# Patio config
HEALTH_CHECK_INTERVAL = os.getenv("HEALTH_CHECK_INTERVAL", 300)
HEARTBEAT_INTERVAL = os.getenv("HEARTBEAT_INTERVAL", 10)

# Event callback defaults. The helpers read the process environment lazily so
# tests and embedded callers can override it with monkeypatch.
EVENT_POLL_INTERVAL = "1"
EVENT_CALLBACK_RETRY_TIMES = "60"
EVENT_CALLBACK_RETRY_INTERVAL = "3"
# End callbacks must fit inside a Pod's termination grace period. The legacy
# 60 x 3s retry loop is too long for the default 30s grace period.
EVENT_END_CALLBACK_RETRY_TIMES = "5"
EVENT_END_CALLBACK_RETRY_INTERVAL = "1"
EVENT_END_CALLBACK_TIMEOUT = "20"
# The downwardAPI label file can lag label removal by tens of seconds.
EVENT_SIGNAL_INTERVAL = "60"


def get_event_poll_interval() -> float:
    return float(os.getenv("EVENT_POLL_INTERVAL", EVENT_POLL_INTERVAL))


def get_event_callback_retry_times() -> int:
    return int(os.getenv("EVENT_CALLBACK_RETRY_TIMES", EVENT_CALLBACK_RETRY_TIMES))


def get_event_callback_retry_interval() -> float:
    return float(
        os.getenv("EVENT_CALLBACK_RETRY_INTERVAL", EVENT_CALLBACK_RETRY_INTERVAL)
    )


def get_event_end_callback_retry_times() -> int:
    return int(
        os.getenv("EVENT_END_CALLBACK_RETRY_TIMES", EVENT_END_CALLBACK_RETRY_TIMES)
    )


def get_event_end_callback_retry_interval() -> float:
    return float(
        os.getenv(
            "EVENT_END_CALLBACK_RETRY_INTERVAL", EVENT_END_CALLBACK_RETRY_INTERVAL
        )
    )


def get_event_end_callback_timeout() -> float:
    return float(
        os.getenv("EVENT_END_CALLBACK_TIMEOUT", EVENT_END_CALLBACK_TIMEOUT)
    )


def get_event_signal_interval() -> float:
    return float(os.getenv("EVENT_SIGNAL_INTERVAL", EVENT_SIGNAL_INTERVAL))


def is_lifecycle_api_insecure() -> bool:
    return os.getenv("LIFECYCLE_API_INSECURE", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }

# RBG Env
GROUP_NAME = os.getenv("RBG_GROUP_NAME")
ROLE_NAME = os.getenv("RBG_ROLE_NAME")
ROLE_INDEX = os.getenv("RBG_ROLE_INDEX")

LWS_GROUP_SIZE = os.getenv("LWS_GROUP_SIZE")


def get_topo_register_endpoint() -> str | None:
    if _topo_register_endpoint:
        return _topo_register_endpoint
    if SCHEDULER_ROLE_NAME and GROUP_NAME:
        return f"http://{GROUP_NAME}-{SCHEDULER_ROLE_NAME}-0.{GROUP_NAME}-{SCHEDULER_ROLE_NAME}:{DEFAULT_PATIO_PORT}"
    return None

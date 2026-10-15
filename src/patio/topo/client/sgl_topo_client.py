# -*- coding: utf-8 -*-
# @Author: zibai.gj
import os
import traceback

import requests
from typing import Optional

from patio import envs
from patio.logger import init_logger
from patio.topo.client.base_topo_client import GroupTopoClient
from patio.topo import utils

logger = init_logger(__name__)

def get_rbg_endpoint(group_name: str, role_name: str, index: str, port: str) -> Optional[str]:
    return f"{group_name}-{role_name}-{index}.s-{group_name}-{role_name}:{port}"

def get_sgl_router_endpoint(worker_info: dict) -> Optional[str]:
    rbg_group_name = envs.GROUP_NAME
    if rbg_group_name is None:
        raise RuntimeError("RBG_GROUP_NAME is not set")

    router_role_name = envs.ROUTER_ROLE_NAME
    if router_role_name is None:
        raise RuntimeError("ROUTER_ROLE_NAME is not set")

    router_port = envs.ROUTER_PORT
    if router_port is None:
        raise RuntimeError("ROUTER_PORT is not set")

    return get_rbg_endpoint(rbg_group_name, router_role_name, "0", router_port)

def get_worker_endpoint(worker_info: dict) -> Optional[str]:
    port = worker_info.get("port", "30000")

    worker_endpoint = os.getenv("POD_IP")
    if worker_endpoint is not None:
        return f"{worker_endpoint}:{port}"

    # Use headless service pod domain if POD_IP is not set
    rbg_group_name = os.getenv("RBG_GROUP_NAME")
    if rbg_group_name is None:
        raise RuntimeError("RBG_GROUP_NAME is not set")

    role_name = os.getenv("RBG_ROLE_NAME")
    if role_name is None:
        raise RuntimeError("RBG_ROLE_NAME is not set")

    role_index = os.getenv("RBG_ROLE_INDEX")
    if role_index is None:
        raise RuntimeError("RBG_ROLE_INDEX is not set")

    return get_rbg_endpoint(rbg_group_name, role_name, role_index, port)

def get_health_check_endpoint(worker_info: dict) -> Optional[str]:
    port = worker_info.get("port", "30000")

    local_url = os.getenv("POD_IP")
    if local_url is None:
        local_url = "localhost"

    return f"{local_url}:{port}"


class SGLangGroupTopoClient(GroupTopoClient):
    _instance = None

    # Singleton
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SGLangGroupTopoClient, cls).__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, worker_info: dict):
        if not self.__initialized:
            self.health_check_endpoint = get_health_check_endpoint(worker_info)
            self.worker_endpoint = get_worker_endpoint(worker_info)
            self.sgl_router_endpoint = get_sgl_router_endpoint(worker_info)
            self.worker_id = None
            self.__initialized = True

    def wait_engine_ready(self, worker_info: dict) -> bool:
        def f():
            health_check_url = f"http://{self.health_check_endpoint}/health"
            resp = requests.get(
                health_check_url,
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_HEALTH_CHECK_TIMEOUT),
            )
            if resp.status_code == 200:
                logger.info("Health check OK, inference engine is now ready.")
            else:
                raise RuntimeError(
                    f"health check failed, url: {health_check_url}, status_code: {resp.status_code}, content: {resp.text}")

        try:
            utils.retry(f, retry_times=60, interval=3)
            return True
        except Exception as e:
            logger.error(f"failed to check if worker engine is ready: {e}")
            traceback.print_exc()
            return False

    def register(self, url: str, worker_info: dict, file_path: Optional[str] = None) -> bool:
        """
        worker_info example:
            port: 8000
            worker_type: "prefill"
            bootstrap_port: 34000
        """

        worker_info = worker_info.copy()

        url = f"http://{self.worker_endpoint}"
        worker_info["url"] = url
        # port is already included in self.worker_endpoint
        del worker_info["port"]

        def f():
            worker_registration_url = f"http://{self.sgl_router_endpoint}/workers"
            resp = requests.post(
                worker_registration_url,
                json=worker_info,
                headers={"Content-Type": "application/json"},
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
            )
            if resp.status_code == 202:
                # Status Code 202 Accepted
                self.worker_id = resp.json().get("worker_id")
                if self.worker_id is None:
                    raise RuntimeError(
                        f"register failed: missing worker_id in response body, "
                        f"url: {worker_registration_url}, status_code: {resp.status_code}, content: {resp.text}"
                    )
                logger.info(f"registered worker successfully. worker_id: {self.worker_id}")
            else:
                raise RuntimeError(f"register failed, url: {worker_registration_url}, status_code: {resp.status_code}, content: {resp.text}")

        try:
            utils.retry(f, retry_times=60, interval=3)
            return True
        except Exception as e:
            logger.error(f"failed to register worker: {e}")
            traceback.print_exc()
            return False


    def unregister(self):
        if self.worker_id is None:
            logger.warning("worker_id is not set, skipping unregister (registration may have failed)")
            return False

        def f():
            worker_registration_url = f"http://{self.sgl_router_endpoint}/workers/{self.worker_id}"
            resp = requests.delete(
                worker_registration_url,
                timeout=(envs.TOPO_CONNECT_TIMEOUT, envs.TOPO_REGISTER_TIMEOUT),
            )
            if resp.status_code == 202:
                # Status Code 202 Accepted
                logger.info(f"unregistered worker successfully. worker_id: {self.worker_id}")
            else:
                raise RuntimeError(
                    f"unregister failed, url: {worker_registration_url}, status_code: {resp.status_code}, content: {resp.text}")

        try:
            utils.retry(f, retry_times=60, interval=3)
            return True
        except Exception as e:
            logger.error(f"failed to unregister worker: {e}")
            traceback.print_exc()
            return False

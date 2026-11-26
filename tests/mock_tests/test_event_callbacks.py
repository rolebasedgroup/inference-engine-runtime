"""Tests for event callback configuration and execution."""

import asyncio
import logging
import socket
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from patio.events.executor import EventExecutor, MemorySignalSource
from patio.events.loader import load_event_bindings
from patio.events.models import EventConfigError, parse_event_callbacks, parse_signal
from patio.events.registry import register_callback, reset_callback_registry
from patio.events.registry import callback_plugins
from patio.application import PatioApplication, parse_worker_instance_info
from patio.adapters.engine.sglang import SGLangEngineProbe
from patio.adapters.engine.vllm import VLLMEngineProbe
from patio.adapters.router.sglang import SGLangRouterAdapter
from patio.adapters.router.vllm import VLLMRouterAdapter
from patio.events.executor import SignalChangedError
from patio.lifecycle.signal_file import FileSignalSource, parse_downward_labels
from patio.lifecycle.signal_label import (
    KubernetesSignalLabelSource,
    _escape_json_pointer,
)


@pytest.fixture(autouse=True)
def clear_callback_registry():
    reset_callback_registry()
    yield
    reset_callback_registry()


def test_parse_signal_accepts_key_value():
    assert parse_signal(" patio.com/router = offboard ") == "patio.com/router=offboard"


@pytest.mark.parametrize(
    "value",
    [
        "patio.com/router",
        "patio.com/router=offboard=extra",
        "=offboard",
        "patio.com/router=",
        123,
    ],
)
def test_parse_signal_rejects_invalid_values(value):
    with pytest.raises(EventConfigError):
        parse_signal(value)


def test_parse_event_callbacks_rejects_unknown_fields():
    with pytest.raises(EventConfigError, match="unknown event_callbacks fields"):
        parse_event_callbacks({"enabled": True})


def test_parse_event_callbacks_rejects_duplicate_hook_signals():
    data = {
        "hook": [
            {"signal": "a=b", "callback": "one"},
            {"signal": "a=b", "callback": "two"},
        ]
    }
    with pytest.raises(EventConfigError, match="duplicate signal binding"):
        parse_event_callbacks(data)


def test_register_callback_rejects_duplicate_names():
    @register_callback("duplicate")
    async def first() -> None:
        pass

    with pytest.raises(ValueError, match="already registered"):

        @register_callback("duplicate")
        async def second() -> None:
            pass


def test_load_event_bindings_resolves_names(tmp_path: Path):
    @register_callback("start_one")
    async def start_one() -> None:
        pass

    @register_callback("hook_one")
    async def hook_one() -> None:
        pass

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - start_one
  hook:
    - signal: "patio.com/router=offboard"
      callback: hook_one
  end:
    - hook_one
""",
        encoding="utf-8",
    )

    start, end, hooks = load_event_bindings(path)

    assert start == (start_one,)
    assert end == (hook_one,)
    assert hooks == (("patio.com/router=offboard", hook_one),)


def test_load_event_bindings_rejects_unknown_callback(tmp_path: Path):
    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - missing
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown callback plugins"):
        load_event_bindings(path)


def test_parse_worker_instance_info_uses_env_topo_type(monkeypatch):
    monkeypatch.setenv("TOPO_TYPE", "sglang")

    worker_info, topo_type = parse_worker_instance_info(
        '{"data": {"port": 30000, "worker_type": "prefill"}}'
    )

    assert worker_info == {"port": 30000, "worker_type": "prefill"}
    assert topo_type == "sglang"


def test_parse_worker_instance_info_rejects_missing_topo_type(monkeypatch):
    monkeypatch.delenv("TOPO_TYPE", raising=False)

    with pytest.raises(ValueError, match="topo_type must be set"):
        parse_worker_instance_info('{"data": {}}')


@pytest.mark.asyncio
async def test_patio_application_runs_start_and_end(monkeypatch, tmp_path: Path):
    calls: list[str] = []

    class FakeRouterAdapter:
        async def add(self) -> bool:
            calls.append("add")
            return True

        async def remove(self) -> bool:
            calls.append("remove")
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            calls.append("ready")
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - engine_wait_ready
    - router_onboard
  end:
    - router_offboard
""",
        encoding="utf-8",
    )

    application = PatioApplication(
        worker_info={"port": 30000, "worker_type": "prefill"},
        topo_type="sglang",
        events_config=path,
        signal_source=MemorySignalSource(),
        poll_interval=0.01,
    )

    await application.start()
    await application.stop()

    assert calls == ["ready", "add", "remove"]


@pytest.mark.asyncio
async def test_sglang_engine_probe_reports_quiescent(monkeypatch):
    monkeypatch.setattr("patio.adapters.engine.sglang.envs.INFERENCE_ENGINE_ENDPOINT", "http://engine")
    monkeypatch.setattr("patio.adapters.engine.sglang.envs.METRIC_SCRAPE_PATH", "/metrics")
    monkeypatch.setattr("patio.adapters.engine.sglang.envs.TOPO_CONNECT_TIMEOUT", 1)
    monkeypatch.setattr("patio.adapters.engine.sglang.envs.TOPO_HEALTH_CHECK_TIMEOUT", 1)

    response = MagicMock()
    response.status_code = 200
    response.text = """
# HELP sglang:num_running_reqs Number of running requests
# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs 0
# HELP sglang:num_queue_reqs Number of queued requests
# TYPE sglang:num_queue_reqs gauge
sglang:num_queue_reqs 0
"""

    with patch("patio.adapters.engine.sglang.requests.get", return_value=response):
        probe = SGLangEngineProbe({})
        assert await probe.quiescent() is True


@pytest.mark.asyncio
async def test_event_executor_runs_start_and_end_in_order():
    calls: list[str] = []

    async def first() -> None:
        calls.append("first")

    async def second() -> None:
        calls.append("second")

    executor = EventExecutor(
        start_callbacks=(first, second),
        end_callbacks=(second, first),
        hook_bindings=(),
        signal_source=MemorySignalSource(),
    )

    await executor.run_start()
    await executor.run_end()

    assert calls == ["first", "second", "second", "first"]


@pytest.mark.asyncio
async def test_event_executor_runs_matching_hook_and_clears_signal():
    calls: list[str] = []
    source = MemorySignalSource({"patio.com/router=offboard"})

    async def callback() -> None:
        calls.append("offboard")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=source,
    )

    assert await executor.run_once() == 1
    assert calls == ["offboard"]
    assert await source.signals() == set()
    assert await executor.run_once() == 0


@pytest.mark.asyncio
async def test_event_executor_logs_hook_lifecycle(caplog):
    source = MemorySignalSource({"patio.com/router=offboard"})

    async def router_offboard() -> None:
        pass

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", router_offboard),),
        signal_source=source,
    )

    with caplog.at_level(logging.INFO, logger="patio.events.executor"):
        assert await executor.run_once() == 1

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        message == "hook signals detected: signals=patio.com/router=offboard"
        for message in messages
    )
    assert any(
        message == "hook callback started: signal=patio.com/router=offboard callback=router_offboard"
        for message in messages
    )
    assert any(
        message == "hook signal cleared: signal=patio.com/router=offboard"
        for message in messages
    )
    assert any(
        message.startswith(
            "hook callback completed: signal=patio.com/router=offboard callback=router_offboard duration="
        )
        for message in messages
    )


@pytest.mark.asyncio
async def test_event_executor_keeps_signal_when_callback_fails():
    source = MemorySignalSource({"patio.com/router=offboard"})

    async def callback() -> None:
        raise RuntimeError("boom")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=source,
    )

    assert await executor.run_once() == 0
    assert await source.signals() == {"patio.com/router=offboard"}


@pytest.mark.asyncio
async def test_event_executor_keeps_signal_when_clear_fails():
    class BrokenSource:
        async def signals(self) -> set[str]:
            return {"patio.com/router=offboard"}

        async def clear(self, signal: str) -> None:
            raise RuntimeError("clear failed")

    async def callback() -> None:
        pass

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=BrokenSource(),
    )

    assert await executor.run_once() == 0


def _configure_sglang_router_env(monkeypatch):
    monkeypatch.setattr("patio.adapters.router.sglang.envs.GROUP_NAME", "demo")
    monkeypatch.setattr("patio.adapters.router.sglang.envs.ROUTER_ROLE_NAME", "router")
    monkeypatch.setattr("patio.adapters.router.sglang.envs.ROUTER_PORT", "8000")
    monkeypatch.setenv("POD_IP", "1.2.3.4")


def _configure_vllm_router_env(monkeypatch):
    monkeypatch.setattr("patio.adapters.router.vllm.envs.GROUP_NAME", "demo")
    monkeypatch.setattr("patio.adapters.router.vllm.envs.ROUTER_ROLE_NAME", "router")
    monkeypatch.setattr("patio.adapters.router.vllm.envs.ROUTER_PORT", "8000")
    monkeypatch.setenv("POD_IP", "1.2.3.4")


def _get_response(payload: dict, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    return response


@pytest.mark.asyncio
async def test_sglang_router_remove_reconciles_uuid_worker_id(monkeypatch):
    _configure_sglang_router_env(monkeypatch)
    adapter = SGLangRouterAdapter({"port": "30000"})

    list_response = _get_response(
        {"workers": [{"id": "2f3a0c3e-0000-0000-0000-1b7d4c3a6e1f", "url": "http://1.2.3.4:30000"}]}
    )
    delete_response = _get_response({"status": "accepted"}, status_code=202)

    with (
        patch("patio.adapters.router.sglang.requests.get", return_value=list_response) as get,
        patch("patio.adapters.router.sglang.requests.delete", return_value=delete_response) as delete,
    ):
        assert await adapter.remove() is True

    get.assert_called_once()
    delete.assert_called_once()
    url = delete.call_args.args[0]
    assert url.endswith("/workers/2f3a0c3e-0000-0000-0000-1b7d4c3a6e1f")


@pytest.mark.asyncio
async def test_sglang_router_remove_reconciles_legacy_url_worker_id(monkeypatch):
    _configure_sglang_router_env(monkeypatch)
    adapter = SGLangRouterAdapter({"port": "30000"})

    list_response = _get_response(
        {"workers": [{"id": "http://1.2.3.4:30000", "url": "http://1.2.3.4:30000"}]}
    )
    delete_response = _get_response({"status": "accepted"}, status_code=202)

    with (
        patch("patio.adapters.router.sglang.requests.get", return_value=list_response),
        patch("patio.adapters.router.sglang.requests.delete", return_value=delete_response) as delete,
    ):
        assert await adapter.remove() is True

    url = delete.call_args.args[0]
    assert url.endswith("/workers/http%3A%2F%2F1.2.3.4%3A30000")


@pytest.mark.asyncio
async def test_sglang_router_remove_is_noop_when_not_registered(monkeypatch):
    _configure_sglang_router_env(monkeypatch)
    adapter = SGLangRouterAdapter({"port": "30000"})

    with (
        patch(
            "patio.adapters.router.sglang.requests.get",
            return_value=_get_response({"workers": []}),
        ) as get,
        patch("patio.adapters.router.sglang.requests.delete") as delete,
    ):
        assert await adapter.remove() is True

    get.assert_called_once()
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_sglang_router_remove_treats_deleted_router_dns_as_success(monkeypatch):
    _configure_sglang_router_env(monkeypatch)
    adapter = SGLangRouterAdapter({"port": "30000"})
    dns_error = socket.gaierror(-2, "Name does not resolve")
    urllib_error = requests.exceptions.ConnectionError(
        "HTTPConnectionPool(host='missing', port=8000)"
    )
    urllib_error.__cause__ = dns_error
    error = requests.exceptions.ConnectionError(urllib_error)
    error.__cause__ = urllib_error

    with (
        patch("patio.adapters.router.sglang.requests.get", side_effect=error),
        patch("patio.adapters.router.sglang.requests.delete") as delete,
    ):
        assert await adapter.remove() is True

    delete.assert_not_called()


@pytest.mark.asyncio
async def test_sglang_router_remove_treats_404_as_success(monkeypatch):
    _configure_sglang_router_env(monkeypatch)
    adapter = SGLangRouterAdapter({"port": "30000"})

    with (
        patch(
            "patio.adapters.router.sglang.requests.get",
            return_value=_get_response(
                {"workers": [{"id": "id-1", "url": "http://1.2.3.4:30000"}]}
            ),
        ),
        patch(
            "patio.adapters.router.sglang.requests.delete",
            return_value=_get_response({"error": "not found"}, status_code=404),
        ),
    ):
        assert await adapter.remove() is True


@pytest.mark.asyncio
async def test_sglang_router_remove_reports_failure(monkeypatch):
    _configure_sglang_router_env(monkeypatch)
    adapter = SGLangRouterAdapter({"port": "30000"})

    with (
        patch(
            "patio.adapters.router.sglang.requests.get",
            return_value=_get_response(
                {"workers": [{"id": "id-1", "url": "http://1.2.3.4:30000"}]}
            ),
        ),
        patch(
            "patio.adapters.router.sglang.requests.delete",
            return_value=_get_response({"error": "boom"}, status_code=500),
        ),
    ):
        assert await adapter.remove() is False


@pytest.mark.asyncio
async def test_vllm_router_add_sends_both_payload_fields(monkeypatch):
    _configure_vllm_router_env(monkeypatch)
    adapter = VLLMRouterAdapter({"type": "prefill", "instance": "1.2.3.4:8000"})

    with patch(
        "patio.adapters.router.vllm.requests.post",
        return_value=_get_response({"message": "added"}),
    ) as post:
        assert await adapter.add() is True

    assert post.call_args.kwargs["json"] == {
        "type": "prefill",
        "instance": "1.2.3.4:8000",
        "instances": ["1.2.3.4:8000"],
    }


@pytest.mark.asyncio
async def test_vllm_router_remove_sends_both_payload_fields(monkeypatch):
    _configure_vllm_router_env(monkeypatch)
    adapter = VLLMRouterAdapter({"type": "prefill", "instance": "1.2.3.4:8000"})

    with patch(
        "patio.adapters.router.vllm.requests.post",
        return_value=_get_response({"message": "removed"}),
    ) as post:
        assert await adapter.remove() is True

    assert post.call_args.args[0].endswith("/instances/remove")
    assert post.call_args.kwargs["json"] == {
        "type": "prefill",
        "instance": "1.2.3.4:8000",
        "instances": ["1.2.3.4:8000"],
    }


@pytest.mark.asyncio
async def test_vllm_router_remove_treats_missing_route_as_noop(monkeypatch):
    _configure_vllm_router_env(monkeypatch)
    adapter = VLLMRouterAdapter({"type": "prefill", "instance": "1.2.3.4:8000"})

    with patch(
        "patio.adapters.router.vllm.requests.post",
        return_value=_get_response({"detail": "not found"}, status_code=404),
    ):
        assert await adapter.remove() is True


@pytest.mark.asyncio
async def test_vllm_router_remove_reports_error_payload(monkeypatch):
    _configure_vllm_router_env(monkeypatch)
    adapter = VLLMRouterAdapter({"type": "prefill", "instance": "1.2.3.4:8000"})

    with patch(
        "patio.adapters.router.vllm.requests.post",
        return_value=_get_response({"error": "invalid instance type"}),
    ):
        assert await adapter.remove() is False


@pytest.mark.asyncio
async def test_vllm_router_remove_treats_deleted_router_dns_as_success(monkeypatch):
    _configure_vllm_router_env(monkeypatch)
    adapter = VLLMRouterAdapter({"type": "prefill", "instance": "1.2.3.4:8000"})
    dns_error = socket.gaierror(-2, "Name does not resolve")
    urllib_error = requests.exceptions.ConnectionError(
        "HTTPConnectionPool(host='missing', port=8000)"
    )
    urllib_error.__cause__ = dns_error
    error = requests.exceptions.ConnectionError(urllib_error)
    error.__cause__ = urllib_error

    with (
        patch("patio.adapters.router.vllm.requests.post", side_effect=error) as post,
    ):
        assert await adapter.remove() is True

    post.assert_called_once()


@pytest.mark.asyncio
async def test_event_executor_retries_transient_start_failure():
    attempts: list[int] = []

    async def flaky() -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("transient failure")

    executor = EventExecutor(
        start_callbacks=(flaky,),
        end_callbacks=(),
        hook_bindings=(),
        signal_source=MemorySignalSource(),
        retry_times=5,
        retry_interval=0,
    )

    await executor.run_start()

    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_event_executor_raises_after_retries_exhausted():
    attempts: list[int] = []

    async def always_fails() -> None:
        attempts.append(1)
        raise RuntimeError("permanent failure")

    executor = EventExecutor(
        start_callbacks=(always_fails,),
        end_callbacks=(),
        hook_bindings=(),
        signal_source=MemorySignalSource(),
        retry_times=2,
        retry_interval=0,
    )

    with pytest.raises(RuntimeError, match="permanent failure"):
        await executor.run_start()

    assert len(attempts) == 2


class FlakySignalSource(MemorySignalSource):
    def __init__(self):
        super().__init__()
        self.failures_remaining = 1

    async def signals(self) -> set[str]:
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("kubernetes api timeout")
        return await super().signals()


@pytest.mark.asyncio
async def test_run_forever_survives_signal_source_error():
    source = FlakySignalSource()
    await source.add("patio.com/router=offboard")
    calls: list[str] = []

    async def callback() -> None:
        calls.append("offboard")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=source,
        poll_interval=0.01,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(executor.run_forever(stop))

    for _ in range(100):
        await asyncio.sleep(0.01)
        if calls:
            break
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert calls == ["offboard"]


@pytest.mark.asyncio
async def test_hook_clear_skipped_when_signal_changes_during_callback():
    source = MemorySignalSource({"patio.com/router=offboard"})

    async def callback() -> None:
        # Simulate the controller replacing the label while we offboard.
        await source.clear("patio.com/router=offboard")
        await source.add("patio.com/router=onboard")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=source,
    )

    processed = await executor.run_once()

    assert processed == 1
    assert await source.signals() == {"patio.com/router=onboard"}


@pytest.mark.asyncio
async def test_patio_application_heartbeat_reregisters_router(monkeypatch, tmp_path: Path):
    add_calls: list[str] = []

    class FakeRouterAdapter:
        async def add(self) -> bool:
            add_calls.append("add")
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - engine_wait_ready
    - router_onboard
  end:
    - router_offboard
""",
        encoding="utf-8",
    )

    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
        signal_source=MemorySignalSource(),
        poll_interval=0.01,
        heartbeat_interval=0.01,
    )

    await application.start()
    await asyncio.sleep(0.05)
    await application.stop()

    assert len(add_calls) > 1


@pytest.mark.asyncio
async def test_patio_application_disables_heartbeat_without_start_onboard(monkeypatch, tmp_path: Path):
    add_calls: list[str] = []

    class FakeRouterAdapter:
        async def add(self) -> bool:
            add_calls.append("add")
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  hook:
    - signal: "patio.com/router=offboard"
      callback: router_offboard
""",
        encoding="utf-8",
    )

    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
        signal_source=MemorySignalSource(),
        poll_interval=0.01,
        heartbeat_interval=0.01,
    )

    await application.start()
    await asyncio.sleep(0.05)
    await application.stop()

    assert add_calls == []


def test_kubernetes_signal_source_request_timeout(monkeypatch):
    monkeypatch.setenv("LIFECYCLE_RI_NAME", "ri-0")
    monkeypatch.setenv("LIFECYCLE_RI_NAMESPACE", "default")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    monkeypatch.setenv("LIFECYCLE_API_TIMEOUT", "7")

    source = KubernetesSignalLabelSource()

    assert source.request_timeout.total == 7

    monkeypatch.setenv("LIFECYCLE_API_TIMEOUT", "3")
    source = KubernetesSignalLabelSource(request_timeout=3.5)
    assert source.request_timeout.total == 3.5


@pytest.mark.asyncio
async def test_patio_application_disables_heartbeat_after_offboard_signal(monkeypatch, tmp_path: Path):
    add_calls: list[str] = []

    class FakeRouterAdapter:
        async def add(self) -> bool:
            add_calls.append("add")
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - engine_wait_ready
    - router_onboard
  hook:
    - signal: "patio.com/router=offboard"
      callback: router_offboard
""",
        encoding="utf-8",
    )
    source = MemorySignalSource()

    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
        signal_source=source,
        poll_interval=0.01,
        heartbeat_interval=0.01,
    )

    await application.start()
    await source.add("patio.com/router=offboard")

    for _ in range(100):
        await asyncio.sleep(0.01)
        if "patio.com/router=offboard" not in await source.signals():
            break

    count_after_offboard = len(add_calls)
    await asyncio.sleep(0.05)
    await application.stop()

    assert len(add_calls) == count_after_offboard


@pytest.mark.asyncio
async def test_event_executor_notifies_hook_processed():
    notified: list[str] = []

    async def callback() -> None:
        pass

    async def on_processed(signal: str) -> None:
        notified.append(signal)

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=MemorySignalSource({"patio.com/router=offboard"}),
        on_hook_processed=on_processed,
    )

    assert await executor.run_once() == 1
    assert notified == ["patio.com/router=offboard"]


@pytest.mark.asyncio
async def test_event_executor_suppresses_same_signal_within_interval():
    class StaleFileSource:
        """Simulates a downwardAPI file that keeps showing a cleared signal."""

        def __init__(self) -> None:
            self.clears: list[str] = []

        async def signals(self) -> set[str]:
            return {"patio.com/router=offboard"}

        async def clear(self, signal: str) -> None:
            self.clears.append(signal)

    source = StaleFileSource()
    calls: list[str] = []

    async def callback() -> None:
        calls.append("offboard")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=source,
        signal_interval=1.0,
    )

    assert await executor.run_once() == 1
    assert calls == ["offboard"]
    # The stale file re-presents the same signal immediately: no second
    # execution and no duplicate clear within the interval.
    assert await executor.run_once() == 0
    assert calls == ["offboard"]
    assert source.clears == ["patio.com/router=offboard"]


@pytest.mark.asyncio
async def test_event_executor_reruns_signal_after_interval(monkeypatch):
    clock = {"now": 100.0}
    monkeypatch.setattr(
        "patio.events.executor.time",
        SimpleNamespace(perf_counter=lambda: clock["now"]),
    )
    source = MemorySignalSource()
    calls: list[str] = []

    async def callback() -> None:
        calls.append("offboard")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("patio.com/router=offboard", callback),),
        signal_source=source,
        signal_interval=1.0,
    )

    await source.add("patio.com/router=offboard")
    assert await executor.run_once() == 1
    # Still within the interval: the re-added signal is suppressed.
    clock["now"] += 0.5
    await source.add("patio.com/router=offboard")
    assert await executor.run_once() == 0
    # Interval elapsed: the signal may run again.
    clock["now"] += 0.6
    assert await executor.run_once() == 1
    assert calls == ["offboard", "offboard"]


@pytest.mark.asyncio
async def test_event_executor_interval_is_per_signal():
    class StaleFileSource:
        async def signals(self) -> set[str]:
            return {"patio.com/router=offboard", "patio.com/ready=onboard"}

        async def clear(self, signal: str) -> None:
            pass

    calls: list[str] = []

    async def offboard() -> None:
        calls.append("offboard")

    async def onboard() -> None:
        calls.append("onboard")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(
            ("patio.com/router=offboard", offboard),
            ("patio.com/ready=onboard", onboard),
        ),
        signal_source=StaleFileSource(),
        signal_interval=1.0,
    )

    assert await executor.run_once() == 2
    assert calls == ["offboard", "onboard"]
    # Both signals ran within the same interval and are now suppressed.
    assert await executor.run_once() == 0


def test_application_reads_signal_interval_from_env(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("EVENT_SIGNAL_INTERVAL", "2.5")

    class FakeRouterAdapter:
        async def add(self) -> bool:
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  hook:
    - signal: "patio.com/router=offboard"
      callback: router_offboard
""",
        encoding="utf-8",
    )

    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
    )

    assert application.signal_interval == 2.5
    assert application.executor.signal_interval == 2.5


def test_parse_downward_labels():
    content = '\n'.join(
        [
            'patio.com/router="offboard"',
            'rbg.workloads.x-k8s.io/role-name="engine"',
            'topology.kubernetes.io/zone="cn-shanghai-n"',
            'patio.com/escaped="say \\"hi\\" now"',
            'patio.com/backslash="path\\\\name"',
            '# not a label',
            'novalue=',
            '',
        ]
    )

    assert parse_downward_labels(content) == {
        "patio.com/router=offboard",
        "rbg.workloads.x-k8s.io/role-name=engine",
        "topology.kubernetes.io/zone=cn-shanghai-n",
        'patio.com/escaped=say "hi" now',
        "patio.com/backslash=path\\name",
    }


@pytest.mark.asyncio
async def test_file_signal_source_reads_labels_file(tmp_path: Path):
    path = tmp_path / "pod-labels"
    path.write_text('patio.com/router="offboard"\n', encoding="utf-8")

    class FakeClearSource:
        def __init__(self):
            self.cleared: list[str] = []

        async def clear(self, signal: str) -> None:
            self.cleared.append(signal)

    clear_source = FakeClearSource()
    source = FileSignalSource(labels_path=path, clear_source=clear_source)

    assert await source.signals() == {"patio.com/router=offboard"}

    await source.clear("patio.com/router=offboard")
    assert clear_source.cleared == ["patio.com/router=offboard"]


@pytest.mark.asyncio
async def test_file_signal_source_missing_file_is_empty(tmp_path: Path):
    class FakeClearSource:
        async def clear(self, signal: str) -> None:
            raise AssertionError("should not be called")

    source = FileSignalSource(
        labels_path=tmp_path / "missing", clear_source=FakeClearSource()
    )

    assert await source.signals() == set()


def test_application_selects_file_signal_source(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("LIFECYCLE_RI_NAME", "ri-0")
    monkeypatch.setenv("LIFECYCLE_RI_NAMESPACE", "default")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    monkeypatch.setenv("LIFECYCLE_SIGNAL_SOURCE", "file")
    monkeypatch.setenv("LIFECYCLE_SIGNAL_FILE", str(tmp_path / "pod-labels"))

    class FakeRouterAdapter:
        async def add(self) -> bool:
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )

    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  hook:
    - signal: "patio.com/router=offboard"
      callback: router_offboard
""",
        encoding="utf-8",
    )

    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
    )

    assert isinstance(application.signal_source, FileSignalSource)
    assert application.signal_source.labels_path == tmp_path / "pod-labels"


def test_application_rejects_unknown_signal_source(monkeypatch):
    monkeypatch.setenv("LIFECYCLE_RI_NAME", "ri-0")
    monkeypatch.setenv("LIFECYCLE_RI_NAMESPACE", "default")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    monkeypatch.setenv("LIFECYCLE_SIGNAL_SOURCE", "queue")

    with pytest.raises(ValueError, match="unsupported LIFECYCLE_SIGNAL_SOURCE"):
        KubernetesSignalLabelSource  # referenced for linters
        from patio.application import PatioApplication as _PatioApplication
        _PatioApplication._select_signal_source(())


@pytest.mark.asyncio
async def test_event_executor_continues_after_failed_hook():
    source = MemorySignalSource({"x=1", "y=1"})
    calls: list[str] = []

    async def failing() -> None:
        calls.append("x")
        raise RuntimeError("boom")

    async def working() -> None:
        calls.append("y")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("x=1", failing), ("y=1", working)),
        signal_source=source,
        signal_interval=0,
    )

    assert await executor.run_once() == 1
    assert calls == ["x", "y"]
    assert await source.signals() == {"x=1"}


@pytest.mark.asyncio
async def test_event_executor_treats_changed_signal_as_processed():
    class ChangedSignalSource:
        async def signals(self) -> set[str]:
            return {"x=1"}

        async def clear(self, signal: str) -> None:
            raise SignalChangedError("signal changed")

    calls: list[str] = []

    async def callback() -> None:
        calls.append("x")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(),
        hook_bindings=(("x=1", callback),),
        signal_source=ChangedSignalSource(),
        signal_interval=0,
    )

    assert await executor.run_once() == 1
    assert calls == ["x"]


@pytest.mark.asyncio
async def test_event_executor_uses_separate_end_retry_budget():
    attempts: list[int] = []

    async def always_fails() -> None:
        attempts.append(1)
        raise RuntimeError("shutdown failure")

    executor = EventExecutor(
        start_callbacks=(),
        end_callbacks=(always_fails,),
        hook_bindings=(),
        signal_source=MemorySignalSource(),
        retry_times=20,
        retry_interval=0,
        end_retry_times=2,
        end_retry_interval=0,
    )

    with pytest.raises(RuntimeError, match="shutdown failure"):
        await executor.run_end()

    assert len(attempts) == 2


def test_escape_json_pointer():
    assert _escape_json_pointer("patio.com/router") == "patio.com~1router"
    assert _escape_json_pointer("a~b/c") == "a~0b~1c"


class _FakeAsyncResponse:
    def __init__(self, status: int):
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *exc_info):
        return False


def _patch_signal_label_client(monkeypatch, response_status: int):
    session = MagicMock()
    session.patch = MagicMock(return_value=_AsyncContext(_FakeAsyncResponse(response_status)))
    monkeypatch.setattr(
        "patio.lifecycle.signal_label.aiohttp.ClientSession",
        MagicMock(return_value=_AsyncContext(session)),
    )
    return session


def _signal_source(monkeypatch, tmp_path: Path, **kwargs):
    monkeypatch.setenv("LIFECYCLE_API_INSECURE", "true")
    token = tmp_path / "token"
    token.write_text("test-token", encoding="utf-8")
    return KubernetesSignalLabelSource(
        name="ri-0",
        namespace="default",
        api_server="https://kubernetes.default:443",
        token_path=str(token),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_signal_clear_uses_conditional_json_patch(monkeypatch, tmp_path: Path):
    source = _signal_source(monkeypatch, tmp_path)
    session = _patch_signal_label_client(monkeypatch, 200)

    await source.clear("patio.com/router=offboard")

    patch = session.patch.call_args.kwargs["json"]
    headers = session.patch.call_args.kwargs["headers"]
    assert headers["Content-Type"] == "application/json-patch+json"
    assert patch == [
        {"op": "test", "path": "/metadata/labels/patio.com~1router", "value": "offboard"},
        {"op": "remove", "path": "/metadata/labels/patio.com~1router"},
    ]


@pytest.mark.asyncio
async def test_signal_clear_reports_changed_value(monkeypatch, tmp_path: Path):
    source = _signal_source(monkeypatch, tmp_path)
    _patch_signal_label_client(monkeypatch, 422)

    with pytest.raises(SignalChangedError, match="signal changed before clear"):
        await source.clear("patio.com/router=offboard")


def test_kubernetes_ssl_context_fails_closed_without_ca(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("patio.lifecycle.signal_label.envs.is_lifecycle_api_insecure", lambda: False)
    source = _signal_source(monkeypatch, tmp_path, ca_path=str(tmp_path / "missing.crt"))

    with pytest.raises(FileNotFoundError, match="Kubernetes CA certificate not found"):
        source._ssl_context()


def test_kubernetes_ssl_context_allows_explicit_insecure(monkeypatch, tmp_path: Path):
    source = _signal_source(monkeypatch, tmp_path)

    assert source._ssl_context() is False


@pytest.mark.asyncio
async def test_vllm_engine_probe_checks_once(monkeypatch):
    monkeypatch.setattr("patio.adapters.engine.vllm.envs.TOPO_CONNECT_TIMEOUT", 1)
    monkeypatch.setattr("patio.adapters.engine.vllm.envs.TOPO_HEALTH_CHECK_TIMEOUT", 1)
    response = MagicMock()
    response.status_code = 200
    client = MagicMock()

    with patch("patio.adapters.engine.vllm.requests.get", return_value=response) as get:
        probe = VLLMEngineProbe(client, {"port": 8000})
        assert await probe.ready() is True

    get.assert_called_once()
    client.wait_engine_ready.assert_not_called()


@pytest.mark.asyncio
async def test_application_preserves_external_callbacks(monkeypatch, tmp_path: Path):
    calls: list[str] = []

    @register_callback("custom_start")
    async def custom_start() -> None:
        calls.append("custom")

    class FakeRouterAdapter:
        async def add(self) -> bool:
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )
    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - custom_start
""",
        encoding="utf-8",
    )

    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
        signal_source=MemorySignalSource(),
    )
    await application.start()
    await application.stop()

    assert calls == ["custom"]
    assert "custom_start" in callback_plugins


@pytest.mark.asyncio
async def test_offboard_serializes_membership_and_suspends_heartbeat(
    monkeypatch, tmp_path: Path
):
    events: list[str] = []
    remove_done = asyncio.Event()

    class FakeRouterAdapter:
        async def add(self) -> bool:
            events.append("add")
            await asyncio.sleep(0.01)
            return True

        async def remove(self) -> bool:
            events.append("remove")
            assert application._heartbeat_enabled is False
            remove_done.set()
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )
    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - engine_wait_ready
    - router_onboard
  hook:
    - signal: "patio.com/router=offboard"
      callback: router_offboard
""",
        encoding="utf-8",
    )
    source = MemorySignalSource()
    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
        signal_source=source,
        poll_interval=0.01,
        heartbeat_interval=0.01,
        signal_interval=0,
    )

    await application.start()
    await source.add("patio.com/router=offboard")

    for _ in range(200):
        await asyncio.sleep(0.01)
        if remove_done.is_set():
            break
    await asyncio.sleep(0.05)
    await application.stop()

    assert events[-1] == "remove"


@pytest.mark.asyncio
async def test_application_resumes_heartbeat_after_onboard_signal(
    monkeypatch, tmp_path: Path
):
    add_calls: list[None] = []

    class FakeRouterAdapter:
        async def add(self) -> bool:
            add_calls.append(None)
            return True

        async def remove(self) -> bool:
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )
    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - engine_wait_ready
    - router_onboard
  hook:
    - signal: "patio.com/router=offboard"
      callback: router_offboard
    - signal: "patio.com/router=onboard"
      callback: router_onboard
""",
        encoding="utf-8",
    )
    source = MemorySignalSource()
    application = PatioApplication(
        worker_info={"port": 30000},
        topo_type="sglang",
        events_config=path,
        signal_source=source,
        poll_interval=0.01,
        heartbeat_interval=0.01,
        signal_interval=0,
    )

    await application.start()
    await source.add("patio.com/router=offboard")
    for _ in range(200):
        await asyncio.sleep(0.01)
        if await source.signals() == set():
            break

    assert application._heartbeat_enabled is False
    count_after_offboard = len(add_calls)

    await source.add("patio.com/router=onboard")
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(add_calls) > count_after_offboard:
            break
    assert application._heartbeat_enabled is True
    await application.stop()

    assert len(add_calls) > count_after_offboard


def test_application_rejects_explicit_kubernetes_source_without_cluster(monkeypatch):
    monkeypatch.delenv("LIFECYCLE_RI_NAME", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setenv("LIFECYCLE_SIGNAL_SOURCE", "kubernetes")

    with pytest.raises(ValueError, match="requires LIFECYCLE_RI_NAME"):
        PatioApplication._select_signal_source(())


def test_application_rejects_file_source_without_clear_config(monkeypatch):
    monkeypatch.delenv("LIFECYCLE_RI_NAME", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setenv("LIFECYCLE_SIGNAL_SOURCE", "file")

    with pytest.raises(ValueError, match="required"):
        PatioApplication._select_signal_source(())


def test_event_application_uses_fastapi_lifespan(monkeypatch, tmp_path: Path):
    from fastapi.testclient import TestClient

    from patio import app as app_module

    calls: list[str] = []

    class FakeRouterAdapter:
        async def add(self) -> bool:
            calls.append("add")
            return True

        async def remove(self) -> bool:
            calls.append("remove")
            return True

    class FakeEngineProbe:
        async def ready(self) -> bool:
            calls.append("ready")
            return True

    monkeypatch.setattr(
        "patio.application.create_router_adapter", lambda topo_type, worker_info: FakeRouterAdapter()
    )
    monkeypatch.setattr(
        "patio.application.create_engine_probe", lambda topo_type, worker_info: FakeEngineProbe()
    )
    path = tmp_path / "events.yaml"
    path.write_text(
        """
event_callbacks:
  start:
    - engine_wait_ready
    - router_onboard
  end:
    - router_offboard
""",
        encoding="utf-8",
    )
    original_lifespan = app_module.app.router.lifespan_context

    try:
        app_module._configure_event_application(
            '{"topo_type": "sglang", "data": {"port": 30000}}',
            str(path),
            logging.getLogger(__name__),
        )
        with TestClient(app_module.app) as client:
            assert client.get("/health").status_code < 500
            assert calls[:2] == ["ready", "add"]
            assert "remove" not in calls
        assert calls[-1] == "remove"
    finally:
        app_module.app.router.lifespan_context = original_lifespan
        app_module.patio_application = None

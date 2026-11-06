# Inference Engine Runtime (Patio)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

> A Python-based sidecar runtime for AI inference engines on Kubernetes. Patio provides a unified interface between workload controllers (such as [RoleBasedGroup](https://github.com/sgl-project/rbg)) and inference engines like SGLang and vLLM.

## Overview

Patio is a lightweight FastAPI server that runs alongside inference engine containers and provides:

- **LoRA Adapter Management** — Dynamically load and unload LoRA adapters at runtime without restarting the engine
- **Unified Prometheus Metrics** — Scrape, normalize, and re-expose inference engine metrics with a `patio:` prefix
- **Distributed Topology Management** — Register workers with a central router, heartbeat-based recovery, and graceful shutdown
- **Health Check & Readiness** — Standard Kubernetes liveness and readiness probes

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Pod                                                         │
│  ┌──────────────────┐    ┌─────────────────────────────────┐│
│  │  Inference Engine │    │  Patio Sidecar (port 9091)      ││
│  │  (SGLang/vLLM)   │◄──►│                                 ││
│  │                  │    │  - LoRA API                       ││
│  │  :8000           │    │  - Metrics (/metrics)            ││
│  │                  │    │  - Topology Client               ││
│  │                  │    │  - Health (/health)              ││
│  └──────────────────┘    └─────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

Patio acts as a proxy and management layer, allowing RBG to control inference engines through a standardized HTTP API.

## Features

### LoRA Adapter Management

Dynamically manage LoRA adapters without engine restarts:

```bash
# Load a LoRA adapter
curl -X POST http://localhost:9091/load_lora_adapter \
  -H "Content-Type: application/json" \
  -d '{"lora_name": "my-adapter", "lora_path": "/models/my-adapter"}'

# Unload a LoRA adapter
curl -X POST http://localhost:9091/unload_lora_adapter \
  -H "Content-Type: application/json" \
  -d '{"lora_name": "my-adapter"}'
```

Patio proxies these requests to the underlying engine (SGLang or vLLM) using their native APIs.

### Unified Prometheus Metrics

Patio scrapes the inference engine's `/metrics` endpoint and normalizes metric names:

| Engine Metric | Patio Metric |
|--------------|--------------|
| `sglang:num_running_reqs` | `patio:num_requests_running` |
| `vllm:num_requests_running` | `patio:num_requests_running` |
| `sglang:num_prompt_tokens_total` | `patio:input_tokens_total` |

Access metrics at `http://localhost:9091/metrics` in Prometheus text format.

### Distributed Topology Management

For multi-node inference deployments, Patio manages worker registration and discovery:

- **Worker Registration** — Automatically registers with a central router on startup
- **Heartbeat** — Periodic heartbeats ensure automatic recovery if the router restarts
- **Graceful Shutdown** — Unregisters from the router on SIGTERM/SIGINT

Configure via environment variables:

```yaml
env:
  - name: TOPO_TYPE
    value: "SGLang"
  - name: ROUTER_ROLE_NAME
    value: "router"
  - name: ROUTER_PORT
    value: "8000"
```

For vLLM proxy registration, set `TOPO_TYPE` to `vLLM` and point
`ROUTER_ROLE_NAME` / `ROUTER_PORT` at the proxy service. Patio registers workers
with the proxy using `/instances/add`.

For MindIE Motor Coordinator (standalone deployment) registration, set
`TOPO_TYPE` to `Motor`. The Coordinator management endpoint derives from the
same `ROUTER_ROLE_NAME` / `ROUTER_PORT` DNS shape, and Patio registers /
deregisters the engine instance through `/instances/refresh` using the
Coordinator's External Deployer event schema (`ExternalInsEventMsg`):

```yaml
env:
  - name: TOPO_TYPE
    value: "Motor"
  - name: ROUTER_ROLE_NAME
    value: "router"          # the Motor Coordinator management service
  - name: ROUTER_PORT
    value: "1026"            # Motor management API port
  - name: MOTOR_MODEL_NAME   # optional; else Motor probes the engine /v1/models
    value: "Qwen2.5-7B"
  - name: MOTOR_MGMT_API_KEY_FILE  # optional; path to the management API key file
    value: "/etc/patio/mgmt-key"
  - name: MOTOR_ENGINE_TYPE        # optional; only vllm is supported by the schema
    value: "vllm"
  - name: MOTOR_DISPATCH_CAPABILITIES  # optional; one plan, empty = Motor's vLLM HANDOFF default
    value: "prefill_handoff_decode"
```

The worker's P/D role is taken from `type`/`worker_type` in the instance info
(`prefill` or `decode` — the schema has no encode/union role), and the endpoint
address from the `instance` field, `POD_IP`, or the RBG headless DNS. Patio sends
just the instance id, role and endpoint address; Motor derives the job name, DP
size and multi-endpoint routing from that. On every heartbeat the engine
`/health` is probed: while the endpoint is healthy Patio keeps the instance
registered (event=add); once the endpoint fails, Patio sends event=del so the
Coordinator stops routing traffic to the dead (P or D) instance — no orphaned
registration is left behind during multi-DP operation.

Deregistration on `SIGTERM`/`SIGINT` is bounded by `MOTOR_UNREGISTER_DEADLINE`
(default 10s) so that a terminating pod is never held up by an unreachable
Coordinator. See [Motor Registration](doc/motor_registration.md) for the full
contract, lifecycle and limitations (notably: Motor never expires an instance on
its own, so the explicit `del` is the only withdrawal path).

## Quick Start

### Prerequisites

- Python 3.10+
- Kubernetes cluster with [RoleBasedGroup](https://github.com/sgl-project/rbg) installed
- Inference engine (SGLang or vLLM) running in the same pod

### Installation

Install the package from source (project uses `pyproject.toml` for packaging):

```bash
pip install .
```

For development, install in editable mode with test dependencies:

```bash
pip install -e ".[dev]"
```

### Running Locally

Start the Patio server:

```bash
patio --host 127.0.0.1 --port 9091
```

Or via the module entry:

```bash
python -m patio.app --host 127.0.0.1 --port 9091
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Host to listen on |
| `--port` | `9091` | Port to listen on |
| `--log-level` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `--enable-fastapi-docs` | `false` | Enable FastAPI documentation endpoints |
| `--scrape-engine-metrics` | `true` | Enable scraping of engine metrics |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `INFERENCE_ENGINE` | Inference engine type (`sglang` or `vllm`) | `sglang` |
| `INFERENCE_ENGINE_VERSION` | Engine version | `v0.5.3` |
| `INFERENCE_ENGINE_ENDPOINT` | Engine endpoint URL | `http://localhost:8000` |
| `TOPO_TYPE` | Topology type (`SGLang`, `vLLM`, `Motor`, or `None`) | `None` |
| `ROUTER_ROLE_NAME` | RBG role name for the topology router, proxy, or Motor Coordinator | `None` |
| `ROUTER_PORT` | Port for the topology router, proxy, or Motor Coordinator | `None` |
| `TOPO_CONNECT_TIMEOUT` | Connection timeout for topology HTTP requests (seconds) | `3` |
| `TOPO_HEALTH_CHECK_TIMEOUT` | Read timeout for engine health checks (seconds) | `30` |
| `TOPO_REGISTER_TIMEOUT` | Read timeout for topology register and unregister requests (seconds) | `10` |
| `GROUP_NAME` | RBG group name | `None` |
| `ROLE_NAME` | RBG role name | `None` |
| `ROLE_INDEX` | RBG role index | `None` |
| `HEARTBEAT_INTERVAL` | Topology heartbeat interval (seconds) | `30` |
| `MOTOR_MODEL_NAME` | Model name for Motor registration; if unset, Motor probes the engine `/v1/models` | `None` |
| `MOTOR_MGMT_API_KEY_FILE` | Path to the Motor management API key file (optional) | `None` |
| `MOTOR_ENGINE_TYPE` | Engine family advertised to Motor (the External Deployer schema accepts `vllm` only) | `vllm` |
| `MOTOR_DISPATCH_CAPABILITIES` | The single P/D dispatch plan (`prefill_handoff_decode` / `concurrent_engine_sync`); empty means Motor's vLLM HANDOFF default | `prefill_handoff_decode` |
| `MOTOR_UNREGISTER_DEADLINE` | Total budget for deregistration at shutdown (seconds) | `10` |
| `MOTOR_UNREGISTER_INTERVAL` | Interval between shutdown deregistration attempts (seconds) | `1` |
| `MOTOR_UNREGISTER_TIMEOUT` | Per-request timeout during shutdown deregistration (seconds) | `3` |

## Usage with RoleBasedGroup

Patio is typically deployed as a sidecar container within a RoleBasedGroup role. Use the `ClusterEngineRuntimeProfile` CRD (defined in RBG) to inject Patio automatically:

```yaml
apiVersion: workloads.x-k8s.io/v1alpha2
kind: ClusterEngineRuntimeProfile
metadata:
  name: patio-runtime
spec:
  containers:
    - name: patio
      image: rolebasedgroup/rbgs-patio-runtime:latest
      ports:
        - containerPort: 9091
          name: patio
      env:
        - name: INFERENCE_ENGINE
          value: "sglang"
        - name: INFERENCE_ENGINE_ENDPOINT
          value: "http://localhost:8000"
        - name: TOPO_TYPE
          value: "SGLang"
        - name: ROUTER_ROLE_NAME
          value: "router"
        - name: ROUTER_PORT
          value: "8000"
      resources:
        requests:
          cpu: "100m"
          memory: "128Mi"
---
apiVersion: workloads.x-k8s.io/v1alpha2
kind: RoleBasedGroup
metadata:
  name: my-inference
spec:
  roles:
    - name: inference
      replicas: 2
      engineRuntimes:
        - profileName: patio-runtime
      standalonePattern:
        template:
          spec:
            containers:
              - name: sglang
                image: lmsysorg/sglang:latest
                ports:
                  - containerPort: 8000
```

## API Reference

### Server Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Server status |
| `GET` | `/health` | Liveness probe |
| `GET` | `/metrics` | Prometheus metrics |

### LoRA Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/load_lora_adapter` | Load a LoRA adapter |
| `POST` | `/unload_lora_adapter` | Unload a LoRA adapter |

### Request Models

**LoadLoraAdapterRequest:**
```json
{
  "lora_name": "string",
  "lora_path": "string"
}
```

**UnLoadLoraAdapterRequest:**
```json
{
  "lora_name": "string"
}
```

## Project Structure

```
inference-engine-runtime/
├── pyproject.toml           # Packaging and project configuration
├── src/patio/               # Source package
│   ├── app.py               # Main entry point (FastAPI + uvicorn)
│   ├── config.py            # Configuration constants
│   ├── envs.py              # Environment variable parsing
│   ├── logger.py            # Logging configuration
│   ├── api/                 # HTTP API layer
│   │   ├── server_router.py # Server endpoints
│   │   ├── lora_router.py   # LoRA endpoints
│   │   └── protocol.py      # Request/response models
│   ├── engine/              # Inference engine abstraction
│   │   ├── base.py          # Base engine interface
│   │   ├── sglang_engine.py # SGLang implementation
│   │   └── vllm_engine.py   # vLLM implementation
│   ├── metrics/             # Prometheus metrics
│   │   ├── metrics.py       # Built-in metrics
│   │   ├── engine_collector.py # Engine metrics scraper
│   │   └── standard_rules.py # Metric normalization
│   └── topo/                # Topology management
│       ├── factory.py       # Client/server factories
│       ├── client/          # Topology clients
│       └── server/          # Topology servers
├── tests/                   # Unit and E2E tests
└── doc/                     # Additional documentation
```

## Development

### Running Tests

```bash
# Run all unit tests (requires pip install -e ".[dev]" first)
pytest

# Run specific test file
pytest tests/mock_tests/test_sglang_engine.py -v
```

### Building Docker Image

```bash
docker build -t inference-engine-runtime:latest .
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Acknowledgments

Patio was originally developed as part of the [RoleBasedGroup](https://github.com/sgl-project/rbg) project and has been extracted into a standalone project for independent development and deployment.

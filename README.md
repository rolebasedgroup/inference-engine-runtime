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
  - name: SGL_ROUTER_ROLE_NAME
    value: "router"
  - name: SGL_ROUTER_PORT
    value: "8000"
```

## Quick Start

### Prerequisites

- Python 3.8+
- Kubernetes cluster with [RoleBasedGroup](https://github.com/sgl-project/rbg) installed
- Inference engine (SGLang or vLLM) running in the same pod

### Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

For development, also install test dependencies:

```bash
pip install -r requirements-dev.txt
```

### Running Locally

Start the Patio server:

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
| `TOPO_TYPE` | Topology type (`SGLang` or `None`) | `None` |
| `GROUP_NAME` | RBG group name | `None` |
| `ROLE_NAME` | RBG role name | `None` |
| `ROLE_INDEX` | RBG role index | `None` |
| `HEARTBEAT_INTERVAL` | Topology heartbeat interval (seconds) | `30` |

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
├── app.py                  # Main entry point (FastAPI + uvicorn)
├── config.py               # Configuration constants
├── envs.py                 # Environment variable parsing
├── logger.py               # Logging configuration
├── api/                    # HTTP API layer
│   ├── server_router.py    # Server endpoints
│   ├── lora_router.py      # LoRA endpoints
│   └── protocol.py         # Request/response models
├── engine/                 # Inference engine abstraction
│   ├── base.py             # Base engine interface
│   ├── sglang_engine.py    # SGLang implementation
│   └── vllm_engine.py      # vLLM implementation
├── metrics/                # Prometheus metrics
│   ├── metrics.py          # Built-in metrics
│   ├── engine_collector.py # Engine metrics scraper
│   └── standard_rules.py   # Metric normalization
├── topo/                   # Topology management
│   ├── factory.py          # Client/server factories
│   ├── client/             # Topology clients
│   └── server/             # Topology servers
├── tests/                  # Unit and E2E tests
└── doc/                    # Additional documentation
```

## Development

### Running Tests

```bash
# Run all unit tests
python -m pytest tests/mock_tests -v

# Run specific test file
python -m pytest tests/mock_tests/test_sglang_engine.py -v

# Run with coverage
python -m pytest tests/mock_tests --cov=patio --cov-report=term-missing
```

### Building Docker Image

```bash
docker build -t inference-engine-runtime:latest .
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Acknowledgments

Patio was originally developed as part of the [RoleBasedGroup](https://github.com/sgl-project/rbg) project and has been extracted into a standalone project for independent development and deployment.

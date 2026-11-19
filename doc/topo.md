# Topology clients

Patio registers the engine it runs next to with whatever routing/control-plane
component sits in front of it. The component is selected with `TOPO_TYPE`; each
`GroupTopoClient` implementation owns its own wire contract, but all of them are
configured through the same `--instance-info` envelope and share the heartbeat /
shutdown lifecycle.

| `TOPO_TYPE` | Client | Router / control plane |
| --- | --- | --- |
| `SGLang` | `SGLangGroupTopoClient` | SGLang router (`/workers`) |
| `vLLM` | `VLLMProxyTopoClient` | vLLM proxy (`/instances/add`) |
| `Motor` | `MotorTopoClient` | MindIE Motor Coordinator (`/instances/refresh`) |
| unset / `None` | no client | standalone Patio |

## `--instance-info`

The instance info is a JSON envelope passed on the command line:

```bash
patio --instance-info '{"topo_type": "Motor", "data": {"type": "prefill", "port": 8000}}'
```

```json
{
  "topo_type": "Motor",
  "data": {
    "type": "prefill",
    "port": 8000
  }
}
```

* `topo_type` selects the client. It may be omitted when `TOPO_TYPE` is set; the
  env var then fills it in.
* `data` is the per-topology worker record. It must be present; its fields are
  documented per topology below.

Common fields:

| Field | Meaning |
| --- | --- |
| `type` / `worker_type` | P/D role: `prefill` or `decode`. Required by the vLLM proxy and Motor clients. |
| `port` | Engine API port (defaults: 8000 for vLLM/Motor, 30000 for SGLang). |
| `instance` | Explicit `host:port` of the engine, overriding `POD_IP` / RBG DNS discovery. |

`POD_IP`, `RBG_GROUP_NAME` / `RBG_ROLE_NAME` / `RBG_ROLE_INDEX`, `ROUTER_ROLE_NAME`
and `ROUTER_PORT` come from the environment, as do the per-topology timeouts and
the `MOTOR_*` knobs.

## Per-topology examples

### SGLang router

```json
{"topo_type": "SGLang", "data": {"type": "prefill", "port": 30000}}
```

### vLLM proxy

```json
{"topo_type": "vLLM", "data": {"type": "decode", "port": 8000}}
```

### MindIE Motor Coordinator

One endpoint (the default; correct for internal-LB topologies):

```json
{"topo_type": "Motor", "data": {"type": "prefill", "port": 8000}}
```

An external-LB DP group is opt-in, either explicitly:

```json
{"topo_type": "Motor",
 "data": {"type": "prefill",
          "endpoints": ["10.0.0.1:7100", "10.0.0.1:7102",
                        "10.0.0.1:7104", "10.0.0.1:7106"]}}
```

or derived with the explicit `derive_endpoints` switch (see
[Motor Registration](motor_registration.md#endpoint-groups-and---instance-info)
for the full field table, precedence and migration notes):

```json
{"topo_type": "Motor",
 "data": {"type": "decode", "derive_endpoints": true,
          "parallel_config": {"dp_size": 16, "tp_size": 1},
          "device_num": 16, "base_port": 7100, "port_step": 2}}
```

## Lifecycle

All clients share the app loop:

1. `wait_engine_ready(worker_info)` — poll the engine's `/health` until ready.
2. `register(url, worker_info)` — announce the engine; called again on every
   heartbeat, where it doubles as the failure detector.
3. `unregister()` — withdraw the engine on `SIGTERM`/`SIGINT`, bounded by the
   client's shutdown budget.

The Motor client documents its wire contract, endpoint groups and failure modes
in [Motor Registration](motor_registration.md).

# Motor Registration

How Patio registers an inference engine instance with MindIE-Motor (standalone
Coordinator), and how it deregisters again.

This document is the design record for `TOPO_TYPE=Motor`: it states which
repository owns which behaviour, why the Motor management API is used instead of
a Patio-specific endpoint, the exact wire contract, the lifecycle, and what is
*not* covered.

## Scope

Patio runs as a sidecar next to a native vLLM (or SGLang) Prefill/Decode engine.
The engine itself knows nothing about the routing layer, so the sidecar is
responsible for the full instance lifecycle in the router:

| Phase | Patio call | Wire effect |
| --- | --- | --- |
| Engine ready | `MotorTopoClient.register(topo_type, worker_info)` | `POST /instances/refresh` with `event=add` |
| Periodic heartbeat | same `register(...)` call | `POST /instances/refresh` with `event=add` (idempotent no-op) |
| Engine unhealthy | `register(...)` returns `False` after deregistering | `POST /instances/refresh` with `event=del` |
| SIGTERM / SIGINT | `MotorTopoClient.unregister()` | `POST /instances/refresh` with `event=del` |

The functional target is parity with the `patio` ↔ `vllm-router` integration:
whichever routing component sits between Patio and the engines, Patio must be
able to *announce* a local engine and *withdraw* it, with the same client-side
lifecycle (readiness gate, heartbeat, failure handling, bounded shutdown).

## Which repository owns what

**No change to Motor's protocol was needed, and no Patio-specific endpoint was
added to Motor.** The management API that Motor already exposes for its own
standalone CLI (`python3 -m motor.coordinator.register`) and for the K8s
Controller is exactly the registration surface a sidecar needs:

| Concern | Owner | Where |
| --- | --- | --- |
| Registration / deregistration protocol | Motor | `POST /instances/refresh` (`motor/coordinator/api_server/management_server.py`) |
| Pool & routing state | Motor | `motor/coordinator/domain/instance_manager.py` |
| Cross-request role validation (all-or-nothing) | Motor | `_validate_refresh_instances` (see [below](#motor-side-changes)) |
| Instance identity, payload shape, retries, shutdown budget | Patio | `src/patio/topo/client/motor_topo_client.py` |
| Sidecar-specific documentation | both | Motor `docs/zh/user_guide/deployment/standalone.md` 附录 F; this document |

So: **the routing-side logic lives in `inference-engine-runtime` (Patio); the
service side needed only a small robustness fix plus documentation in
`MindIE-Motor`.** `vllm-router` is not modified — it remains the reference for
the client-side lifecycle.

### Why not the `/instances/add` vLLM-proxy shape?

The `TOPO_TYPE=vLLM` client posts to `/instances/add`, which is a
`vllm-router`-specific proxy shape. Motor never had that route (its full
management API is `/startup`, `/liveness`, `/readiness`, `GET /instances`,
`POST /instances/refresh`, `POST /precision/alarm_cleared`, `GET /`), and the
route does not exist anywhere in `vllm-router`'s history either — in
`vllm-router` the admin surface is `POST /add_worker?url=` /
`POST /remove_worker?url=`.

Adding a compatibility route to Motor was rejected:

* it would duplicate a protocol Motor already models with a typed schema
  (`InsEventMsg`);
* a vLLM-proxy-shaped route carries less information than Motor's own model
  (no role, no dispatch capabilities, no multi-endpoint groups), so the
  Coordinator would have to guess what the CLI can state exactly;
* it would leave two ways to reach the same pool, with no single source of
  truth for validation.

Instead the Patio client speaks Motor's native protocol and keeps the
*vllm-router client's lifecycle* (readiness gate → register → heartbeat →
failure/shutdown deregistration).

## Wire contract

`POST {mgmt_endpoint}/instances/refresh` with header
`X-Motor-Management-Key: <key>` when Motor's management API key is enabled.

```json
{
  "event": "add",
  "instances": [
    {
      "job_name": "Qwen3-8B-prefill-10.10.0.11-8000",
      "model_name": "Qwen3-8B",
      "engine_type": "vllm",
      "dispatch_capabilities": ["prefill_handoff_decode"],
      "id": 1166527670,
      "role": "prefill",
      "status": "active",
      "enable_multi_endpoints": true,
      "endpoints": {
        "10.10.0.11": {
          "0": {"id": 0, "ip": "10.10.0.11", "business_port": "8000",
                "status": "normal", "headless": false}
        }
      }
    }
  ]
}
```

Notes that matter on the wire:

* `endpoints` is a **map of maps** (`{pod_ip: {endpoint_id: Endpoint}}`), not a
  flat list, and `business_port` is a **string**.
* `Endpoint` rejects unknown keys and requires `business_port`; a violation is
  answered with `400 Invalid request format: ...`.
* `del` uses the same identity fields. `job_name` / `model_name` are carried so
  that `del` does not depend on probing an engine that may already be gone.
* Only `add` is used for registration. Motor's `set` event makes the pools
  contain *exactly* the submitted set, so a per-instance client must never use
  it — `set` would drop every other instance.
* Repeating an identical `add` is a `200` no-op; `del` of an unknown instance is
  also `200` (idempotent), which makes the shutdown path safe to retry.

Motor's response for both events is `{"request_id": "refresh_request",
"status": "success", "message": ..., "data": {"event_type": ..., ...}}`. Errors:
`400` (malformed body / bad role / duplicate ids), `401`/`403` (API key),
`409` (instance id already belongs to a different instance).

## Instance identity

Patio derives the instance id exactly like Motor's own CLI, so the same engine
registered by hand and by the sidecar produces the *same* id (and therefore does
not fight over two entries in the pools):

```python
identity = "|".join([role, "ip:port", ...])          # endpoints sorted by (ip, port)
instance_id = 0x40000000 | (zlib.crc32(identity.encode()) & 0x3FFFFFFF)
```

The namespace bit (`0x40000000`) keeps sidecar/CLI ids out of the range used by
Controller-pushed instances.

## Lifecycle and shutdown budget

```
engine ready ──► wait_engine_ready()  (own /health gate)
                      │
                      ▼
                 register() ──► POST add ──► Coordinator pools (immediately routable)
                      ▲              │
        heartbeat ────┘              │ engine /health fails
        (every HEARTBEAT_INTERVAL)   ▼
                                register() ──► POST del ──► withdrawn
                      │
   SIGTERM/SIGINT ────┴──► unregister() ──► POST del (bounded retry)
```

`unregister()` runs *inside* the signal handler, so it must not be able to delay
pod termination:

* `2xx` → success.
* `404` → the instance is already gone; treated as success (idempotent).
* other `4xx` → a deterministic client error; log and return `False` immediately
  instead of retrying.
* transport error / `5xx` → retry with `MOTOR_UNREGISTER_INTERVAL` until
  `MOTOR_UNREGISTER_DEADLINE` (default 10s), then log the last error and return
  `False`, leaving the client marked as still registered.

Before this was bounded, `unregister()` reused the registration retry policy
(60 attempts × 3s ≈ 180s), which could hold a terminating pod for three minutes
against a Coordinator that was itself gone. `MOTOR_UNREGISTER_DEADLINE` must stay
well below the pod's `terminationGracePeriodSeconds`.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `TOPO_TYPE` | – | set to `Motor` to select this client |
| `ROUTER_ROLE_NAME` | – | router/Coordinator role name in the RBG headless service |
| `ROUTER_PORT` | – | Motor **management** port (1026 by default) |
| `MOTOR_MODEL_NAME` | probe `GET /v1/models` | model name registered with Motor; set it when the engine is unreachable at shutdown |
| `MOTOR_MGMT_API_KEY_FILE` | – | file holding the `X-Motor-Management-Key` value |
| `MOTOR_ENGINE_TYPE` | `vllm` | engine family advertised to Motor (`vllm`/`sglang`) |
| `MOTOR_DISPATCH_CAPABILITIES` | `prefill_handoff_decode` | comma-separated `DispatchPlan` values; empty means "let Motor assume vLLM HANDOFF" |
| `MOTOR_UNREGISTER_DEADLINE` | `10` | seconds, total shutdown deregistration budget |
| `MOTOR_UNREGISTER_INTERVAL` | `1` | seconds between shutdown attempts |
| `MOTOR_UNREGISTER_TIMEOUT` | `3` | per-request timeout during shutdown |

`MOTOR_ENGINE_TYPE` and `MOTOR_DISPATCH_CAPABILITIES` exist because Motor derives
the same information from `engine_config` / `kv_transfer_config` when the K8s
Controller registers an instance; a sidecar has no engine introspection channel
and must state it. A wrong `engine_type` fails at request time inside Motor
("Unsupported native engine type"), and empty capabilities silently mean
"vLLM HANDOFF", so neither is left implicit.

## Failure modes and limits

| Situation | Behaviour |
| --- | --- |
| Coordinator not up yet | registration retries (`utils.retry`, 60 × 3s) like the vLLM client |
| Coordinator gone at shutdown | deregistration gives up after `MOTOR_UNREGISTER_DEADLINE` |
| Engine died but sidecar alive | heartbeat sees `/health` fail → `del`; traffic stops |
| Pod hard-killed (SIGKILL/OOM) | `del` never sent — see below |
| Engine comes back | heartbeat re-registers; `add` is idempotent |

**No TTL/eviction in Motor.** A standalone Coordinator has no reaper for
management-API-registered instances: nothing expires them, and the circuit
breaker is only populated by *reported* failures, so an instance that receives
no traffic after its engine died stays "available" indefinitely. Consequences:

* graceful shutdown (SIGTERM) and engine-death detection are fully covered;
* a hard-killed pod can leave a stale entry until traffic fails against it (the
  circuit breaker then stops routing to it) or an operator cleans it up.

A lease/TTL extension in Motor was deliberately deferred: the same
`/instances/refresh` endpoint is used by the K8s Controller, so the feature would
have to default to *off*, and a wrong expiry policy would evict healthy engines.
Tracked as a Motor-side follow-up rather than a Patio workaround.

Other Motor-side limits a sidecar should know:

* `GET /instances` does not report `engine_type`, `dispatch_capabilities`,
  `parallel_config` or `bootstrap_port`, so the sidecar cannot reconcile those
  from the API — it must remember what it registered.
* Motor's `port_allocator_config.enable` defaults to `true`, so if 1026 is busy
  the management port is silently rewritten (1026 → 1027 …); `ROUTER_PORT` must
  point at the *actual* port.
* The management API key is **off** by default; the management TLS support is
  server-authenticated only and fails open (plaintext) if the cert files are
  missing. Exposing the management port beyond localhost requires enabling the
  API key and terminating TLS.

## Motor-side changes

Two small changes accompany this design (see `MindIE-Motor`):

1. `_validate_refresh_instances` now resolves every instance role *before* any
   pool mutation. Previously an unmappable role raised inside
   `_add_instance_to_available_pool`, i.e. after earlier entries of the same
   request had already been applied, so the caller received a bare `400` while
   some instances were registered — a caller cannot distinguish "registered" from
   "not registered" in that case. The Patio client additionally rejects an
   invalid role locally, before any HTTP call.
2. `docs/zh/user_guide/deployment/standalone.md` 附录 F documents the sidecar
   contract (endpoint, payload, lifecycle, port/auth caveats).

## Verification

* `tests/mock_tests/test_motor_topo_client.py` — payload shape, id derivation,
  register/heartbeat behaviour, failure-driven deregistration, and the bounded,
  fail-fast shutdown path (`404` → success, other `4xx` → immediate failure,
  `5xx`/transport → retried until the deadline).
* `MindIE-Motor/tests/coordinator/api_server/test_management_server_instances.py`
  — the same contract against Motor's real HTTP surface and real
  `InstanceManager`: add → `GET /instances` → idempotent re-add → del → idempotent
  del, immediate routability, all-or-nothing rejection, and API-key gating.
* A wire-level end-to-end run (real HTTP on both sides) exercised the full
  sequence: register → heartbeat no-op → engine death → deregistration → engine
  recovery → re-registration → SIGTERM deregistration → bounded shutdown with the
  Coordinator down.

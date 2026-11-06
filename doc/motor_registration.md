# Motor Registration

How Patio registers an inference engine instance with MindIE-Motor (standalone
Coordinator), and how it deregisters again.

This document is the design record for `TOPO_TYPE=Motor`: it states which
repository owns which behaviour, why the Motor management API is used instead of
a Patio-specific endpoint, the exact wire contract, the lifecycle, and what is
*not* covered.

## Scope

Patio runs as a sidecar next to a native vLLM Prefill/Decode engine. The engine
itself knows nothing about the routing layer, so the sidecar is responsible for
the full instance lifecycle in the router:

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

Patio speaks the protocol Motor designed for exactly this case: the
**coordinator-standalone "External Deployer" event schema**
(`ExternalInsEventMsg`), submitted to the same management endpoint Motor's CLI
(`python3 -m motor.coordinator.register`) uses. A sidecar states only *where* its
engine is; Motor derives the rest of the instance record.

| Concern | Owner | Where |
| --- | --- | --- |
| Registration / deregistration endpoint | Motor | `POST /instances/refresh` (`motor/coordinator/api_server/management_server.py`) |
| External Deployer payload schema + validation | Motor | `ExternalInsEventMsg.to_internal()` (`motor/common/resources/http_msg_spec.py`) |
| Pool & routing state | Motor | `motor/coordinator/domain/instance_manager.py` |
| Cross-request role validation of full `InsEventMsg` events (all-or-nothing) | Motor | `_validate_refresh_instances` (see [below](#motor-side-changes)) |
| Instance identity, payload construction, retries, shutdown budget | Patio | `src/patio/topo/client/motor_topo_client.py` |

So: **the routing-side logic lives in `inference-engine-runtime` (Patio); the
service side needed only a small robustness fix plus documentation in
`MindIE-Motor`.** `vllm-router` is not modified — it remains the reference for
the client-side lifecycle.

### Why not a vLLM-proxy-compatible `/instances/add` route?

The `TOPO_TYPE=vLLM` client posts to `/instances/add`, which is a
`vllm-router`-specific proxy shape. Motor never had that route (its management
API is `/startup`, `/liveness`, `/readiness`, `GET /instances`,
`POST /instances/refresh`, `POST /precision/alarm_cleared`, `GET /`), and the
route does not exist in `vllm-router`'s history either — there the admin surface
is `POST /add_worker?url=` / `POST /remove_worker?url=`.

Motor's answer was a dedicated protocol rather than a compatibility route
(issue #614): the External Deployer schema accepts the *minimum* a deployer
knows — instance id, role, routable endpoints, plus the shared model name and
P/D dispatch plan — and the Coordinator fills in job name, status, DP size and
multi-endpoint routing when converting it to its internal `Instance` model.
Adding a second, vLLM-proxy-shaped way into the same pools was rejected because
it would carry *less* information than Motor's own model (no role, no dispatch
plan, no multi-endpoint groups) and would leave two paths into the pools with no
single source of truth for validation.

## Wire contract

`POST {mgmt_endpoint}/instances/refresh` with header
`X-Motor-Management-Key: <key>` when Motor's management API key is enabled.

```json
{
  "event": "add",
  "model_name": "Qwen3-8B",
  "dispatch_capabilities": "prefill_handoff_decode",
  "engine_type": "vllm",
  "instances": [
    {
      "id": 1166527670,
      "role": "prefill",
      "endpoints": [
        {"id": 0, "address": "10.10.0.11:8000"}
      ]
    }
  ]
}
```

`model_name` is optional — Patio omits it when it cannot resolve one, and Motor
then probes the engines itself:

```json
{
  "event": "add",
  "dispatch_capabilities": "prefill_handoff_decode",
  "engine_type": "vllm",
  "instances": [{"id": 1166527670, "role": "prefill",
                 "endpoints": [{"id": 0, "address": "10.10.0.11:8000"}]}]
}
```

Notes that matter on the wire:

* The schema is `extra="forbid"` and minimal: `event`, `model_name` (optional),
  `dispatch_capabilities`, `engine_type`, `instances[]`, and per instance only
  `id` / `role` / `endpoints[]` with `{"id", "address"}`.
* **Never mix payload shapes.** Motor classifies an event by its shape
  (`_is_standalone_instance_refresh`): the presence of Controller-shaped fields
  (`job_name`, `endpoints` as a *dict*, `status`, `enable_multi_endpoints`) sends
  the request to the internal `InsEventMsg` schema instead. A payload that mixes
  both shapes is rejected rather than guessed.
* Motor derives the fields the payload omits: `job_name = "external-<role>-<id>"`,
  `status = active`, `dp_size = len(endpoints)`, and multi-endpoint routing (one
  instance covering several DP endpoints).
* `endpoints[].address` is a combined `host:port` string (IPv6 as
  `[2001:db8::1]:8200`); the port must be in `1..65535`. This is the flat form —
  not the `{pod_ip: {endpoint_id: Endpoint}}` map of the Controller schema, whose
  `Endpoint` carries `ip` + string `business_port` separately.
* `model_name` omitted → Motor asks **every reachable DP** for `/v1/models`
  (2 s per endpoint) and requires them to agree on a single model; unreachable
  instances are dropped from the event. `model_name` provided → no probe is made,
  which is why `del` carries the name resolved at registration time: shutdown
  must not depend on an engine that is already gone.
* Only `add` / `del` are used by a sidecar. `set` makes the pools contain
  *exactly* the submitted set, so a per-instance client must never use it in a
  multi-instance deployment — it would drop every other instance. (A sidecar
  deployment therefore also needs no `set` replay after a Coordinator restart:
  the pools come back empty and every sidecar re-adds its own instance within one
  heartbeat interval. That differs from a central external deployer, which must
  replay the full topology.)
* Repeating an identical `add` is a `200` no-op; `del` of an unknown instance is
  also `200` (idempotent), which makes the shutdown path safe to retry.

Motor's response for both events is `{"request_id": "refresh_request",
"status": "success", "message": ..., "data": {"event_type": ..., ...}}`. Errors:
`400` (malformed body / unsupported role / duplicate ids / model mismatch),
`401`/`403` (API key), `409` (instance id already belongs to a different
instance).

### What the external schema cannot express

A sidecar can only register what a P/D routing plane can route:

| Knob | Accepted | Notes |
| --- | --- | --- |
| `instances[].role` | `prefill`, `decode` | `encode` / `union` are Controller-topology roles; Motor answers `400 unsupported external instance role`. Patio rejects them locally, before any HTTP call. |
| `engine_type` | `vllm` | Motor rejects anything else ("External Deployer currently supports engine_type='vllm' only"); SGLang standalone deployments are tracked separately (issue #722). |
| `dispatch_capabilities` | `prefill_handoff_decode`, `concurrent_engine_sync` | **One** plan, not a list — the external schema carries a single enum value, and `decode_colocation` is rejected. |

Patio validates all three at startup, so a misconfiguration fails with an
actionable message instead of a late `400` from the Coordinator.

## Instance identity

Patio derives the instance id exactly like Motor's own CLI, so the same engine
registered by hand and by the sidecar produces the *same* id:

```python
identity = "|".join([role, "ip:port", ...])          # endpoints sorted by (ip, port)
instance_id = 0x40000000 | (zlib.crc32(identity.encode()) & 0x3FFFFFFF)
```

The namespace bit (`0x40000000`) keeps sidecar/CLI ids out of the range used by
Controller-pushed instances.

The *id* is shared with the CLI, but the derived `job_name` is not: the external
path fixes `external-<role>-<id>`, while the CLI uses
`<model>-<role>-<ip>-<port>`. Motor treats an `add` as idempotent only when id,
role, job name and endpoint structure all match, so registering the **same
endpoint through both the CLI and a sidecar** conflicts with
`409 instance ID ... already belongs to another instance`. Use one surface per
instance.

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
| `MOTOR_MODEL_NAME` | probe `GET /v1/models` | model name registered with Motor; set it when the engine is unreachable at shutdown, or when a DP serves several models |
| `MOTOR_MGMT_API_KEY_FILE` | – | file holding the `X-Motor-Management-Key` value |
| `MOTOR_ENGINE_TYPE` | `vllm` | engine family advertised to Motor; only `vllm` is accepted by the External Deployer protocol |
| `MOTOR_DISPATCH_CAPABILITIES` | `prefill_handoff_decode` | the single `DispatchPlan` value for this deployment (`prefill_handoff_decode` / `concurrent_engine_sync`); empty means "let Motor assume the vLLM HANDOFF default" |
| `MOTOR_UNREGISTER_DEADLINE` | `10` | seconds, total shutdown deregistration budget |
| `MOTOR_UNREGISTER_INTERVAL` | `1` | seconds between shutdown attempts |
| `MOTOR_UNREGISTER_TIMEOUT` | `3` | per-request timeout during shutdown |

`MOTOR_ENGINE_TYPE` and `MOTOR_DISPATCH_CAPABILITIES` exist because Motor derives
the same information from `engine_config` / `kv_transfer_config` when its
Controller registers an instance; a sidecar has no engine introspection channel
and must state it. Neither is left implicit: a value Motor cannot accept fails at
startup in the Patio client.

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

* `GET /instances` reports `id`, `role`, `job_name`, `model_name`, `status`,
  `pool`, `healthy`, `circuit_breaker` and the endpoint list — but not
  `engine_type`, `dispatch_capabilities`, `parallel_config` or `bootstrap_port`,
  so a sidecar cannot reconcile those from the API; it must remember what it
  registered.
* Motor's `port_allocator_config.enable` defaults to `true`, so if 1026 is busy
  the management port is silently rewritten (1026 → 1027 …); `ROUTER_PORT` must
  point at the *actual* port.
* The management API key is **off** by default; the management TLS support is
  server-authenticated only and fails open (plaintext) if the cert files are
  missing. Exposing the management port beyond localhost requires enabling the
  API key and terminating TLS.
* A Motor build must include the External Deployer schema (`ExternalInsEventMsg`
  in `motor/common/resources/http_msg_spec.py`, issue #614). Older builds only
  know the Controller-shaped `InsEventMsg`, which this client no longer sends.

## Motor-side changes

One small change accompanies this design (see `MindIE-Motor`):
`_validate_refresh_instances` now resolves every instance role *before* any pool
mutation. Previously an unmappable role raised inside
`_add_instance_to_available_pool`, i.e. after earlier entries of the same request
had already been applied, so the caller received a bare `400` while some
instances were registered — a caller cannot distinguish "registered" from "not
registered" in that case.

That path is the Controller-shaped `InsEventMsg` one; the External Deployer path
this client uses validates roles (and endpoint id/address collisions of the whole
event) up front in `to_internal()`. The fix makes the two consistent, and the
Patio client additionally rejects an invalid role locally, before any HTTP call.

## Verification

* `tests/mock_tests/test_motor_topo_client.py` — payload shape (including the
  "never mix shapes" invariant), id derivation, the three protocol restrictions,
  address formatting (IPv4 / DNS / IPv6), register/heartbeat behaviour,
  failure-driven deregistration, and the bounded, fail-fast shutdown path
  (`404` → success, other `4xx` → immediate failure, `5xx`/transport → retried
  until the deadline).
* `MindIE-Motor/tests/coordinator/api_server/test_management_server_instances.py`
  — the same contract against Motor's real HTTP surface and real
  `InstanceManager`: add → `GET /instances` → idempotent re-add → del →
  idempotent del, immediate routability, all-or-nothing rejection, and API-key
  gating.
* A wire-level end-to-end run (real HTTP on both sides, real
  `MotorTopoClient`, real Motor `ManagementServer`) exercised the full sequence:
  payload classified as an External event by Motor's own shape detector → Motor
  derives `job_name` / `dp_size` / dispatch plan → heartbeat no-op → engine death
  → deregistration → engine recovery → re-registration → SIGTERM deregistration →
  omitted `model_name` resolved by Motor's own `/v1/models` probe.

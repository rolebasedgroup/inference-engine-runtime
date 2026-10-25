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
| Engine ready | `MotorTopoClient.register(url, worker_info)` | `POST /instances/refresh` with `event=add` |
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

## The standalone deployment this targets

Motor's official guide is the *Coordinator 独立部署* (standalone Coordinator) page.
What a Patio deployment must match:

* **Ports**: 1025 inference traffic, **1026 management** (`ROUTER_PORT` must point
  here), 1027 observability.
* **Registration entry point**: the Coordinator CLI,
  `python3 -m motor.coordinator.register [set|add|del|list] --prefill IP:PORT[,IP:PORT…] --decode …`,
  with optional `--model-name`, `--mgmt-api-key-file`, `--coordinator` and
  `--dry-run`. It posts the same `ExternalInsEventMsg` shape to the same
  `POST /instances/refresh` endpoint the sidecar uses — the sidecar is simply a
  per-pod replacement for that CLI, which is why both derive the instance id the
  same way.
* **Management API key**: off by default. Enable it in the Coordinator config
  (`mgmt_api_key_config.enable_api_key` / `api_key_file`); Patio then sends
  `X-Motor-Management-Key` from `MOTOR_MGMT_API_KEY_FILE`.
* **Bind address**: the Coordinator binds `127.0.0.1` unless `POD_IP` is injected
  or `coordinator_api_host` is set. Without one of those, Patio's headless-DNS
  endpoint cannot reach port 1026 in another pod — the registration would fail
  with a connection error even though both containers are healthy.
* A standalone Coordinator serves **a single model**, which is why the event
  carries one optional shared `model_name`.

Minimal external event (all Patio actually needs to state):

```json
{
  "event": "add",
  "instances": [
    {"id": 1166527670, "role": "prefill",
     "endpoints": [{"id": 0, "address": "10.10.0.11:8000"}]}
  ]
}
```

`model_name`, `dispatch_capabilities` and `engine_type` are optional on the wire;
Patio sends them so the instance record does not depend on the Coordinator's
defaults.

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
* `model_name` omitted on `add` → Motor asks **every reachable DP** for
  `/v1/models` (2 s per endpoint) and requires them to agree on a single model;
  unreachable instances are dropped from the event. Supplying `model_name` on
  `add` does **not** skip the probe: Motor verifies that every reachable DP
  advertises the name. Patio therefore resolves the name locally first (env or
  the local engine's `/v1/models`) and sends it, so the payload states what the
  sidecar already knows.
* `model_name` is what makes `del` independent of the engine: Motor skips its
  `/v1/models` probe only for a `del` that carries the name. Without it, a `del`
  sent after the engine died answers `400 no reachable native engine /v1/models`
  and the entry stays registered. This is why Patio caches the name resolved at
  registration time and reuses it at shutdown.
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

A sidecar registers one endpoint by default, so its identity is simply
`"<role>|<host>:<port>"`; an opt-in DP group extends it to `"<role>|a|b|..."`.
The addresses are canonicalized before hashing (bare host → `host:8000`, IPv6
bracketed, port as a decimal integer, sorted by `(host, port)`), which is also
the form the CLI hashes — so both surfaces agree for those inputs too.

The namespace bit (`0x40000000`) keeps sidecar/CLI ids out of the range used by
Controller-pushed instances.

The *id* is shared with the CLI, but the derived `job_name` is not: the external
path fixes `external-<role>-<id>`, while the CLI uses
`<model>-<role>-<ip>-<port>`. Motor treats an `add` as idempotent only when id,
role, job name and endpoint structure all match, so registering the **same
endpoint through both the CLI and a sidecar** conflicts with
`409 instance ID ... already belongs to another instance`. Use one surface per
instance.

## Endpoint groups and `--instance-info`

A sidecar owns one P/D instance. **By default it registers exactly one endpoint**
(the `instance` / `port` / `POD_IP` / RBG-DNS address): TP/PP/EP/CP, in-process
DP attention and vLLM's internal LB (`--data-parallel-size=N`, also multi-node)
expose a single routable API server and balance internally, so rank 0 is the
complete routing answer and Motor correctly derives `dp_size=1`.

An external-LB deployment instead runs one vLLM API server per DP rank (dp4×tp4
on one 16-NPU Atlas 800I A3 node = 4 prefill endpoints; dp16×tp1 = 16 decode
endpoints). Such a deployment **opts in** to reporting the whole group; Motor
derives `dp_size` from its size and, on `add`, probes every endpoint and rejects
the instance if one is unreachable.

`--instance-info` is the primary configuration surface for this client:

```json
{"topo_type": "Motor", "data": {"type": "prefill", "port": 8000}}
```

`data` fields, first match wins:

| Field | Meaning |
| --- | --- |
| `endpoints` | Authoritative complete group. Entries are `"host:port"` strings or `{"address": ...}` / `{"ip": ..., "port": ...}` mappings. May span pods. |
| `ports` | Explicit DP ports of this pod's host; the host comes from the address below. |
| `endpoint_num` | Explicit endpoint count, generated as `base_port + i*port_step`. |
| `derive_endpoints` | Boolean opt-in to derive the count from `parallel_config` + `device_num` (see below). |
| `type` / `worker_type` | `prefill` or `decode` (required). |
| `instance` | Explicit `host:port` (or bare host → port 8000) of the single endpoint. |
| `port` | Engine port for the single endpoint (default 8000). |
| `base_port` / `port_step` | Port convention for generated endpoints (defaults 10000 / 2). |
| `parallel_config` | `dp_size`, `tp_size`, `pp_size`, `pcp_size`, `local_world_size`; `local_world_size` defaults to `tp*pp*pcp`. |
| `device_num` / `npu_num` | NPUs on the node, needed by the derivation switch. |
| `data_parallel_size` | Fallback for `parallel_config.dp_size`. |

The equivalent environment variables are `MOTOR_INSTANCE_ENDPOINTS`
(comma-separated group), `MOTOR_ENDPOINT_NUM`, `MOTOR_BASE_PORT`,
`MOTOR_PORT_STEP` and `MOTOR_DERIVE_ENDPOINTS`. An explicit `data` field wins
over its env var, and `endpoints` wins over every generated form.

Examples:

```json
// internal LB (SGLang / vLLM DP): one routable endpoint
{"topo_type": "Motor", "data": {"type": "decode", "port": 8000}}

// external LB, explicit group (dp4 x tp4 prefill on one host)
{"topo_type": "Motor",
 "data": {"type": "prefill",
          "endpoints": ["10.0.0.1:7100", "10.0.0.1:7102",
                        "10.0.0.1:7104", "10.0.0.1:7106"]}}

// external LB, ports derived with Motor's convention (dp16 x tp1 decode)
{"topo_type": "Motor",
 "data": {"type": "decode", "derive_endpoints": true,
          "parallel_config": {"dp_size": 16, "tp_size": 1},
          "device_num": 16, "base_port": 7100, "port_step": 2}}

// group spanning two pods (Motor keys endpoints by ip)
{"topo_type": "Motor",
 "data": {"type": "decode",
          "endpoints": ["prefill-0.s-demo:7100", "prefill-1.s-demo:7100"]}}
```

Notes:

* Derivation is **never implicit**: `parallel_config` alone still means one
  endpoint, because guessing `base_port + i*step` would register ports that do
  not exist for an internal-LB engine.
* A configuration that is present but invalid is an error, not a silent
  fallback: `ports: []`, `endpoint_num: 0` / `-2`, a port outside `1..65535`,
  or `derive_endpoints` without `dp_size`/`device_num` all fail at construction.
* Injected group addresses must be **stable DNS names**, not pod IPs: the
  instance id is a CRC32 over the endpoint list, so a changed pod IP derives a
  new id and orphans the old registration (Motor has no TTL). The owner pod
  must withdraw the whole group when it goes away.
* The health gate probes **every** endpoint in the group. A single unreachable
  rank fails the gate and withdraws the instance, which is what Motor would do
  on the next `add` anyway; a network partition that makes a remote rank
  unreachable therefore withdraws the whole instance.
* Canonicalization can change an existing id for non-canonical input: a bare
  host is now `host:8000`, IPv6 is bracketed and `h:08000` is `h:8000`. Register
  through one surface (CLI or sidecar) and use canonical addresses in
  inventory inputs; the one-endpoint id for a canonical `host:port` is
  unchanged.

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
* `404` → treated as "already deregistered". Motor itself returns `200` for a
  `del` of an unknown instance and only answers `400`/`401`/`403`/`409`/`504`,
  so this branch is defensive (an ingress or gateway deciding a route does not
  exist), not the Coordinator-restart path.
* other `4xx` → a deterministic client error; log and return `False` immediately
  instead of retrying.
* transport error / `5xx` → retry with `MOTOR_UNREGISTER_INTERVAL` until
  `MOTOR_UNREGISTER_DEADLINE` (default 10s), then log the last error and return
  `False`, leaving the client marked as still registered.

Before this was bounded, `unregister()` reused the registration retry policy
(60 attempts × 3s ≈ 180s), which could hold a terminating pod for three minutes
against a Coordinator that was itself gone. `MOTOR_UNREGISTER_DEADLINE` must stay
well below the pod's `terminationGracePeriodSeconds`.

The signal handler is installed *before* `uvicorn.run()`, so deregistration on
SIGTERM depends on uvicorn restoring a custom SIGTERM handler and re-raising the
signal after its graceful drain. That cooperative behaviour exists from
**uvicorn 0.29** (encode/uvicorn#1600); with 0.28 the handler is never called and
the process exits without a `del`. `pyproject.toml` therefore requires
`uvicorn>=0.29`. Even then the handler runs only after the drain, so heartbeats
keep posting `add` until that point — `EXIT_EVENT` plus the add/del lock in the
client close that window.

Shutdown and health-driven deregistration are deliberately different paths:
`EXIT_EVENT` starts no new `add`, while the health path may re-register once the
engine recovers, so it never consults the flag.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `TOPO_TYPE` | – | set to `Motor` to select this client |
| `ROUTER_ROLE_NAME` | – | router/Coordinator role name in the RBG headless service |
| `ROUTER_PORT` | – | Motor **management** port (1026 by default) |
| `MOTOR_MODEL_NAME` | probe the local `GET /v1/models` | model name registered with Motor; when unset, Patio probes the engine itself and only falls back to letting Motor probe if that fails |
| `MOTOR_MGMT_API_KEY_FILE` | – | file holding the `X-Motor-Management-Key` value |
| `MOTOR_ENGINE_TYPE` | `vllm` | engine family advertised to Motor; only `vllm` is accepted by the External Deployer protocol |
| `MOTOR_DISPATCH_CAPABILITIES` | `prefill_handoff_decode` | the single `DispatchPlan` value for this deployment (`prefill_handoff_decode` / `concurrent_engine_sync`); an empty value sends `prefill_handoff_decode` explicitly (Motor's own default), so behaviour does not depend on the Coordinator's defaults |
| `MOTOR_INSTANCE_ENDPOINTS` | – | comma-separated `host:port` DP group (opt-in; see [Endpoint groups](#endpoint-groups-and---instance-info)) |
| `MOTOR_ENDPOINT_NUM` | – | explicit endpoint count, generated as `base_port + i*port_step` |
| `MOTOR_BASE_PORT` | `10000` | first generated DP port |
| `MOTOR_PORT_STEP` | `2` | step between generated DP ports |
| `MOTOR_DERIVE_ENDPOINTS` | off | explicit opt-in to derive ports from `parallel_config` + `device_num` |
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
| Coordinator answers a deterministic `4xx` on `add` | logged and **not** retried: a 409/401 cannot start succeeding, and this runs on the heartbeat, so the next beat retries anyway |
| SIGTERM while an `add` is in flight | no new `add` starts, the shutdown `del` waits (half the budget) for the in-flight one, then is sent |
| Engine dies between heartbeats | the next heartbeat's group health gate fails → `del` |
| One DP of a registered group dies | the gate fails → the whole instance is withdrawn (Motor would reject the next `add` anyway) |
| Coordinator gone at shutdown | deregistration gives up after `MOTOR_UNREGISTER_DEADLINE` |
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
  "never mix shapes" invariant), id derivation over one endpoint and a DP group
  plus address canonicalization, the three protocol restrictions (validated at
  construction), address formatting (IPv4 / DNS / IPv6, including a bracketed
  IPv6 `POD_IP` in both the payload and the `/health` endpoint), endpoint-group
  resolution (explicit group, env, ports, `endpoint_num`, derivation behind its
  opt-in, invalid/empty configurations, port range), per-endpoint health gating,
  register/heartbeat behaviour, failure-driven deregistration, and the bounded,
  fail-fast shutdown path (`404` → success, other `4xx` → immediate failure,
  `5xx`/transport → retried until the deadline, per-attempt timeout capped at
  the remaining budget). The shutdown race has its own cases: no `add` after
  `EXIT_EVENT`, and a `del` blocked until an in-flight `add` returns. A rejected
  heartbeat (`4xx`) must not clear the registration mirror — that regression is
  covered explicitly.
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

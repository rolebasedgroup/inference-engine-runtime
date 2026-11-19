# Proposal 0001: Register Sidecar-Managed Engines with MindIE Motor

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Background](#background)
  - [Problems](#problems)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [User Impact](#user-impact)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Proposed Solution](#proposed-solution)
  - [Detailed Design](#detailed-design)
    - [Endpoint groups are opt-in](#endpoint-groups-are-opt-in)
  - [Configuration](#configuration)
  - [Compatibility](#compatibility)
  - [Observability](#observability)
  - [Security](#security)
  - [Performance and Scalability](#performance-and-scalability)
  - [Risks and Mitigations](#risks-and-mitigations)
  - [Drawbacks](#drawbacks)
  - [Alternatives](#alternatives)
- [Test Plan](#test-plan)
- [Rollout](#rollout)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
- [References](#references)
<!-- /toc -->

## Summary

MindIE Motor Coordinator is a routing/control plane of the same class as the
SGLang router and the vLLM proxy: it holds prefill/decode instances and routes
requests across them. This proposal adds `MotorTopoClient`, a `GroupTopoClient`
that registers the local engine instance with a standalone Motor Coordinator and
withdraws it on failure or shutdown.

The integration is a new `TOPO_TYPE=Motor` value plus a small set of `MOTOR_*`
environment variables; no existing client, API or metric changes. It is enabled
per deployment, so other topology types keep their current behaviour.

## Motivation

### Background

Patio runs as a sidecar next to a native vLLM Prefill/Decode engine. The engine
knows nothing about the routing layer, so the sidecar announces the engine to
whatever router/proxy sits in front of it:

| `TOPO_TYPE` | Client | Router |
| --- | --- | --- |
| `SGLang` | `SGLangGroupTopoClient` | SGLang router (`/workers`) |
| `vLLM` | `VLLMProxyTopoClient` | vLLM proxy (`/instances/add`) |
| `Motor` | `MotorTopoClient` (this proposal) | MindIE Motor Coordinator (`/instances/refresh`) |

Motor's standalone Coordinator exposes the External Deployer protocol
(`ExternalInsEventMsg`): a deployer states instance id, role, routable endpoints
and optionally the shared model and one P/D dispatch plan; Motor derives job
name, status, DP size and multi-endpoint routing.

### Problems

A MindIE Motor deployment has no sidecar path into the Coordinator:

* The only supported registration surface is the Coordinator CLI
  (`python3 -m motor.coordinator.register`). In a Kubernetes deployment that
  means a separate central deployer that must know every pod's endpoint, and it
  cannot react to a single engine dying.
* Without a per-pod client, an engine that crashes keeps its registration:
  Motor standalone has **no TTL/eviction** for management-API instances and
  never answers `404` for an unknown `del`.
* A termination that races an in-flight registration leaves a permanently stale
  entry — the heartbeat's `add` can land after the shutdown's `del` (Motor
  parses an `add` before taking its refresh lock).
* The engine's model name is needed by Motor but is not part of Patio's current
  registration payloads; a `del` without it makes Motor probe an already-dead
  engine and fail with `400`.

### Goals

- [x] Register the local engine with the Coordinator through the External
      Deployer event schema, deriving the Coordinator endpoint from the same
      RBG headless DNS shape as the other clients.
- [x] Same client-side lifecycle as the vLLM integration: readiness gate,
      heartbeat re-registration, failure-driven withdrawal, bounded shutdown
      with an explicit deadline.
- [x] Derive the instance id exactly like the Motor CLI, so a CLI-registered and
      a sidecar-registered instance are the same instance.
- [x] Reject protocol values Motor cannot accept at startup, not at
      registration time.
- [x] Leave no registration behind on SIGTERM/SIGINT even when a heartbeat
      `add` is in flight.

### Non-Goals

- Endpoint groups: the default is **one** endpoint per instance. Reporting a
  whole DP group is an explicit opt-in (`endpoints` / `ports` /
  `endpoint_num` / `MOTOR_DERIVE_ENDPOINTS`) delivered in the follow-up PR #9;
  this proposal decides the default and records the decision
  ([see below](#endpoint-groups-are-opt-in)).
- `encode` / `union` roles and non-vLLM engines: the External Deployer schema
  supports `prefill`/`decode` and `engine_type="vllm"` only.
- A Motor-side TTL/lease, or health-checking of registered instances by the
  Coordinator. Tracked as a Motor follow-up; a wrong expiry policy would evict
  healthy engines.
- Changes to the vLLM proxy or SGLang router integrations.

### User Impact

| Group | Impact |
| --- | --- |
| Kubernetes / RBG operators | New optional `TOPO_TYPE=Motor` deployment shape; the Coordinator must be configured so port 1026 is reachable (it binds `127.0.0.1` unless `POD_IP` / `coordinator_api_host` is set). |
| Engine integration developers | A new reference implementation of the sidecar lifecycle; no change to existing clients. |
| Patio maintainers | One more `GroupTopoClient` implementation and its tests. |
| Application developers | No API change. |

## Proposal

### User Stories

```text
As a Kubernetes operator,
I want each engine pod to register itself with the Motor Coordinator,
so that I do not run a central deployer that has to track every endpoint.

As an operator rolling out a model,
I want a terminating pod to withdraw itself before it exits,
so that Motor stops routing to it and no stale instance survives the pod.
```

### Proposed Solution

Add `MotorTopoClient(GroupTopoClient)` under
`src/patio/topo/client/motor_topo_client.py` and register it in
`topo/factory.py` for `TOPO_TYPE=Motor`.

```text
Patio sidecar (same pod as engine)              Motor Coordinator
  │                                                   │
  │ wait_engine_ready()  ── GET /health ──────────────│
  │                                                   │
  │ register()          ── POST /instances/refresh ──>│ event=add
  │                     <─ 200 ───────────────────────│
  │                                                   │
  │ heartbeat: probe /health, then event=add (no-op)  │
  │                                                   │
  │ engine unhealthy    ── event=del ────────────────>│ withdrawn
  │                                                   │
  │ SIGTERM/SIGINT      ── event=del (bounded) ──────>│ withdrawn
  │                     ── sys.exit(0) ───────────────│
```

The client is a thin, stateless-on-the-wire adapter: all routing state lives in
Motor, and Patio keeps only the cached model name and a registration mirror.

### Detailed Design

#### Wire contract

`POST http://{mgmt_endpoint}/instances/refresh`, header
`X-Motor-Management-Key: <key>` when the Coordinator's management API key is
enabled:

```json
{
  "event": "add",
  "model_name": "Qwen3-8B",
  "dispatch_capabilities": "prefill_handoff_decode",
  "engine_type": "vllm",
  "instances": [
    {"id": 1166527670, "role": "prefill",
     "endpoints": [{"id": 0, "address": "10.10.0.11:8000"}]}
  ]
}
```

* The schema is `extra="forbid"` and minimal. Mixing it with the
  Controller-shaped `InsEventMsg` fields (`job_name`, `endpoints` as a dict,
  `status`) is rejected by Motor, so the client never sends them.
* `set` is never used: it makes the pools contain exactly the submitted set, so
  a per-instance client would drop every other instance.
* A repeated `add` is a `200` no-op; a `del` of an unknown instance is also
  `200`, so both are safe to retry.

#### Lifecycle and concurrency

* `register()` probes the engine's `/health` first. Unhealthy → `del` (if
  registered) and return `False` without posting `add`; healthy → resolve and
  cache the model name, then post `add`. This makes the heartbeat the failure
  detector.
* `EXIT_EVENT` is set by the signal handler before deregistering; `register()`
  checks it before probing and again under the add/del lock, so no new `add`
  starts after shutdown began. The health-driven `del` deliberately ignores the
  flag so a recovered engine can re-register.
* An add/del `threading.Lock` serializes the two events: Motor parses an `add`
  (probing every DP) before taking its own refresh lock, so a concurrent `del`
  could otherwise be applied first and leave the instance registered after
  shutdown. `unregister()` waits at most half of its remaining budget for the
  lock, so an in-flight `add` cannot starve it.
* `unregister()` treats `2xx` and `404` as success, fails fast on other `4xx`,
  retries transport errors / `5xx` until `MOTOR_UNREGISTER_DEADLINE`, and caps
  each request timeout at the remaining budget. It never raises: it also runs
  inside the signal handler, where an exception would skip `sys.exit(0)`.
* `app.py` sets `EXIT_EVENT` and wraps `unregister()` in `try/except` before
  `sys.exit(0)`.
* Deregistration on SIGTERM requires uvicorn ≥ 0.29 (cooperative signal
  handling); `pyproject.toml` carries that floor.

#### Instance identity

```python
identity = "<role>|<host>:<port>"
instance_id = 0x40000000 | (zlib.crc32(identity.encode()) & 0x3FFFFFFF)
```

The address is canonicalized first (`host` → `host:8000`, IPv6 bracketed), which
is also the form the Motor CLI hashes. The id matches the CLI; the derived
`job_name` does not (`external-<role>-<id>` vs `<model>-<role>-<ip>-<port>`), so
Motor answers `409` if the same endpoint is registered through both surfaces.
The doc states this explicitly.

#### Endpoint groups are opt-in

A sidecar owns one P/D instance, and **the default is to register exactly one
endpoint** (the `instance` / `port` / `POD_IP` / RBG-DNS address). TP/PP/EP/CP,
in-process DP attention and vLLM's internal LB (`--data-parallel-size=N`, also
multi-node) all expose a single routable API server and balance internally, so
rank 0 is the complete routing answer and Motor correctly derives `dp_size=1`.

An external-LB deployment instead runs one vLLM API server per DP rank (dp4×tp4
on one 16-NPU Atlas 800I A3 node = 4 prefill endpoints; dp16×tp1 = 16 decode
endpoints). Forcing a group would register ports that do not exist in the common
case, so reporting the group is **explicit opt-in**; the follow-up PR #9 adds it:

| Source (first match wins) | Meaning |
| --- | --- |
| `data.endpoints` | Authoritative complete group (`host:port` strings or mappings); may span pods (Motor keys endpoints by ip). |
| `MOTOR_INSTANCE_ENDPOINTS` | Comma-separated group for env-only deployments. |
| `data.ports` | Explicit DP ports on this pod's host. |
| `data.endpoint_num` / `MOTOR_ENDPOINT_NUM` | Explicit count, generated as `base_port + i*port_step`. |
| `data.derive_endpoints` / `MOTOR_DERIVE_ENDPOINTS` | Explicit switch that derives the count from `parallel_config` + `device_num` with Motor's own convention (`min(dp_size, device_num // local_world_size)`). |

Two decisions are deliberately fixed here:

* **D1 — default = one endpoint; a group is never inferred.** `parallel_config`
  alone still means one endpoint. This keeps internal-LB deployments correct and
  makes "register N endpoints" an explicit statement by the operator.
* **D2 — `parallel_config` + `device_num` derivation is kept, but behind an
  explicit switch.** It encodes a real deployment convention (vLLM API servers at
  `base_port + i*port_step`, one per DP rank), which is useful for large DP
  nodes, but it must not be guessed.

A present-but-invalid configuration (`ports: []`, `endpoint_num < 1`, a port
outside `1..65535`, derivation without `dp_size`/`device_num`) is an error at
construction rather than a silent single-endpoint fallback. The group is
canonicalized and sorted before the id is derived, so a one-endpoint instance
keeps its previous id and a group id matches the CLI's. See
`doc/motor_registration.md` for the full contract.

#### Model name

`MOTOR_MODEL_NAME` wins; otherwise Patio probes the local engine's `/v1/models`
(OpenAI-compatible) and caches the single model id. The cached name is sent on
`del` because Motor skips its own probe only for a `del` that carries it — its
fallback probe of a dead engine fails with `400`. On `add` the name does not skip
Motor's probe (Motor verifies it on every reachable DP); it is sent so the
payload reflects what the sidecar already knows.

#### Validation

`engine_type` and `dispatch_capabilities` are validated once in `__init__` and
cached. A value Motor cannot accept (`sglang`, `decode_colocation`, a comma
separated list) fails at startup with an actionable message instead of a late
`400`, and can never raise from the heartbeat or the signal handler.

### Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `TOPO_TYPE` | – | `Motor` selects this client |
| `ROUTER_ROLE_NAME` / `ROUTER_PORT` | – | Coordinator service in the RBG headless DNS; port is the management port (1026) |
| `MOTOR_MODEL_NAME` | local `/v1/models` probe | model name registered with Motor; if unset Patio probes locally and only then lets Motor probe |
| `MOTOR_MGMT_API_KEY_FILE` | – | file holding the management API key |
| `MOTOR_ENGINE_TYPE` | `vllm` | only `vllm` is accepted |
| `MOTOR_DISPATCH_CAPABILITIES` | `prefill_handoff_decode` | one plan; empty sends the default explicitly |
| `MOTOR_UNREGISTER_DEADLINE` | `10` | total shutdown deregistration budget (s) |
| `MOTOR_UNREGISTER_INTERVAL` | `1` | delay between shutdown attempts (s) |
| `MOTOR_UNREGISTER_TIMEOUT` | `3` | per-request read timeout during shutdown (s) |

The endpoint-group opt-ins (`MOTOR_INSTANCE_ENDPOINTS`, `MOTOR_ENDPOINT_NUM`,
`MOTOR_BASE_PORT`, `MOTOR_PORT_STEP`, `MOTOR_DERIVE_ENDPOINTS`) and the
`--instance-info` `data.*` fields that mirror them arrive with the follow-up
PR #9; they are documented in `doc/motor_registration.md`.

All are read at startup; changing them requires a pod restart. No migration is
needed — a deployment that does not set `TOPO_TYPE=Motor` is unaffected.

### Compatibility

* Additive: a new enum value, new env vars, a new factory entry. Existing
  topology types, APIs and metrics are unchanged.
* Requires a Motor build that includes the External Deployer schema
  (`ExternalInsEventMsg`); older builds only know the Controller-shaped event.
* The shared `app.py` change (set `EXIT_EVENT`, never let `unregister()` block
  exit) also affects the SGLang and vLLM clients, but only makes the existing
  heartbeat stop at shutdown and guarantees the process exits; their
  deregistration is unchanged.
* Rollback: remove `TOPO_TYPE=Motor` from the deployment; no state is stored on
  the Patio side.

### Observability

* Logs at INFO: resolved role/address/instance id/endpoint, successful
  registration and deregistration, shutdown-skipped `add`.
* Logs at WARNING: engine unhealthy → `del`, failed model-name resolution, no
  cached model name at `del`, retried deregistration attempts.
* Logs at ERROR: exhausted registration retries, permanent `4xx`, deadline
  exceeded.
* No new Prometheus metrics; registration health is visible through the
  existing heartbeat log line and Motor's `GET /instances`.

### Security

* The management API is optional-key and binds to localhost by default; Patio
  reads the key from a file (`MOTOR_MGMT_API_KEY_FILE`) and never logs it.
* `mgmt_headers` are passed to `requests` only; the payload and id contain no
  secrets.
* Input validation rejects unsupported roles/engines/plans locally, so a
  compromised sidecar cannot use them to drive Motor into an inconsistent
  state beyond its own instance id.
* TLS to the Coordinator is not implemented (Motor's management TLS is
  server-authenticated); deployments that expose the port beyond localhost must
  enable the API key and terminate TLS in front of the Coordinator.

### Performance and Scalability

* One `add` per `HEARTBEAT_INTERVAL` (default 30s) per engine pod, plus one
  `/health` probe; Motor treats a repeated `add` as a no-op, but it still probes
  `/v1/models` on each `add`, so the heartbeat interval is the main knob for
  Coordinator load in large clusters.
* Registration happens once per pod; no per-request overhead on the data path.
* Memory: one client instance, a cached string, a lock.

### Risks and Mitigations

| Risk | Impact | Detection | Mitigation |
| --- | --- | --- | --- |
| Heartbeat `add` lands after shutdown `del` | Stale instance, no TTL | Instance still in `GET /instances` after pod exit | `EXIT_EVENT` gate + add/del lock |
| Coordinator unreachable at shutdown | Pod held past its grace period | Shutdown log lines, pod termination time | Bounded retry + per-attempt timeout cap |
| Engine dies before `del` | Withdrawal rejected (no model name) | WARNING "no cached model_name" | Cached name from registration; doc note |
| Misconfigured protocol knob | Late `400`, no instance | Startup exception | Validate once in `__init__` |
| Management port unreachable (localhost bind) | Registration never succeeds | Connection errors in log | README/doc note to set `POD_IP` / `coordinator_api_host` |
| Motor restart | Pools empty until next heartbeat | `GET /instances` empty | Heartbeat re-adds; `set` is never used |

### Drawbacks

* Another engine-specific integration to maintain, and the External Deployer
  schema is vllm-only, so SGLang Motor deployments are not covered.
* The heartbeat `add` is not free on the Coordinator (it probes `/v1/models`),
  so the interval has a cost in very large clusters.
* The sidecar cannot reconcile `engine_type` / `dispatch_capabilities` from
  `GET /instances` (not exposed), so it must trust its own configuration.

### Alternatives

1. **Do nothing / central external deployer.** Rejected: a central deployer
   must know every pod endpoint, and it cannot react to a single engine death;
   Patio already owns the per-pod lifecycle for the other routers.
2. **A Patio-specific HTTP endpoint in Motor.** Rejected by Motor maintainers:
   it would duplicate the pool with less information than Motor's own model
   (no role, no dispatch plan) and leave two paths with two validations.
3. **The Controller-shaped `InsEventMsg`.** Rejected: it needs a job name,
   status and a `{pod_ip: {endpoint_id: Endpoint}}` map, i.e. information a
   sidecar does not own; the External Deployer schema exists for this case.
4. **`set` with the full topology from each sidecar.** Rejected: each sidecar
   knows one instance, so `set` would drop every other instance.
5. **Report the full DP group by default (auto-group whenever `parallel_config`
   is present).** Rejected in favour of D1/D2: internal-LB topologies expose one
   routable API server, so inferring a group would register ports that do not
   exist. The group is opt-in, and `parallel_config`-derived ports sit behind an
   explicit switch; the implementation lands in the follow-up PR #9
   ([Endpoint groups are opt-in](#endpoint-groups-are-opt-in)).

## Test Plan

### Unit tests

`tests/mock_tests/test_motor_topo_client.py`:

* payload shape and the "never mix shapes" invariant, `add`/`del` identity
  consistency;
* id derivation and address canonicalization (bare host, IPv4, DNS, bracketed
  IPv6);
* validation of `engine_type` / `dispatch_capabilities` at construction;
* model-name resolution order (env, local probe, failure) and reuse on `del`;
* health-gated registration, failure-driven `del`, re-registration after
  recovery;
* shutdown: no `add` after `EXIT_EVENT`, `del` waits for an in-flight `add`,
  deadline-bounded retries, per-attempt timeout cap, no overshoot when waiting
  for the lock;
* body-construction failures return `False` instead of raising.

### Integration tests

* Factory: `TOPO_TYPE=Motor` selects `MotorTopoClient`.
* Motor-side contract tests (in `MindIE-Motor`) against its real HTTP surface and
  `InstanceManager`: add → `GET /instances` → idempotent re-add → del →
  idempotent del, immediate routability, all-or-nothing rejection, API-key
  gating.

### End-to-end tests

A wire-level run with a real `MotorTopoClient` and a real Motor `ManagementServer`
covered: shape classification, derived `job_name` / `dp_size` / dispatch plan,
heartbeat no-op, engine death → `del`, recovery → re-add, SIGTERM `del`, and
model-name resolution when omitted.

### Failure and adversarial tests

Covered by the unit cases above: invalid configuration, unreachable engine,
Coordinator gone at shutdown, 4xx (permanent) vs 5xx (retryable), and the
shutdown race.

## Rollout

* Ship behind `TOPO_TYPE=Motor`; no feature flag is needed since the client is
  only instantiated for that value.
* Document the deployment contract (management port 1026, API key toggle,
  Coordinator bind address, uvicorn ≥ 0.29) in `README.md` and
  `doc/motor_registration.md`.
* Rollback is a configuration change (switch `TOPO_TYPE` back); no Patio state
  to clean up, and Motor's stale entries are withdrawn by the next `del` or are
  cleaned up by the operator.

## Open Questions

<<[UNRESOLVED maintainers]>> Whether `MOTOR_DISPATCH_CAPABILITIES` and
`MOTOR_ENGINE_TYPE` should keep explicit defaults in Patio or be omitted when
unset so Motor applies its own defaults. The current client validates and sends
them, which makes the behaviour independent of the Coordinator version.
<<[/UNRESOLVED]>>

## Implementation History

- 2026-11-15: Initial draft, extracted from `doc/motor_registration.md` and the
  implementation in PR #6.
- 2026-11-15: Endpoint groups decided — **D1**: one endpoint by default, a DP
  group only when explicitly requested; **D2**: `parallel_config` + `device_num`
  port derivation kept behind an explicit switch. Implementation follows in
  PR #9; the resolved question was folded into
  [Endpoint groups are opt-in](#endpoint-groups-are-opt-in).

## References

- Design record: `doc/motor_registration.md`
- Motor Event Management / External Deployer: MindIE-Motor issue #614
- Motor standalone deployment guide: Coordinator 独立部署
- Motor CLI registration: `python3 -m motor.coordinator.register`
- Related PR: the `MotorTopoClient` implementation in this repository

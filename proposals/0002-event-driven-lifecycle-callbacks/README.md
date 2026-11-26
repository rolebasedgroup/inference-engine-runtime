# Proposal 0002: Event-Driven Lifecycle Callbacks

## Summary

Patio runs engine lifecycle operations as configured callbacks instead of
hard-coding one topology startup sequence. A new `events.yaml` format binds
`start`, `hook`, and `end` events to named callbacks, and new SGLang/vLLM
adapters provide those callbacks for engine readiness, router onboarding, and
router offboarding. RoleInstance signal labels can be read through the
Kubernetes API or a downwardAPI label file, while signal clearing remains an
atomic Kubernetes API operation. This gives RBG a stable, engine-neutral way to
drive membership transitions without changing Patio code for every engine.

## Motivation

### Background

Patio currently has two separate paths: a FastAPI management server and a
legacy topology client that registers workers with SGLang/vLLM routers during
process startup. The topology sequence, retry policy, and shutdown behavior are
hard-coded in each client. RBG increasingly needs lifecycle hooks (for example
worker offboarding before deletion), and each hook currently requires another
engine-specific code path.

### Problems

- Engine startup, onboarding, and shutdown behavior cannot be configured for
  different deployment models.
- RBG has no reliable way to trigger an engine action from a RoleInstance
  label change.
- Polling the Kubernetes API from every engine sidecar scales poorly for large
  groups.
- Router membership changes must not race with the heartbeat that keeps
  re-registering a healthy worker.

### Goals

- [ ] Provide a validated, ordered `events.yaml` configuration for start, hook,
  and end callbacks.
- [ ] Support SGLang and vLLM readiness, onboarding, and offboarding.
- [ ] Read hook signals from the Kubernetes API or a downwardAPI label file.
- [ ] Make signal clearing conditional on the processed signal value.
- [ ] Keep heartbeat recovery while preventing re-registration after offboard.
- [ ] Package Patio as a non-root sidecar image.

### Non-Goals

- No new public HTTP API on the Patio server.
- No engine API changes; only existing SGLang/vLLM router and health endpoints
  are used.
- No support for engines other than SGLang and vLLM in the first version.
- No generic workflow engine, DAG execution, or user-defined script hooks.

### User Impact

- RBG operators can drive lifecycle transitions with RoleInstance labels.
- Engine integrators implement one callback plugin contract instead of a full
  topology client.
- Deployment owners can switch signal polling from the API to downwardAPI to
  reduce API-server load, and must provide `get`/`patch` RBAC for RoleInstance.

## Proposal

### Proposed Solution

The application assembles an `EventExecutor` from `events.yaml`, engine
adapters, and a signal source. Start and end callbacks run in configuration
order. The executor polls matching `key=value` signals, runs each hook callback,
and clears the signal only when the API server confirms the label still has the
processed value.

```text
RBG Controller     RoleInstance/Pod labels       Patio Sidecar
      │                    │                          │
      │ set signalLabel    │                          │
      │───────────────────>│                          │
      │                    │ signal file / API GET    │
      │                    │─────────────────────────>│
      │                    │                          │ run hook callback
      │                    │                          │ (engine / router)
      │   conditional JSON Patch (test + remove)     │
      │<──────────────────────────────────────────────│
      │ remove pod label / update signal             │
```

### Detailed Design

#### Event model

`events.yaml` contains one `event_callbacks` object with `start`, `end`, and
`hook` lists. Names must resolve to registered callbacks. Hook entries bind one
`key=value` signal to one callback; duplicate signals are rejected.

#### Execution

- Start callbacks use the configured retry policy.
- End callbacks use a shorter separate budget so shutdown fits inside
  `terminationGracePeriodSeconds`.
- A failed hook callback does not stop other matching signals in the same
  polling round; the failed signal is retried after its execution interval.
- The executor treats a signal-value change during clear as processed and
  leaves the controller's new value untouched.

#### Signal sources

- `kubernetes`: poll RoleInstance labels through the API.
- `file`: read labels from a downwardAPI projection; clearing still goes
  through the API.
- An explicitly configured source is validated at startup. Without an explicit
  configuration and outside a cluster, Patio falls back to an empty in-memory
  source and warns when hooks are configured.

Signal clearing is a JSON Patch `test` followed by `remove`, with JSON Pointer
escaping for label keys. A 422 response means the value changed and no label is
removed.

#### Membership safety

Heartbeat re-registration and offboard hooks share one membership lock. The
offboard callback suspends the heartbeat before removing the worker, and the
heartbeat rechecks its state inside the lock. A later onboard hook may resume a
suspended heartbeat.

#### Error handling

Readiness probes perform one bounded check and let the executor own retry
policy. Deleted router DNS names during offboard are treated as successful
removal. Kubernetes API TLS verification fails closed unless
`LIFECYCLE_API_INSECURE=true` is explicitly set.

### Configuration

| Setting | Default | Meaning |
|---|---|---|
| `EVENTS_CONFIG` / `--events-config` | none | Path to `events.yaml` |
| `LIFECYCLE_SIGNAL_SOURCE` | `kubernetes` | `kubernetes` or `file` |
| `LIFECYCLE_SIGNAL_FILE` | `/etc/patio/pod-labels` | downwardAPI label path |
| `LIFECYCLE_API_TIMEOUT` | `10` | Kubernetes request timeout |
| `LIFECYCLE_API_INSECURE` | `false` | Explicit TLS opt-out |
| `EVENT_POLL_INTERVAL` | `1` | Delay between idle polls |
| `EVENT_CALLBACK_RETRY_TIMES` | `60` | Start/hook attempts |
| `EVENT_CALLBACK_RETRY_INTERVAL` | `3` | Start/hook retry delay |
| `EVENT_END_CALLBACK_RETRY_TIMES` | `5` | End attempts |
| `EVENT_END_CALLBACK_RETRY_INTERVAL` | `1` | End retry delay |
| `EVENT_SIGNAL_INTERVAL` | `60` | Same-signal execution cooldown |

### Compatibility

Deployments that do not pass `--events-config` keep the legacy topology client
path and its heartbeat. The event path preserves the legacy heartbeat contract
when `router_onboard` is configured as a start callback. Existing environment
variables remain unchanged. Rollback is removing `--events-config` from the
container command.

### Observability

Callback start, completion, failure, retry, signal detection, clear, skipped
clear, heartbeat suspension, and heartbeat resume are logged with structured
fields. No new Prometheus metrics are introduced in this version.

### Security

The sidecar needs a ServiceAccount with `get` and `patch` on RoleInstance in its
namespace. TLS verification uses the mounted cluster CA and fails closed if the
CA is missing; insecure mode is opt-in for test environments. The container runs
as non-root UID 1000 with a narrowed build context.

### Risks and Mitigations

| Risk | Impact | Detection | Mitigation |
|---|---|---|---|
| Stale downwardAPI file causes duplicate hook execution | Duplicate offboard requests | Hook logs | Execution cooldown plus idempotent callbacks |
| Controller updates signal during callback | New command discarded | JSON Patch 422 warning | Conditional `test`/`remove` patch |
| Heartbeat races offboard | Removed worker re-registered | Membership logs | Shared membership lock and pre-callback suspension |
| End callbacks exceed grace period | SIGKILL before cleanup | Shutdown logs | Separate short end retry budget |

### Drawbacks

The callback indirection makes startup behavior less obvious from code alone.
It also introduces a second topology execution path that must be kept behavior
compatible with the legacy client until that client is retired.

### Alternatives

1. Keep hard-coded topology clients: rejected because every lifecycle change
   needs engine-specific code.
2. Put lifecycle logic in RBG only: rejected because RBG cannot call engine
   internals such as worker-ID reconciliation.
3. A generic plugin/script runner: deferred; it would add an unsafe execution
   surface without a current requirement.

## Test Plan

### Unit tests

`tests/mock_tests/test_event_callbacks.py` covers YAML parsing, callback
resolution, ordering, retries, end retry budget, hook starvation, changed-signal
clearing, heartbeat suspend/resume, membership serialization, both signal
sources, DNS failure handling, vLLM single-shot readiness, and the vLLM proxy
payload compatibility fields.

### Integration tests

Mocked router and engine adapters exercise start, offboard, heartbeat, and
end-to-end application startup/shutdown.

### End-to-end tests

Manual smoke tests with a real RBG group, SGLang and vLLM engines, and a
downwardAPI volume remain required before promotion of the image.

### Failure and adversarial tests

Unit tests cover signal value changes, API clear failures, missing Kubernetes
configuration, missing CA files, router deletion during offboard, and persistently
failing hooks.

## Rollout

1. Merge the callback implementation and proposal.
2. Build the non-root sidecar image and validate it in a test RBG group.
3. Update the runtime profile to pass `--events-config`.
4. Remove the legacy topology path in a later release after migration.

## Open Questions

<<[UNRESOLVED vLLM maintainers]>>
Confirm which vLLM proxy implementation is the supported integration target and
whether its membership API is stable. The official vLLM disaggregated proxy demo
exposes only `/instances/add` with a singular `instance` field; the vLLM Ascend
load-balance proxy exposes both `/instances/add` and `/instances/remove` with a
plural `instances` field, and their authentication requirements differ. Until
the contract is standardized, Patio sends both fields and treats HTTP 404 from
removal as a no-op because the official demo has no remove route.
<<[/UNRESOLVED]>>

<<[UNRESOLVED RBG maintainers]>>
Should an onboard signal resume heartbeat recovery, or should only process
restart re-enable registration after an explicit offboard?
<<[/UNRESOLVED]>>

## Implementation History

- 2026-11-18: Initial event callback implementation.
- 2026-11-24: File signal source and signal execution cooldown.
- 2026-11-26: Review fixes for atomic clears, membership races, retry budgets,
  TLS verification, the vLLM proxy payload compatibility fields, and this
  proposal.

## References

- Related PR: https://github.com/rolebasedgroup/inference-engine-runtime/pull/15
- Proposal process: /proposals/README.md

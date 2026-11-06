---
name: patio-dev
description: >-
  Implement features and fixes in Patio (inference-engine-runtime), the Python sidecar between RoleBasedGroup
  (RBG) and SGLang/vLLM inference engines. Use when adding or changing a topology or router registration
  client (GroupTopoClient for SGLang Router, vLLM proxy, MindIE Motor, and similar), an inference engine
  backend, engine metric normalization rules, environment variables or CLI flags, HTTP endpoints, or
  startup/shutdown behavior. Also use it when you need Patio's detailed architecture, its configuration
  reference, or its unit-test patterns.
---

# Patio development

Read `AGENTS.md` at the repository root first: it has the repo map, public contracts, conventions, and
commands. This skill adds the workflow, playbooks, and detailed references.

## Workflow

1. **Locate.** Find the components and flows involved in `references/architecture.md`.
2. **Check scope.** Adding an engine backend or a router integration, or changing a public API, protocol,
   env/CLI contract, lifecycle, or failure recovery, requires a proposal. See `proposals/README.md` and copy
   `proposals/NNNN-template/`. Start implementing once the direction is agreed.
3. **Follow the playbook** for the task (table below). Each playbook lists the exact files to touch.
4. **Write tests** first or alongside the code, using the patterns in `references/testing.md`. Cover the
   failure paths, not only the happy path.
5. **Sync contracts and docs:** README tables, `doc/*.md`, and `references/configuration.md` for any new env
   var or CLI flag.
6. **Validate.** Run `pytest`; every test must pass. For CLI changes, also run `patio --help`.
7. **Self-review** against `.agents/skills/patio-review/references/checklist.md` before handing off.

## Playbooks

| Task | Reference |
|------|-----------|
| Add or change a router/proxy integration (`TOPO_TYPE`) | `references/add-topo-client.md` |
| Add an inference engine backend (`INFERENCE_ENGINE`) | `references/add-inference-engine.md` |
| Add or rename standardized engine metrics | `references/add-engine-metrics.md` |
| Add an env var or CLI flag | `references/configuration.md`, section "Adding configuration" |
| Add an HTTP endpoint | `references/architecture.md`, section "Adding an HTTP endpoint" |
| Write or fix tests | `references/testing.md` |

## Guardrails (the most frequent review findings)

- Every outbound request has an explicit timeout, and nothing on the SIGTERM path can outlast the pod's
  grace period.
- A renamed env var keeps the old name as a fallback, and topology-type matching stays case-insensitive.
- `register()` is idempotent, because the heartbeat calls it again and again.
- No blocking I/O inside `async def`, and no new `httpx.AsyncClient` per request unless it is closed.
- Only pydantic v2 validators; request models extend `NoExtraBaseModel`.
- Don't copy any pattern listed in `.agents/skills/patio-review/references/known-issues.md`.

## Done when

The "Definition of done" in `AGENTS.md` is met and `pytest` passes.

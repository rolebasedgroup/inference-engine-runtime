---
name: patio-review
description: >-
  Review pull requests, diffs, or patches for Patio (inference-engine-runtime), the Python sidecar between
  RoleBasedGroup and SGLang/vLLM inference engines. Use when asked to review code or a PR that touches
  src/patio, tests, README or doc, env vars, CLI flags, metrics, topology/router clients, or proposals. Provides
  the review procedure, severity rubric, project-specific checklist, verified known issues, and the comment
  format.
---

# Patio code review

Read `AGENTS.md` at the repository root first: it lists the public contracts and conventions. The code is the
source of truth; README and `doc/` are out of date in places.

## Procedure

1. **Scope the diff.** List the touched areas: api, engine, metrics, topo, app/lifecycle, configuration (envs,
   config, CLI), tests, docs, proposals.
2. **Find contract changes** (`AGENTS.md`, "Public contracts"): env vars and CLI flags, inbound endpoints and
   payloads and error shapes, exposed metric names and labels, outbound engine and router requests, RBG
   addressing. For each one, ask: is it backward compatible, is it documented, does it need a proposal?
3. **Run the area checklists** in `references/checklist.md` for every touched area.
4. **Check the tests.** New behavior has success and failure-path tests in `tests/mock_tests/`; outbound calls
   are asserted exactly; there is no real network access and no long sleep. Run `pytest` if you can execute
   commands.
5. **Check the docs.** README tables and `doc/*.md` match the new code: names, defaults, accepted values.
6. **Check for known-issue patterns** (`references/known-issues.md`). Flag new code that copies one. Don't
   re-flag pre-existing instances on lines the PR doesn't touch. If the PR fixes one, ask for its entry to be
   removed.
7. **Write the findings** in the format below. A few high-confidence findings beat many speculative ones.
   Verify a claim against the code (or by running it) before you post it.

## Severity

| Level | Use for |
|-------|---------|
| **Blocker** | Startup or shutdown can hang or crash the pod (missing timeout, unbounded retry on the SIGTERM path, uncaught exception during startup); breaking contract change without compatibility (env/CLI rename without fallback, changed metric name, endpoint, or payload); secrets in logs or responses; failing tests |
| **Major** | Missing tests for new behavior or failure paths; README/`doc` not updated for a contract change; resource leak; blocking I/O in `async def`; non-idempotent `register()`; unbounded metric label values; new integration without a proposal |
| **Minor** | Inconsistent log messages or naming, small simplifications, unclear comments in touched lines |

## Don't flag (maintainer decisions)

- Topology clients that catch exceptions, log with a traceback, and return `False`. The pattern is kept
  deliberately consistent across clients (#3).
- The canonical topology names `SGLang`/`vLLM` in `config.SUPPORTED_TOPO_TYPES` alongside case-insensitive
  matching in `topo/factory.py`. Deployed profiles use lowercase values (#3).
- `VLLMProxyTopoClient.unregister()` being a no-op: the vLLM proxy has no unregister API.
- Clients that don't re-check `worker_info is None`: `app.run_topo_client` validates `data` (#3).
- Formatting-only issues. No formatter is enforced.
- Pre-existing issues on lines the PR doesn't touch. Mention them at most once, as a note.

## Output format

Open with a short summary and a verdict: **approve**, **request changes**, or **comment**. Then:

```markdown
### Findings
**[Blocker] `src/patio/topo/client/x_topo_client.py:42`: unregister can block SIGTERM for minutes**
What is wrong, the concrete impact on the pod, router, or deployment, and a suggested fix (with code if short).

### Contract changes
- `ROUTER_FOO` env var (new): compatible; README updated: no; proposal: not needed.

### Docs to update
- README "Environment Variables" table: add `ROUTER_FOO` (default `5`).
```

Use "none" for empty sections. In inline PR comments, post one finding per comment with its severity prefix.

## References

- `references/checklist.md`: per-area checklist.
- `references/known-issues.md`: verified existing problems and documentation drift. Don't copy these patterns.
- `.agents/skills/patio-dev/references/architecture.md`: flows, threads, component details.
- `.agents/skills/patio-dev/references/configuration.md`: env-var and CLI truth table.

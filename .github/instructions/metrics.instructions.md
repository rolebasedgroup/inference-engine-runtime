---
applyTo: "src/patio/metrics/**"
---

# Metrics

- Register collectors and metrics on `patio.metrics.metrics.patio_registry`, never on the global
  `prometheus_client.REGISTRY`. `/metrics` exports only `patio_registry`.
- Rules in `engine_metric_rules.py` are keyed by the **family name** that
  `prometheus_client.parser.text_string_to_metric_families` returns. Counters lose `_total`
  (`sglang:prompt_tokens_total` becomes `sglang:prompt_tokens`). Histograms keep the base name, and their
  samples keep `_bucket`, `_sum`, and `_count`. The dict key must equal `RenameStandardRule.old_name`.
- Only engine metrics that have a rule are exposed; every other family is dropped.
- Standardized names use the `patio:` prefix and the constants at the top of `engine_metric_rules.py`. Map two
  engines to the same `patio:` name only when the semantics and units match.
- Exposed metric names and labels are a public contract, used by dashboards and autoscaling. A rename or
  removal is a breaking change: keep compatibility or document the migration, and update the README metrics
  table and `doc/metrics.md`.
- `EngineCollector.collect()` and the rules must never raise; one exception fails the whole `/metrics`
  response. The scrape is synchronous and runs on every Prometheus scrape, so keep its timeout small
  (default 2 s).
- Keep label values bounded: no raw paths, IDs, or user input.
- Test rules end to end. Feed Prometheus text through `EngineCollector` (patch
  `patio.metrics.engine_collector.requests.Session`) into a fresh `CollectorRegistry`, then assert on the
  `generate_latest` output.

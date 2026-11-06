# Playbook: add or change standardized engine metrics

Patio exposes **only** the engine metrics that have a rule in `src/patio/metrics/engine_metric_rules.py`, renamed
into the `patio:` namespace. Background: `references/architecture.md` §8.

## Steps

1. **Find the family name.** Rules are keyed by the family name that `prometheus_client` parses, which is not
   always the name in the exposition text. Counters lose `_total` (`sglang:prompt_tokens_total` becomes family
   `sglang:prompt_tokens`). Histograms and summaries use the base name, and their samples keep `_bucket`,
   `_sum`, and `_count`. Check against real engine output:

   ```python
   from prometheus_client.parser import text_string_to_metric_families

   with open("engine_metrics.txt") as f:  # saved from curl http://<engine>/metrics
       for family in text_string_to_metric_families(f.read()):
           print(family.name, family.type)
   ```

2. **Choose the Patio name.** Reuse an existing constant at the top of `engine_metric_rules.py` when the
   semantics and units match: `sglang:token_usage` and `vllm:kv_cache_usage_perc` both map to
   `patio:gpu_cache_usage_perc`. Otherwise add a constant `patio:<name>`. Counter constants have no `_total`;
   the exposition adds it.
3. **Add the rule:** `"<family>": RenameStandardRule("<family>", <CONSTANT>)` in the engine's dict. The key must
   equal `old_name`, or the rule's assertion fails during `/metrics`.
4. **Write a new rule type only when renaming isn't enough** (unit conversion, label changes). Subclass
   `StandardRule` and implement `__call__(metric) -> Iterable[Metric]`. It must never raise, because one
   exception fails the whole `/metrics` response. Keep label values bounded.
5. **Treat it as a contract change.** Exposed names feed dashboards and autoscaling. Adding a metric is
   compatible; renaming or removing one is breaking and needs a migration note. Update the README metrics table
   and `doc/metrics.md`.
6. **Test end to end** with the template below.

## Test template

```python
from unittest.mock import MagicMock, patch

from prometheus_client import CollectorRegistry, generate_latest

from patio.metrics.engine_collector import EngineCollector
from patio.metrics.engine_metric_rules import SGLANG_METRIC_STANDARD_RULES
from patio.metrics.standard_rules import RenameStandardRule

ENGINE_METRICS = """\
# HELP sglang:num_running_reqs Running requests
# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{model_name="m"} 3.0
# HELP sglang:unmapped Not mapped
# TYPE sglang:unmapped gauge
sglang:unmapped 1.0
"""

COUNTER_METRICS = """\
# HELP sglang:prompt_tokens_total Prompt tokens
# TYPE sglang:prompt_tokens_total counter
sglang:prompt_tokens_total{model_name="m"} 5.0
"""


def scrape(text, rules):
    registry = CollectorRegistry()
    with patch("patio.metrics.engine_collector.requests.Session") as session_class:
        session_class.return_value.get.return_value = MagicMock(
            status_code=200, text=text
        )
        registry.register(EngineCollector("http://engine/metrics", rules))
        return generate_latest(registry).decode()


def test_sglang_gauge_is_renamed_and_unmapped_metrics_are_dropped():
    output = scrape(ENGINE_METRICS, SGLANG_METRIC_STANDARD_RULES)

    assert "# TYPE patio:num_requests_running gauge" in output
    assert 'patio:num_requests_running{model_name="m"} 3.0' in output
    assert "unmapped" not in output


def test_counter_rule_uses_family_name_without_total():
    rules = {
        "sglang:prompt_tokens": RenameStandardRule(
            "sglang:prompt_tokens", "patio:input_tokens"
        )
    }

    output = scrape(COUNTER_METRICS, rules)

    assert "# TYPE patio:input_tokens_total counter" in output
    assert 'patio:input_tokens_total{model_name="m"} 5.0' in output
```

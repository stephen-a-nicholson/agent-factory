"""eval_threshold_present: fail if a domain with rag.enabled has an empty
rag.eval.promotion_threshold. Reads domain.yml directly rather than the
bundle config: promotion_threshold is domain metadata consumed at job run
time (src/evaluate/rag.py) and embedded into the eval_regression alert's
query, not a bundle resource field gates could check any other way.
"""

from __future__ import annotations

from typing import Any

from gates.models import Finding, Rule, Severity


class EvalThresholdPresent(Rule):
    name = "eval_threshold_present"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        from factory.schema import discover_domains, load_domain

        findings: list[Finding] = []
        for path in discover_domains():
            domain = load_domain(path)
            if domain.rag is None or not domain.rag.enabled:
                continue
            if not domain.rag.eval.promotion_threshold:
                findings.append(
                    Finding(
                        self.name,
                        Severity.FAIL,
                        f"domain {domain.name!r} has rag.enabled but no "
                        "rag.eval.promotion_threshold entries",
                        resource=f"domains/{domain.name}/domain.yml",
                    )
                )
        return findings

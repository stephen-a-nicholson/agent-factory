"""prod_no_dev_mode: fail if a validate config resolved for the test or
prod target reports mode: development. gates/run.py resolves one target
per invocation (the config already reflects whichever -t was passed to
`databricks bundle validate`), so this rule just checks the resolved
`bundle.mode`/`bundle.target` pair on whatever config it was given.
"""

from __future__ import annotations

from typing import Any

from gates.models import Finding, Rule, Severity

NON_DEV_TARGETS = ("test", "prod")


class ProdNoDevMode(Rule):
    name = "prod_no_dev_mode"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        bundle = config.get("bundle", {})
        target = bundle.get("target")
        mode = bundle.get("mode")
        if target in NON_DEV_TARGETS and mode != "production":
            return [
                Finding(
                    self.name,
                    Severity.FAIL,
                    f"target {target!r} must be mode: production, got {mode!r}",
                    resource="bundle",
                )
            ]
        return []

"""vs_sync_in_nonprod: fail if a vector search index is not TRIGGERED
outside prod. CONTINUOUS sync costs more and is unnecessary before prod;
domain.yml's `${var.vs_sync}` should resolve to TRIGGERED for every
non-prod target (see docs/DESIGN.md section 5).
"""

from __future__ import annotations

from typing import Any

from gates.loader import resources_of_type
from gates.models import Finding, Rule, Severity


class VsSyncInNonprod(Rule):
    name = "vs_sync_in_nonprod"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        target = config.get("bundle", {}).get("target")
        if target == "prod":
            return []

        findings: list[Finding] = []
        for key, index in resources_of_type(config, "vector_search_indexes").items():
            pipeline_type = index.get("delta_sync_index_spec", {}).get("pipeline_type")
            if pipeline_type != "TRIGGERED":
                findings.append(
                    Finding(
                        self.name,
                        Severity.FAIL,
                        f"pipeline_type is {pipeline_type!r}, must be TRIGGERED "
                        f"outside prod (target: {target!r})",
                        resource=f"resources.vector_search_indexes.{key}",
                    )
                )
        return findings

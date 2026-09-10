"""endpoint_scale_to_zero (warn): a bundle-declared model_serving_endpoints
resource should set scale_to_zero_enabled: true outside prod, to avoid
idle serverless capacity cost.

This project has no bundle-declared model_serving_endpoints resource at
all (see docs/DESIGN.md section 4: the endpoint is created and updated
directly via the SDK, in src/agent/deploy.py/promote.py, to avoid a
circular dependency and a numeric-only entity_version limitation), so
this rule always finds nothing to check here — not a bug, just nothing
of this resource type for gates to see via `bundle validate`/`bundle
plan`. Kept as a real rule (rather than dropped) for a future domain or a
different project using this gates package that does declare one.
"""

from __future__ import annotations

from typing import Any

from gates.loader import resources_of_type
from gates.models import Finding, Rule, Severity


class EndpointScaleToZero(Rule):
    name = "endpoint_scale_to_zero"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        target = config.get("bundle", {}).get("target")
        if target == "prod":
            return []

        findings: list[Finding] = []
        for key, endpoint in resources_of_type(config, "model_serving_endpoints").items():
            served_entities = endpoint.get("config", {}).get("served_entities", []) or []
            for entity in served_entities:
                if not entity.get("scale_to_zero_enabled"):
                    findings.append(
                        Finding(
                            self.name,
                            Severity.WARN,
                            "served entity does not set scale_to_zero_enabled: true "
                            f"(target: {target!r})",
                            resource=f"resources.model_serving_endpoints.{key}",
                        )
                    )
        return findings

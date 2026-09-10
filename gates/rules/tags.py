"""required_tags: fail if a job is missing the owner or cost_centre tag.

DESIGN.md section 7 also says "every job and endpoint": this project has
no bundle-declared model_serving_endpoints resource (see DESIGN.md
section 4 and factory/generate.py's _deploy_agent_job_resource docstring
for why), so there is currently no endpoint resource type in the bundle
config to check tags on. If a future phase adds a taggable endpoint-like
resource, extend this rule to cover it too.
"""

from __future__ import annotations

from typing import Any

from gates.loader import resources_of_type
from gates.models import Finding, Rule, Severity

REQUIRED_TAGS = ("owner", "cost_centre")


class RequiredTags(Rule):
    name = "required_tags"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        findings: list[Finding] = []
        for job_key, job in resources_of_type(config, "jobs").items():
            tags = job.get("tags", {}) or {}
            missing = [t for t in REQUIRED_TAGS if t not in tags]
            if missing:
                findings.append(
                    Finding(
                        self.name,
                        Severity.FAIL,
                        f"missing required tag(s): {', '.join(missing)}",
                        resource=f"resources.jobs.{job_key}",
                    )
                )
        return findings

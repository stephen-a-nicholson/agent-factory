"""no_all_purpose_clusters: fail if any job task pins an all-purpose
cluster (existing_cluster_id) or defines its own (new_cluster), or if the
bundle declares a top-level clusters resource. Every job in this project
must run on serverless (an environment_key, no cluster fields) or a job
cluster referenced via job_cluster_key, per CLAUDE.md's hard constraints.
"""

from __future__ import annotations

from typing import Any

from gates.loader import resources_of_type
from gates.models import Finding, Rule, Severity


class NoAllPurposeClusters(Rule):
    name = "no_all_purpose_clusters"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        findings: list[Finding] = []
        if resources_of_type(config, "clusters"):
            findings.append(
                Finding(
                    self.name,
                    Severity.FAIL,
                    "bundle declares a clusters resource; use serverless or job clusters",
                    resource="resources.clusters",
                )
            )

        for job_key, job in resources_of_type(config, "jobs").items():
            for task in job.get("tasks", []) or []:
                task_key = task.get("task_key", "?")
                if "existing_cluster_id" in task:
                    findings.append(
                        Finding(
                            self.name,
                            Severity.FAIL,
                            f"task {task_key!r} pins an all-purpose cluster (existing_cluster_id)",
                            resource=f"resources.jobs.{job_key}",
                        )
                    )
                if "new_cluster" in task:
                    findings.append(
                        Finding(
                            self.name,
                            Severity.FAIL,
                            f"task {task_key!r} defines its own cluster (new_cluster)",
                            resource=f"resources.jobs.{job_key}",
                        )
                    )
        return findings

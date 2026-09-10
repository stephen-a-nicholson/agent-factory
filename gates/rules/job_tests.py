"""job_has_test (warn): each job task's spark_python_task entry point has
a matching test file under tests/. Best-effort and warn-only: this
project's actual test file names are not perfectly consistent (some
mirror just the module name, e.g. src/ingest/documents.py ->
tests/test_documents.py; others also carry the parent package, e.g.
src/evaluate/rag.py -> tests/test_evaluate_rag.py), so this rule accepts
either form rather than forcing a rename.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from gates.loader import resources_of_type
from gates.models import Finding, Rule, Severity

REPO_ROOT = Path(__file__).resolve().parents[2]


class JobHasTest(Rule):
    name = "job_has_test"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        findings: list[Finding] = []
        for job_key, job in resources_of_type(config, "jobs").items():
            for task in job.get("tasks", []) or []:
                python_file = task.get("spark_python_task", {}).get("python_file")
                if not python_file:
                    continue
                repo_relative = _repo_relative_path(python_file)
                if repo_relative is None:
                    continue
                if not _has_matching_test(repo_relative):
                    findings.append(
                        Finding(
                            self.name,
                            Severity.WARN,
                            f"no test file found for {repo_relative} "
                            f"(task {task.get('task_key', '?')!r})",
                            resource=f"resources.jobs.{job_key}",
                        )
                    )
        return findings


def _repo_relative_path(python_file: str) -> str | None:
    """A spark_python_task's python_file is either a repo-relative path
    (as written in factory/generate.py, e.g. "../../src/ingest/documents.py")
    or, once resolved by `bundle validate`/`bundle plan`, a workspace path
    ending in the same repo-relative path after a "/files/" segment.
    Returns None if neither form matches (e.g. a notebook path)."""
    match = re.search(r"/files/(src/.+\.py)$", python_file)
    if match:
        return match.group(1)
    normalised = python_file.lstrip("./")
    if normalised.startswith("src/") and normalised.endswith(".py"):
        return normalised
    return None


def _has_matching_test(repo_relative_src_path: str) -> bool:
    src_path = Path(repo_relative_src_path)
    module_name = src_path.stem
    parent_name = src_path.parent.name
    candidates = [
        REPO_ROOT / "tests" / f"test_{module_name}.py",
        REPO_ROOT / "tests" / f"test_{parent_name}_{module_name}.py",
    ]
    return any(c.exists() for c in candidates)

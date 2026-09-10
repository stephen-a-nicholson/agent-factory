"""Moves the champion alias to the version currently aliased candidate,
and repoints the serving endpoint at it.

Runs as a Databricks job (spark_python_task, serverless), as the task
right after evaluate_<name> in the same job (see
_evaluate_rag_job_resource in factory/generate.py). The generator makes
this task depend_on the evaluate task, so a task-level dependency (not
code in this file) is what actually stops champion from moving when
evaluation fails: a failed evaluate task means this task is skipped
entirely, never runs, and never even sees a chance to move the alias.
This file has no failure-handling logic of its own because it doesn't
need any.

promote() is a plain function, directly testable against a fake MLflow
client; main() and update_serving_endpoint need a live Unity Catalog
registry and workspace respectively, so they are exercised by actually
deploying and running the job, per docs/PLAN.md notes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import mlflow

# See src/ingest/structured.py for why this sys.path bootstrap has to run
# before the sibling-package import that follows: this job script runs
# against a synced copy of the whole repo (unlike rag_agent.py, which
# never does, see its module docstring), so the import itself is fine,
# but Databricks execs the file with no package context, so the repo root
# still needs to be on sys.path by hand.
try:
    _REPO_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    _REPO_ROOT = Path(sys.argv[0]).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.agent.deploy import build_endpoint_name, update_serving_endpoint  # noqa: E402


def promote(client, registered_model_name: str) -> str:
    """Moves the champion alias to whatever version is currently aliased
    candidate, and returns that version number."""
    candidate = client.get_model_version_by_alias(registered_model_name, "candidate")
    client.set_registered_model_alias(registered_model_name, "champion", candidate.version)
    return candidate.version


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, help="Domain name, e.g. f1")
    parser.add_argument("--catalog", required=True, help="Catalog holding the domain's schema")
    parser.add_argument(
        "--schema", required=True, help="Destination schema (resolved, may be dev-prefixed)"
    )
    parser.add_argument(
        "--target",
        required=True,
        help="Bundle target (dev, test, prod); must match the value deploy_agent used",
    )
    args = parser.parse_args()

    mlflow.set_registry_uri("databricks-uc")
    registered_model_name = f"{args.catalog}.{args.schema}.rag_agent"
    client = mlflow.MlflowClient()
    version = promote(client, registered_model_name)

    endpoint_name = build_endpoint_name(args.domain, args.target)
    update_serving_endpoint(endpoint_name, registered_model_name, version)

    print(f"promoted {registered_model_name} version {version} to champion")
    print(f"serving endpoint {endpoint_name} now points at version {version}")


if __name__ == "__main__":
    main()

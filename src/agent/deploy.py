"""Logs a domain's RAG agent, registers it in Unity Catalog, and points
the serving endpoint at the new version.

Runs as a Databricks job (spark_python_task, serverless; no Spark APIs
used, just a convenient way to run a Python file as a job task).
Config-building and pure logic are plain functions, directly testable;
log_and_register_agent and update_serving_endpoint need a live MLflow
tracking server and a live Databricks workspace respectively, so they are
exercised by actually deploying and running the job, per docs/PLAN.md
notes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import mlflow
import yaml
from mlflow.models.resources import (
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
    DatabricksVectorSearchIndex,
    Resource,
)


def _default_repo_root() -> Path:
    """See src/ingest/structured.py for why this can't be a module-level
    Path(__file__) constant."""
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(sys.argv[0]).resolve().parents[2]


def load_rag_config(domain_yml_path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(domain_yml_path.read_text())
    return raw["rag"]


def load_structured_table_names(domain_yml_path: Path) -> list[str]:
    raw = yaml.safe_load(domain_yml_path.read_text())
    return [table["name"] for table in raw["structured"]["tables"]]


def build_model_config(
    rag: dict[str, Any],
    vector_search_index_name: str,
    genie_space_id: str | None,
) -> dict[str, Any]:
    retriever = rag.get("retriever", {})
    genie_enabled = rag.get("tools", {}).get("genie", False)
    return {
        "llm_endpoint": rag["llm"],
        "system_prompt": rag.get("system_prompt", ""),
        "top_k": retriever.get("top_k", 6),
        "filter_columns": retriever.get("filter_columns", []),
        "vector_search_index": vector_search_index_name,
        "genie_space_id": genie_space_id if genie_enabled else None,
    }


def build_resources(
    rag: dict[str, Any],
    vector_search_index_name: str,
    genie_space_id: str | None,
    warehouse_id: str | None = None,
    genie_table_names: list[str] | None = None,
) -> list[Resource]:
    """Declares every Databricks resource the served agent needs, so
    MLflow can set up automatic auth passthrough for each. Declaring only
    DatabricksGenieSpace is not enough for the Genie tool to actually
    work: confirmed by a real deploy where the space's own conversation
    API call succeeded (auth for that one call was fine) but every query
    Genie then tried to run failed with PERMISSION_DENIED, "No access to"
    every one of the domain's tables. The served identity separately needs
    the warehouse and the underlying tables declared too. See
    docs/PLAN.md notes, phase 2."""
    resources: list[Resource] = [
        DatabricksServingEndpoint(endpoint_name=rag["llm"]),
        DatabricksVectorSearchIndex(index_name=vector_search_index_name),
    ]
    if rag.get("tools", {}).get("genie", False) and genie_space_id:
        resources.append(DatabricksGenieSpace(genie_space_id=genie_space_id))
        if warehouse_id:
            resources.append(DatabricksSQLWarehouse(warehouse_id=warehouse_id))
        for table_name in genie_table_names or []:
            resources.append(DatabricksTable(table_name=table_name))
    return resources


def build_experiment_path(domain: str) -> str:
    """A fixed, user-independent workspace path: a spark_python_task has
    no implicit MLflow experiment context the way an interactive notebook
    run does (confirmed by a real deploy failing with "Could not find
    experiment with ID None" on a bare mlflow.start_run()), and a shared
    path avoids depending on which user or service principal happens to
    run the job. mlflow.set_experiment creates it on first use."""
    return f"/Shared/agent-factory/{domain}"


def log_and_register_agent(
    agent_file: Path,
    model_config: dict[str, Any],
    resources: list[Resource],
    registered_model_name: str,
    experiment_path: str,
) -> str:
    """Logs the RAG agent with MLflow's "models from code" pattern and
    registers a new version in Unity Catalog. Returns the new version
    number as a string."""
    from databricks.sdk import WorkspaceClient

    mlflow.set_registry_uri("databricks-uc")
    # set_experiment only creates the leaf experiment, not parent
    # workspace directories (confirmed by a real deploy failing with
    # "Parent directory does not exist: /Shared/agent-factory").
    WorkspaceClient().workspace.mkdirs(str(Path(experiment_path).parent))
    mlflow.set_experiment(experiment_path)
    input_example = {"input": [{"role": "user", "content": "What is the minimum car weight?"}]}
    with mlflow.start_run():
        logged_agent_info = mlflow.pyfunc.log_model(
            python_model=str(agent_file),
            name="agent",
            input_example=input_example,
            model_config=model_config,
            resources=resources,
        )
    model_version = mlflow.register_model(
        model_uri=logged_agent_info.model_uri, name=registered_model_name
    )
    return model_version.version


def build_endpoint_name(domain: str, target: str) -> str:
    """Workspace-unique serving endpoint name: dev/test/prod may share a
    single workspace (see docs/PLAN.md phase 0 notes on Free Edition), so
    the target has to disambiguate the name here rather than relying on
    DAB's own dev-mode name prefixing, which does not apply since there is
    no bundle-declared resource for this endpoint. See
    _deploy_agent_job_resource in factory/generate.py."""
    return f"{domain}-agent-{target}"


def set_alias(registered_model_name: str, alias: str, version: str) -> None:
    client = mlflow.MlflowClient()
    client.set_registered_model_alias(registered_model_name, alias, version)


def update_serving_endpoint(endpoint_name: str, registered_model_name: str, version: str) -> None:
    """Points endpoint_name's served entity at version, creating the
    endpoint if it doesn't exist yet.

    This job owns the endpoint entirely rather than the bundle declaring a
    model_serving_endpoints resource for it: Databricks serving endpoints
    only accept a numeric entity_version, never an alias, and
    factory/generate.py has no live workspace access at generate time so
    it can never know the real latest version number (a documented DAB
    limitation), and a real deploy hit a genuine circular dependency
    trying to declare one anyway. See this module's docstring in
    _deploy_agent_job_resource (factory/generate.py) and docs/PLAN.md
    notes, phase 2.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    served_entities = [
        ServedEntityInput(
            entity_name=registered_model_name,
            entity_version=version,
            scale_to_zero_enabled=True,
            workload_size="Small",
        )
    ]

    client = WorkspaceClient()
    try:
        client.serving_endpoints.get(endpoint_name)
    except NotFound:
        client.serving_endpoints.create_and_wait(
            name=endpoint_name,
            config=EndpointCoreConfigInput(name=endpoint_name, served_entities=served_entities),
        )
    else:
        client.serving_endpoints.update_config_and_wait(
            name=endpoint_name, served_entities=served_entities
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, help="Domain name, e.g. f1")
    parser.add_argument("--catalog", required=True, help="Catalog holding the domain's schema")
    parser.add_argument(
        "--schema", required=True, help="Destination schema (resolved, may be dev-prefixed)"
    )
    parser.add_argument("--index-name", required=True, help="Fully qualified vector index name")
    parser.add_argument(
        "--target",
        required=True,
        help=(
            "Bundle target (dev, test, prod), used to make the serving endpoint name "
            "workspace-unique since targets may share one workspace"
        ),
    )
    parser.add_argument(
        "--genie-space-id", default=None, help="Genie space ID, if the RAG agent uses it as a tool"
    )
    parser.add_argument(
        "--warehouse-id",
        default=None,
        help=(
            "SQL warehouse ID, for Genie tool auth passthrough (needed alongside --genie-space-id)"
        ),
    )
    args = parser.parse_args()

    repo_root = _default_repo_root()
    domain_dir = repo_root / "domains" / args.domain
    domain_yml_path = domain_dir / "domain.yml"
    rag = load_rag_config(domain_yml_path)
    genie_table_names = [
        f"{args.catalog}.{args.schema}.{name}"
        for name in load_structured_table_names(domain_yml_path)
    ]

    model_config = build_model_config(rag, args.index_name, args.genie_space_id)
    resources = build_resources(
        rag, args.index_name, args.genie_space_id, args.warehouse_id, genie_table_names
    )
    registered_model_name = f"{args.catalog}.{args.schema}.rag_agent"
    endpoint_name = build_endpoint_name(args.domain, args.target)

    agent_file = repo_root / "src" / "agent" / "rag_agent.py"
    experiment_path = build_experiment_path(args.domain)
    version = log_and_register_agent(
        agent_file, model_config, resources, registered_model_name, experiment_path
    )
    set_alias(registered_model_name, "candidate", version)
    update_serving_endpoint(endpoint_name, registered_model_name, version)

    print(f"registered {registered_model_name} version {version}, alias candidate")
    print(f"serving endpoint {endpoint_name} now points at version {version}")


if __name__ == "__main__":
    main()

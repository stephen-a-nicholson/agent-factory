from pathlib import Path

from mlflow.models.resources import (
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
    DatabricksVectorSearchIndex,
)

from src.agent.deploy import (
    build_endpoint_name,
    build_experiment_path,
    build_model_config,
    build_resources,
    load_rag_config,
    load_structured_table_names,
)

RAG_WITH_GENIE = {
    "enabled": True,
    "llm": "databricks-meta-llama-3-3-70b-instruct",
    "system_prompt": "You answer questions about regulations.",
    "retriever": {"top_k": 4, "filter_columns": ["category"]},
    "tools": {"genie": True},
    "eval": {"dataset": "eval/rag.jsonl"},
}

RAG_WITHOUT_GENIE = {
    "enabled": True,
    "llm": "databricks-meta-llama-3-3-70b-instruct",
    "eval": {"dataset": "eval/rag.jsonl"},
}


def test_load_rag_config(tmp_path: Path):
    domain_yml = tmp_path / "domain.yml"
    domain_yml.write_text(
        """
name: f1
display_name: F1
description: test
owner: a@b.com
structured:
  loader: parquet_folder
  source: data/
  tables:
    - name: t
      primary_key: id
rag:
  enabled: true
  llm: databricks-meta-llama-3-3-70b-instruct
  eval:
    dataset: eval/rag.jsonl
"""
    )
    rag = load_rag_config(domain_yml)
    assert rag["llm"] == "databricks-meta-llama-3-3-70b-instruct"


def test_build_model_config_with_genie():
    config = build_model_config(RAG_WITH_GENIE, "cat.schema.f1_chunks", genie_space_id="abc123")
    assert config == {
        "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
        "system_prompt": "You answer questions about regulations.",
        "top_k": 4,
        "filter_columns": ["category"],
        "vector_search_index": "cat.schema.f1_chunks",
        "genie_space_id": "abc123",
    }


def test_build_model_config_without_genie_tool_omits_space_id_even_if_passed():
    config = build_model_config(RAG_WITHOUT_GENIE, "cat.schema.f1_chunks", genie_space_id="abc123")
    assert config["genie_space_id"] is None
    assert config["top_k"] == 6
    assert config["filter_columns"] == []


def test_build_resources_with_genie_but_no_warehouse_or_tables():
    # Declaring only the Genie space is not enough for the tool to work
    # (see build_resources' docstring), but callers can still omit
    # warehouse_id/genie_table_names, e.g. before those are known.
    resources = build_resources(RAG_WITH_GENIE, "cat.schema.f1_chunks", genie_space_id="abc123")
    types = [type(r) for r in resources]
    assert types == [DatabricksServingEndpoint, DatabricksVectorSearchIndex, DatabricksGenieSpace]


def test_build_resources_with_genie_warehouse_and_tables():
    resources = build_resources(
        RAG_WITH_GENIE,
        "cat.schema.f1_chunks",
        genie_space_id="abc123",
        warehouse_id="wh123",
        genie_table_names=["cat.schema.races", "cat.schema.results"],
    )
    types = [type(r) for r in resources]
    assert types == [
        DatabricksServingEndpoint,
        DatabricksVectorSearchIndex,
        DatabricksGenieSpace,
        DatabricksSQLWarehouse,
        DatabricksTable,
        DatabricksTable,
    ]
    table_resources = [r for r in resources if isinstance(r, DatabricksTable)]
    assert [r.name for r in table_resources] == ["cat.schema.races", "cat.schema.results"]


def test_build_resources_without_genie():
    resources = build_resources(
        RAG_WITHOUT_GENIE,
        "cat.schema.f1_chunks",
        genie_space_id="abc123",
        warehouse_id="wh123",
        genie_table_names=["cat.schema.races"],
    )
    types = [type(r) for r in resources]
    assert types == [DatabricksServingEndpoint, DatabricksVectorSearchIndex]


def test_build_resources_genie_enabled_but_no_space_id():
    rag = {**RAG_WITH_GENIE}
    resources = build_resources(
        rag, "cat.schema.f1_chunks", genie_space_id=None, warehouse_id="wh123"
    )
    types = [type(r) for r in resources]
    assert types == [DatabricksServingEndpoint, DatabricksVectorSearchIndex]


def test_load_structured_table_names(tmp_path: Path):
    domain_yml = tmp_path / "domain.yml"
    domain_yml.write_text(
        """
name: f1
display_name: F1
description: test
owner: a@b.com
structured:
  loader: parquet_folder
  source: data/
  tables:
    - name: races
      primary_key: race_id
    - name: results
      primary_key: [race_id, driver_id]
"""
    )
    assert load_structured_table_names(domain_yml) == ["races", "results"]


def test_build_endpoint_name_includes_target_for_workspace_uniqueness():
    assert build_endpoint_name("f1", "dev") == "f1-agent-dev"
    assert build_endpoint_name("f1", "prod") == "f1-agent-prod"


def test_build_experiment_path_is_user_independent():
    assert build_experiment_path("f1") == "/Shared/agent-factory/f1"

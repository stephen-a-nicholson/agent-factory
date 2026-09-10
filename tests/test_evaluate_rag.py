from pathlib import Path
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from mlflow.genai.scorers import Correctness, RelevanceToQuery, RetrievalGroundedness
from pyspark.sql import SparkSession

from src.evaluate.rag import (
    EvalOutcome,
    build_scorers,
    check_thresholds,
    extract_final_text,
    extract_metric_scores,
    load_eval_dataset,
    load_rag_config,
    write_results,
)

CATALOG = "spark_catalog"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-evaluate-rag")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


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
    judges: [correctness]
    promotion_threshold:
      correctness: 0.8
"""
    )
    rag = load_rag_config(domain_yml)
    assert rag["eval"]["judges"] == ["correctness"]
    assert rag["eval"]["promotion_threshold"] == {"correctness": 0.8}


def test_load_eval_dataset(tmp_path: Path):
    dataset_path = tmp_path / "rag.jsonl"
    dataset_path.write_text(
        '{"id": "a", "question": "What is the cost cap?", "category": "financial", '
        '"expected_response": "The cost cap limits team spending."}\n'
        '{"id": "b", "question": "What is the minimum weight?", "category": "technical", '
        '"expected_response": "The car must meet a minimum weight."}\n'
    )
    rows = load_eval_dataset(dataset_path)
    assert rows == [
        {
            "inputs": {"question": "What is the cost cap?"},
            "expectations": {"expected_response": "The cost cap limits team spending."},
        },
        {
            "inputs": {"question": "What is the minimum weight?"},
            "expectations": {"expected_response": "The car must meet a minimum weight."},
        },
    ]


def test_build_scorers_known_judges():
    scorers = build_scorers(["correctness", "groundedness", "relevance"])
    assert [type(s) for s in scorers] == [Correctness, RetrievalGroundedness, RelevanceToQuery]


def test_build_scorers_unknown_judge_raises():
    with pytest.raises(ValueError, match="unknown judge"):
        build_scorers(["made_up_judge"])


def test_extract_final_text():
    response = {
        "output": [
            {"type": "function_call", "name": "search_regulations"},
            {
                "type": "message",
                "content": [{"type": "output_text", "text": "The cost cap is $135 million."}],
            },
        ]
    }
    assert extract_final_text(response) == "The cost cap is $135 million."


def test_extract_final_text_no_message():
    assert extract_final_text({"output": []}) == ""


def test_extract_metric_scores():
    metrics = {
        "correctness/mean": 0.8,
        "retrieval_groundedness/mean": 0.9,
        "relevance_to_query/mean": 1.0,
        "some_other_metric/mean": 0.5,
    }
    scores = extract_metric_scores(metrics, ["correctness", "groundedness", "relevance"])
    assert scores == {"correctness": 0.8, "groundedness": 0.9, "relevance": 1.0}


def test_extract_metric_scores_missing_metric_omitted():
    scores = extract_metric_scores({"correctness/mean": 0.8}, ["correctness", "groundedness"])
    assert scores == {"correctness": 0.8}


def test_check_thresholds_all_pass():
    outcome = check_thresholds(
        {"correctness": 0.85, "groundedness": 0.9}, {"correctness": 0.8, "groundedness": 0.85}
    )
    assert outcome.passed is True
    assert outcome.failures == {}


def test_check_thresholds_one_fails():
    outcome = check_thresholds(
        {"correctness": 0.5, "groundedness": 0.9}, {"correctness": 0.8, "groundedness": 0.85}
    )
    assert outcome.passed is False
    assert outcome.failures == {"correctness": (0.5, 0.8)}


def test_check_thresholds_missing_score_counts_as_failure():
    outcome = check_thresholds({"groundedness": 0.9}, {"correctness": 0.8})
    assert outcome.passed is False
    assert "correctness" in outcome.failures


def test_check_thresholds_ignores_judges_without_a_threshold():
    # "relevance" has a score but no configured threshold: informational
    # only, shouldn't gate promotion.
    outcome = check_thresholds({"correctness": 0.9, "relevance": 0.1}, {"correctness": 0.8})
    assert outcome.passed is True


def test_eval_outcome_passed_property():
    assert EvalOutcome(scores={}, failures={}).passed is True
    assert EvalOutcome(scores={}, failures={"correctness": (0.5, 0.8)}).passed is False


def test_write_results(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.rag_eval_test")
    table = f"{CATALOG}.rag_eval_test.eval_results"

    write_results(
        spark,
        {"correctness": 0.9, "groundedness": 0.85},
        domain="f1",
        target="dev",
        git_sha="abc123",
        table=table,
    )

    rows = {r.metric: r.score for r in spark.sql(f"SELECT * FROM {table}").collect()}
    assert rows["rag_eval.correctness"] == 0.9
    assert rows["rag_eval.groundedness"] == 0.85
    domains = {r.domain for r in spark.sql(f"SELECT * FROM {table}").collect()}
    assert domains == {"f1"}


def test_build_predict_fn_calls_model_and_extracts_text():
    from src.evaluate.rag import build_predict_fn

    model = MagicMock()
    model.predict.return_value = {
        "output": [{"type": "message", "content": [{"type": "output_text", "text": "answer"}]}]
    }
    predict_fn = build_predict_fn(model)

    result = predict_fn("What is the cost cap?")

    assert result == "answer"
    model.predict.assert_called_once_with(
        {"input": [{"role": "user", "content": "What is the cost cap?"}]}
    )

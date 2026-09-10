from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.evaluate.genie import (
    Benchmark,
    evaluate_answer,
    load_benchmarks,
    run_benchmarks,
    write_results,
)

CATALOG = "spark_catalog"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-evaluate-genie")
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


def _text_message(content: str):
    """A stand-in for the databricks-sdk GenieMessage shape: attachments is
    a list of objects with a `.text` that may be None, and `.text.content`
    when present."""
    return SimpleNamespace(attachments=[SimpleNamespace(text=SimpleNamespace(content=content))])


def test_load_benchmarks(tmp_path: Path):
    path = tmp_path / "genie.jsonl"
    path.write_text(
        '{"id": "a", "question": "Q1?", "answer_contains": [["x", "y"]]}\n'
        '{"id": "b", "question": "Q2?", "answer_contains": [["z"]]}\n'
    )
    benchmarks = load_benchmarks(path)
    assert benchmarks == [
        Benchmark(id="a", question="Q1?", answer_contains=[["x", "y"]]),
        Benchmark(id="b", question="Q2?", answer_contains=[["z"]]),
    ]


@pytest.mark.parametrize(
    ("answer", "checks", "expected"),
    [
        ("Max Verstappen won", [["verstappen"]], True),
        ("max_verstappen won", [["verstappen"]], True),
        ("Lando Norris won", [["verstappen"]], False),
        ("Red Bull scored 790 points", [["red bull", "red_bull"], ["790"]], True),
        ("red_bull scored 790 points", [["red bull", "red_bull"], ["790"]], True),
        ("red_bull scored 500 points", [["red bull", "red_bull"], ["790"]], False),
        ("", [["anything"]], False),
    ],
)
def test_evaluate_answer(answer: str, checks: list[list[str]], expected: bool):
    assert evaluate_answer(answer, checks) is expected


def test_run_benchmarks_calls_genie_and_extracts_text():
    client = MagicMock()
    client.genie.start_conversation_and_wait.return_value = _text_message(
        "The answer is verstappen with 43 wins"
    )
    benchmarks = [
        Benchmark(id="q1", question="Who won most?", answer_contains=[["verstappen"], ["43"]])
    ]

    results = run_benchmarks(client, "space-123", benchmarks)

    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].benchmark_id == "q1"
    client.genie.start_conversation_and_wait.assert_called_once_with("space-123", "Who won most?")


def test_run_benchmarks_marks_failure_when_answer_missing_fact():
    client = MagicMock()
    client.genie.start_conversation_and_wait.return_value = _text_message("norris won")
    benchmarks = [Benchmark(id="q1", question="Who won?", answer_contains=[["verstappen"]])]

    results = run_benchmarks(client, "space-123", benchmarks)

    assert results[0].passed is False


def test_run_benchmarks_handles_no_text_attachment():
    client = MagicMock()
    client.genie.start_conversation_and_wait.return_value = SimpleNamespace(attachments=[])
    benchmarks = [Benchmark(id="q1", question="Who won?", answer_contains=[["verstappen"]])]

    results = run_benchmarks(client, "space-123", benchmarks)

    assert results[0].passed is False
    assert results[0].answer_text == ""


def test_write_results_writes_per_question_and_pass_rate_rows(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.agent_factory")
    table = f"{CATALOG}.agent_factory.eval_results"
    from src.evaluate.genie import BenchmarkResult

    results = [
        BenchmarkResult(benchmark_id="q1", question="Q1?", passed=True, answer_text="a"),
        BenchmarkResult(benchmark_id="q2", question="Q2?", passed=False, answer_text="b"),
    ]

    write_results(spark, results, domain="f1", target="dev", git_sha="abc123", table=table)

    rows = {r.metric: r.score for r in spark.sql(f"SELECT * FROM {table}").collect()}
    assert rows["genie_benchmark.q1"] == 1.0
    assert rows["genie_benchmark.q2"] == 0.0
    assert rows["genie_benchmark_pass_rate"] == pytest.approx(0.5)

    domains = {r.domain for r in spark.sql(f"SELECT * FROM {table}").collect()}
    assert domains == {"f1"}

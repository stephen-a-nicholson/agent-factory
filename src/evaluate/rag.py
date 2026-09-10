"""Runs a domain's RAG eval dataset through MLflow GenAI evaluation and
records the result against domain.yml's promotion thresholds.

Runs as a Databricks job (spark_python_task, serverless; no Spark APIs
used directly, just a convenient way to run a Python file as a job task
alongside the rest of this factory's jobs). Loads the domain's rag_agent
model at the `candidate` alias (the version deploy_agent_<name> just
registered) and evaluates it with the judges configured in domain.yml.
Exits non-zero if any judge with a configured threshold scores below it,
so the job — and the promote task that depends on it succeeding, see
factory/generate.py — fails, and the champion alias never moves.

Dataset loading and scorer/metric-name mapping are plain functions,
directly testable; run_evaluation needs a live MLflow tracking server and
a registered model with warehouse/vector-search/Genie access to actually
run, so it is exercised by actually deploying and running the job, per
docs/PLAN.md notes.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import mlflow
import yaml
from mlflow.genai.scorers import Correctness, RelevanceToQuery, RetrievalGroundedness
from pyspark.sql import Row, SparkSession

# Maps domain.yml's rag.eval.judges names (and rag.eval.promotion_threshold
# keys) to the mlflow.genai.scorers class that implements them, and to the
# metric key mlflow.genai.evaluate()'s result.metrics dict actually uses
# for it (confirmed against a real evaluate() call: scorer class names and
# their result.metrics keys don't always match, e.g. "relevance" here is
# RelevanceToQuery, whose metric key is "relevance_to_query/mean", not
# "relevance/mean"). See docs/PLAN.md notes, phase 3.
SCORER_BUILDERS = {
    "correctness": Correctness,
    "groundedness": RetrievalGroundedness,
    "relevance": RelevanceToQuery,
}
METRIC_KEYS = {
    "correctness": "correctness/mean",
    "groundedness": "retrieval_groundedness/mean",
    "relevance": "relevance_to_query/mean",
}


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


def load_eval_dataset(dataset_path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in dataset_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        raw = json.loads(line)
        rows.append(
            {
                "inputs": {"question": raw["question"]},
                "expectations": {"expected_response": raw["expected_response"]},
            }
        )
    return rows


def build_scorers(judges: list[str]) -> list:
    scorers = []
    for judge in judges:
        builder = SCORER_BUILDERS.get(judge)
        if builder is None:
            raise ValueError(
                f"unknown judge {judge!r} in domain.yml rag.eval.judges; "
                f"expected one of {sorted(SCORER_BUILDERS)}"
            )
        scorers.append(builder(model="databricks"))
    return scorers


def extract_final_text(response: dict[str, Any]) -> str:
    for item in response.get("output", []):
        if item.get("type") == "message":
            for block in item.get("content", []):
                if block.get("type") == "output_text":
                    return block.get("text", "")
    return ""


def build_predict_fn(model):
    def predict_fn(question: str) -> str:
        response = model.predict({"input": [{"role": "user", "content": question}]})
        return extract_final_text(response)

    return predict_fn


def extract_metric_scores(metrics: dict[str, Any], judges: list[str]) -> dict[str, float]:
    """Pulls this run's mean score out of mlflow.genai.evaluate()'s
    result.metrics for each configured judge, keyed by the same short
    names domain.yml's rag.eval.judges and promotion_threshold use."""
    scores = {}
    for judge in judges:
        key = METRIC_KEYS[judge]
        if key in metrics:
            scores[judge] = float(metrics[key])
    return scores


@dataclass(frozen=True)
class EvalOutcome:
    scores: dict[str, float]
    failures: dict[str, tuple[float, float]]  # judge -> (actual, threshold)

    @property
    def passed(self) -> bool:
        return not self.failures


def check_thresholds(
    scores: dict[str, float], promotion_threshold: dict[str, float]
) -> EvalOutcome:
    failures = {}
    for judge, threshold in promotion_threshold.items():
        actual = scores.get(judge)
        if actual is None or actual < threshold:
            failures[judge] = (actual if actual is not None else float("nan"), threshold)
    return EvalOutcome(scores=scores, failures=failures)


def write_results(
    spark: SparkSession,
    scores: dict[str, float],
    domain: str,
    target: str,
    git_sha: str,
    table: str,
) -> None:
    timestamp = datetime.now(UTC)
    rows = [
        Row(
            domain=domain,
            target=target,
            git_sha=git_sha,
            metric=f"rag_eval.{judge}",
            score=score,
            timestamp=timestamp,
        )
        for judge, score in scores.items()
    ]
    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(table)


def run_evaluation(
    model,
    dataset: list[dict[str, Any]],
    judges: list[str],
    experiment_path: str,
) -> dict[str, float]:
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_path)
    scorers = build_scorers(judges)
    predict_fn = build_predict_fn(model)
    result = mlflow.genai.evaluate(data=dataset, predict_fn=predict_fn, scorers=scorers)
    return extract_metric_scores(result.metrics, judges)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, help="Domain name, e.g. f1")
    parser.add_argument("--catalog", required=True, help="Catalog holding the domain's schema")
    parser.add_argument(
        "--schema", required=True, help="Destination schema (resolved, may be dev-prefixed)"
    )
    parser.add_argument(
        "--eval-table",
        required=True,
        help="Fully qualified eval_results table (resolved, schema may be dev-prefixed)",
    )
    parser.add_argument("--target", required=True, help="Bundle target (dev, test, prod)")
    parser.add_argument(
        "--git-sha",
        default=None,
        help="Commit SHA to record with results; set by CI (phase 5), 'unknown' otherwise",
    )
    args = parser.parse_args()

    repo_root = _default_repo_root()
    domain_dir = repo_root / "domains" / args.domain
    rag = load_rag_config(domain_dir / "domain.yml")
    dataset = load_eval_dataset(domain_dir / rag["eval"]["dataset"])

    mlflow.set_registry_uri("databricks-uc")
    registered_model_name = f"{args.catalog}.{args.schema}.rag_agent"
    model = mlflow.pyfunc.load_model(f"models:/{registered_model_name}@candidate")

    experiment_path = f"/Shared/agent-factory/{args.domain}-eval"
    scores = run_evaluation(model, dataset, rag["eval"]["judges"], experiment_path)

    spark = SparkSession.builder.getOrCreate()
    write_results(
        spark,
        scores,
        domain=args.domain,
        target=args.target,
        git_sha=args.git_sha or "unknown",
        table=args.eval_table,
    )

    outcome = check_thresholds(scores, rag["eval"]["promotion_threshold"])
    for judge, score in scores.items():
        threshold = rag["eval"]["promotion_threshold"].get(judge)
        status = "no threshold" if threshold is None else ("PASS" if score >= threshold else "FAIL")
        print(f"{judge}: {score:.2%} ({status})")

    if not outcome.passed:
        details = ", ".join(
            f"{judge} {actual:.2%} < {threshold:.2%}"
            for judge, (actual, threshold) in outcome.failures.items()
        )
        raise SystemExit(f"rag eval failed promotion thresholds: {details}")


if __name__ == "__main__":
    main()

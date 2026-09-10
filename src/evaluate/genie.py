"""Runs a domain's Genie benchmark questions and records pass/fail scores.

Runs as a Databricks job (spark_python_task, serverless). Calling Genie and
writing results are plain functions taking a client/SparkSession, directly
testable with pytest (the Genie client is mocked; the Delta write uses a
local PySpark session, same as src/ingest/structured.py); only main() is
Databricks-job plumbing.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from pyspark.sql import Row, SparkSession

DEFAULT_PASS_THRESHOLD = 0.8


@dataclass(frozen=True)
class Benchmark:
    id: str
    question: str
    answer_contains: list[list[str]]


@dataclass(frozen=True)
class BenchmarkResult:
    benchmark_id: str
    question: str
    passed: bool
    answer_text: str


def _default_repo_root() -> Path:
    """Databricks runs a spark_python_task by exec()-ing its source with
    __file__ unset, so this can't be a plain module-level `Path(__file__)`
    constant: that would raise NameError on import. sys.argv[0] carries
    the same real path there instead. See src/ingest/structured.py, which
    has the same fix."""
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(sys.argv[0]).resolve().parents[2]


def load_benchmarks(path: Path) -> list[Benchmark]:
    benchmarks = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        raw = json.loads(line)
        benchmarks.append(
            Benchmark(
                id=raw["id"],
                question=raw["question"],
                answer_contains=raw["answer_contains"],
            )
        )
    return benchmarks


def evaluate_answer(answer_text: str, answer_contains: list[list[str]]) -> bool:
    """answer_contains is an AND of OR-groups: every group must have at
    least one of its alternatives appear in the answer (case-insensitive),
    so a benchmark can accept both a raw ID ("red_bull") and a
    human-readable phrasing ("Red Bull") for the same fact."""
    lowered = answer_text.lower()
    return all(any(alt.lower() in lowered for alt in group) for group in answer_contains)


def _extract_answer_text(message) -> str:
    for attachment in message.attachments or []:
        if attachment.text is not None:
            return attachment.text.content or ""
    return ""


def run_benchmarks(
    client: WorkspaceClient, space_id: str, benchmarks: list[Benchmark]
) -> list[BenchmarkResult]:
    results = []
    for benchmark in benchmarks:
        message = client.genie.start_conversation_and_wait(space_id, benchmark.question)
        answer_text = _extract_answer_text(message)
        passed = evaluate_answer(answer_text, benchmark.answer_contains)
        results.append(
            BenchmarkResult(
                benchmark_id=benchmark.id,
                question=benchmark.question,
                passed=passed,
                answer_text=answer_text,
            )
        )
    return results


def write_results(
    spark: SparkSession,
    results: list[BenchmarkResult],
    domain: str,
    target: str,
    git_sha: str,
    table: str,
) -> None:
    timestamp = datetime.now(UTC)
    pass_rate = sum(r.passed for r in results) / len(results) if results else 0.0

    rows = [
        Row(
            domain=domain,
            target=target,
            git_sha=git_sha,
            metric=f"genie_benchmark.{r.benchmark_id}",
            score=1.0 if r.passed else 0.0,
            timestamp=timestamp,
        )
        for r in results
    ]
    rows.append(
        Row(
            domain=domain,
            target=target,
            git_sha=git_sha,
            metric="genie_benchmark_pass_rate",
            score=pass_rate,
            timestamp=timestamp,
        )
    )
    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(table)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, help="Domain name, e.g. f1")
    parser.add_argument("--space-id", required=True, help="Genie space ID to query")
    parser.add_argument(
        "--eval-table",
        required=True,
        help=(
            "Fully qualified eval_results table, resolved by the caller "
            "(e.g. via ${resources.schemas.agent_factory.name}) since its "
            "schema may be dev-mode prefixed like any other schema"
        ),
    )
    parser.add_argument("--target", required=True, help="Bundle target (dev, test, prod)")
    parser.add_argument(
        "--git-sha",
        default=os.environ.get("GIT_SHA", "unknown"),
        help="Commit SHA to record with results; set by CI (phase 5), 'unknown' otherwise",
    )
    parser.add_argument("--pass-threshold", type=float, default=DEFAULT_PASS_THRESHOLD)
    args = parser.parse_args()

    domain_dir = _default_repo_root() / "domains" / args.domain
    benchmarks = load_benchmarks(domain_dir / "eval" / "genie.jsonl")

    client = WorkspaceClient()
    results = run_benchmarks(client, args.space_id, benchmarks)

    spark = SparkSession.builder.getOrCreate()
    write_results(
        spark,
        results,
        domain=args.domain,
        target=args.target,
        git_sha=args.git_sha,
        table=args.eval_table,
    )

    pass_rate = sum(r.passed for r in results) / len(results) if results else 0.0
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"{status} {r.benchmark_id}: {r.question!r} -> {r.answer_text!r}")
    print(f"pass rate: {pass_rate:.2%} (threshold {args.pass_threshold:.0%})")

    if pass_rate < args.pass_threshold:
        raise SystemExit(
            f"genie benchmark pass rate {pass_rate:.2%} is below threshold "
            f"{args.pass_threshold:.0%}"
        )


if __name__ == "__main__":
    main()

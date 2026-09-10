"""Loads a domain's Parquet snapshots into Delta tables.

Runs as a Databricks job (spark_python_task, serverless). The loading
logic is plain functions taking a SparkSession, so it is directly testable
with pytest against a local PySpark session; only main() is Databricks-job
plumbing. Keep this file thin: see CLAUDE.md, "Keep notebooks thin."
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

# Databricks runs a spark_python_task by exec()-ing its source directly,
# with no package context and __file__ unset, so two things below need
# handling before the sibling-package import that follows: sys.argv[0]
# stands in for __file__, and the repo root has to be put on sys.path by
# hand for `from src.ingest.delta import ...` to resolve at all (locally,
# `uv sync`'s editable install makes src.* importable from anywhere, which
# is why this only surfaces once deployed). See docs/PLAN.md notes, phase 2.
try:
    _REPO_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    _REPO_ROOT = Path(sys.argv[0]).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingest.delta import write_delta_table  # noqa: E402


def _default_repo_root() -> Path:
    return _REPO_ROOT


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: list[str]
    description: str | None


def load_table_specs(domain_yml_path: Path) -> tuple[str, list[TableSpec]]:
    """Read structured.source and structured.tables straight out of
    domain.yml, without depending on factory.schema, so this job has no
    import-path dependency on the factory package at runtime."""
    raw = yaml.safe_load(domain_yml_path.read_text())
    structured = raw["structured"]
    specs = []
    for table in structured["tables"]:
        primary_key = table["primary_key"]
        if isinstance(primary_key, str):
            primary_key = [primary_key]
        specs.append(
            TableSpec(
                name=table["name"],
                primary_key=list(primary_key),
                description=table.get("description"),
            )
        )
    return structured["source"], specs


def ingest_table(
    spark: SparkSession,
    parquet_path: Path,
    catalog: str,
    schema: str,
    spec: TableSpec,
    apply_primary_key_constraint: bool = True,
) -> None:
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"expected a Parquet snapshot at {parquet_path}; run "
            f"factory/refresh_<domain>.py to generate it, or check "
            f"structured.source in domain.yml"
        )

    full_name = f"{catalog}.{schema}.{spec.name}"
    df = spark.read.parquet(str(parquet_path))
    write_delta_table(
        spark,
        df,
        full_name,
        table_name=spec.name,
        primary_key=spec.primary_key,
        comment=spec.description,
        apply_primary_key_constraint=apply_primary_key_constraint,
    )


def ingest_domain(
    spark: SparkSession,
    domain_name: str,
    catalog: str,
    schema: str,
    repo_root: Path | None = None,
    apply_primary_key_constraint: bool = True,
) -> None:
    domain_dir = (repo_root or _default_repo_root()) / "domains" / domain_name
    source, specs = load_table_specs(domain_dir / "domain.yml")
    data_dir = domain_dir / source
    for spec in specs:
        ingest_table(
            spark,
            data_dir / f"{spec.name}.parquet",
            catalog,
            schema,
            spec,
            apply_primary_key_constraint=apply_primary_key_constraint,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, help="Domain name, e.g. f1")
    parser.add_argument("--catalog", required=True, help="Destination Unity Catalog catalog")
    parser.add_argument(
        "--schema", required=True, help="Destination schema (resolved, may be dev-prefixed)"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    ingest_domain(spark, args.domain, args.catalog, args.schema)


if __name__ == "__main__":
    main()

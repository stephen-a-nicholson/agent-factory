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
from pyspark.sql import DataFrame, SparkSession


def _default_repo_root() -> Path:
    """Databricks runs a spark_python_task by exec()-ing its source with
    __file__ unset, so this can't be a plain module-level `Path(__file__)`
    constant: that would raise NameError on import, before main() even
    runs. sys.argv[0] carries the same real path there instead. Locally
    (pytest, `python -m src.ingest.structured`), __file__ works fine."""
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(sys.argv[0]).resolve().parents[2]


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


def _escape_sql_string(text: str) -> str:
    return text.replace("'", "''")


def _create_table_ddl(full_name: str, df: DataFrame, not_null_columns: list[str]) -> str:
    """A primary key constraint requires its columns to be declared NOT
    NULL. Writing df straight into a new table with `saveAsTable` doesn't
    reliably carry the DataFrame schema's nullable=False through to the
    stored table schema, so create the table explicitly from DDL first,
    with the primary key columns marked NOT NULL, then append into it."""
    not_null = set(not_null_columns)
    columns = [
        f"{f.name} {f.dataType.simpleString()}" + (" NOT NULL" if f.name in not_null else "")
        for f in df.schema.fields
    ]
    return f"CREATE TABLE {full_name} ({', '.join(columns)}) USING DELTA"


def primary_key_ddl(full_name: str, spec: TableSpec) -> list[str]:
    """The DDL that declares spec's primary key as an informational
    constraint on full_name. Unity-Catalog-only SQL (ADD CONSTRAINT ...
    PRIMARY KEY ... NOT ENFORCED isn't part of the open-source Spark SQL
    grammar at all), so this can only run against a real Databricks
    workspace; see the apply_primary_key_constraint flag on ingest_table
    for how that's kept testable locally."""
    constraint_name = f"{spec.name}_pk"
    primary_key_cols = ", ".join(spec.primary_key)
    return [
        f"ALTER TABLE {full_name} DROP CONSTRAINT IF EXISTS {constraint_name}",
        f"ALTER TABLE {full_name} ADD CONSTRAINT {constraint_name} "
        f"PRIMARY KEY ({primary_key_cols}) NOT ENFORCED",
    ]


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

    # Drop and recreate rather than overwrite: a plain mode("overwrite")
    # saveAsTable on an existing Delta V2 table requires TRUNCATE
    # capability that isn't resolved reliably across catalog
    # implementations, and this is a full-snapshot reload, not an
    # incremental merge, so there's nothing an overwrite would preserve.
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(_create_table_ddl(full_name, df, spec.primary_key))
    df.write.format("delta").mode("append").saveAsTable(full_name)

    if spec.description:
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{_escape_sql_string(spec.description)}'")

    if apply_primary_key_constraint:
        for statement in primary_key_ddl(full_name, spec):
            spark.sql(statement)


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

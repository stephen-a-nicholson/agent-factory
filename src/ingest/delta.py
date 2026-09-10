"""Shared Delta table-writing helpers for ingest jobs.

Used by both structured.py and documents.py so the Delta/Unity Catalog
quirks below are handled in exactly one place, discovered and documented
in docs/PLAN.md phase 1 notes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def escape_sql_string(text: str) -> str:
    return text.replace("'", "''")


def create_table_ddl(
    full_name: str,
    df: DataFrame,
    not_null_columns: list[str],
    table_properties: dict[str, str] | None = None,
) -> str:
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
    ddl = f"CREATE TABLE {full_name} ({', '.join(columns)}) USING DELTA"
    if table_properties:
        props = ", ".join(f"'{k}' = '{v}'" for k, v in table_properties.items())
        ddl += f" TBLPROPERTIES ({props})"
    return ddl


def primary_key_ddl(full_name: str, table_name: str, primary_key: list[str]) -> list[str]:
    """The DDL that declares primary_key as an informational constraint on
    full_name. Unity-Catalog-only SQL (ADD CONSTRAINT ... PRIMARY KEY ...
    NOT ENFORCED isn't part of the open-source Spark SQL grammar at all),
    so this can only run against a real Databricks workspace; see the
    apply_primary_key_constraint flag on write_delta_table for how that's
    kept testable locally."""
    constraint_name = f"{table_name}_pk"
    primary_key_cols = ", ".join(primary_key)
    return [
        f"ALTER TABLE {full_name} DROP CONSTRAINT IF EXISTS {constraint_name}",
        f"ALTER TABLE {full_name} ADD CONSTRAINT {constraint_name} "
        f"PRIMARY KEY ({primary_key_cols}) NOT ENFORCED",
    ]


def write_delta_table(
    spark: SparkSession,
    df: DataFrame,
    full_name: str,
    table_name: str,
    primary_key: list[str],
    comment: str | None = None,
    apply_primary_key_constraint: bool = True,
    table_properties: dict[str, str] | None = None,
) -> None:
    """Write df as a fresh managed Delta table at full_name, with primary_key
    columns NOT NULL and, unless disabled, an informational PRIMARY KEY
    constraint.

    Drops and recreates rather than overwriting: on the OSS Delta/Spark
    used for local tests, a plain mode("overwrite").saveAsTable() on an
    existing Delta V2 table requires TRUNCATE capability that isn't
    resolved reliably, and every caller here is doing a full-snapshot
    reload, not an incremental merge, so there's nothing an overwrite
    would preserve anyway — usually. The one caller for which that's not
    true is documents.py's chunks table: see
    overwrite_delta_table_in_place below for why that one specifically
    must never be dropped and recreated.

    table_properties is for callers like documents.py that need
    'delta.enableChangeDataFeed': 'true' — a Databricks Vector Search
    delta-sync index refuses to be created against a source table without
    it ("Source table ... is not a valid Vector Search source"), confirmed
    against a real deploy. See docs/PLAN.md notes, phase 2.
    """
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(create_table_ddl(full_name, df, primary_key, table_properties))
    df.write.format("delta").mode("append").saveAsTable(full_name)

    if comment:
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{escape_sql_string(comment)}'")

    if apply_primary_key_constraint:
        for statement in primary_key_ddl(full_name, table_name, primary_key):
            spark.sql(statement)


def overwrite_delta_table_in_place(
    spark: SparkSession,
    df: DataFrame,
    full_name: str,
    table_name: str,
    primary_key: list[str],
    comment: str | None = None,
    apply_primary_key_constraint: bool = True,
    table_properties: dict[str, str] | None = None,
) -> None:
    """Like write_delta_table, but overwrites an existing table's data in
    place rather than dropping and recreating it, preserving the table's
    identity.

    Needed specifically for a table with a downstream Delta-sync
    streaming consumer — a vector search index tracks its source table
    by internal UUID via a streaming checkpoint, not by name, and drop-
    and-recreate changes that UUID on every ingest run. Confirmed by a
    real vector search sync failing repeatedly with
    DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE ("It used to read from
    another Delta table according to checkpoint... you deleted and
    re-created a table") after documents.py re-ingested into a
    write_delta_table-managed chunks table. See docs/PLAN.md notes,
    phase 3. Only relevant on real Databricks: this needs a table that
    already has a NOT NULL primary key and a streaming consumer attached,
    neither of which meaningfully exists in local OSS-Delta tests, so
    only the "table doesn't exist yet" branch is exercised there
    (equivalent to write_delta_table); the true in-place overwrite branch
    is exercised by actually deploying and running the job.
    """
    if not spark.catalog.tableExists(full_name):
        write_delta_table(
            spark,
            df,
            full_name,
            table_name,
            primary_key,
            comment,
            apply_primary_key_constraint,
            table_properties,
        )
        return

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        full_name
    )
    # A plain overwrite doesn't reliably preserve NOT NULL/comment/
    # constraints (see write_delta_table's own NOT NULL note above), so
    # reapply them every time rather than assume they survived.
    for column in primary_key:
        spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN {column} SET NOT NULL")
    if comment:
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{escape_sql_string(comment)}'")
    if apply_primary_key_constraint:
        for statement in primary_key_ddl(full_name, table_name, primary_key):
            spark.sql(statement)

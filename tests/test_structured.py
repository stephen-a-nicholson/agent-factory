from pathlib import Path

import pandas as pd
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingest.structured import (
    TableSpec,
    _create_table_ddl,
    ingest_domain,
    ingest_table,
    load_table_specs,
    primary_key_ddl,
)

CATALOG = "spark_catalog"  # the only catalog name a plain local Spark session resolves


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # An isolated warehouse dir per test session: a shared spark-warehouse/
    # under the repo would leak tables between separate `pytest` runs and
    # make failures order-dependent (a table created by an earlier run can
    # still exist, in a different format, on the next one).
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-structured")
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


@pytest.fixture
def fake_domain(tmp_path: Path) -> Path:
    domain_dir = tmp_path / "domains" / "widgets"
    data_dir = domain_dir / "data"
    data_dir.mkdir(parents=True)

    (domain_dir / "domain.yml").write_text(
        """
name: widgets
display_name: Widgets
description: test domain
owner: test@example.com
structured:
  loader: parquet_folder
  source: data/
  tables:
    - name: items
      primary_key: item_id
      description: One row per item
    - name: orders
      primary_key: [order_id, item_id]
"""
    )

    pd.DataFrame({"item_id": [1, 2, 3], "name": ["a", "b", "c"]}).to_parquet(
        data_dir / "items.parquet", index=False
    )
    pd.DataFrame({"order_id": [10, 10, 11], "item_id": [1, 2, 1], "qty": [5, 1, 2]}).to_parquet(
        data_dir / "orders.parquet", index=False
    )

    return tmp_path


def test_load_table_specs(fake_domain: Path):
    source, specs = load_table_specs(fake_domain / "domains" / "widgets" / "domain.yml")
    assert source == "data/"
    assert specs == [
        TableSpec(name="items", primary_key=["item_id"], description="One row per item"),
        TableSpec(name="orders", primary_key=["order_id", "item_id"], description=None),
    ]


def test_primary_key_ddl():
    spec = TableSpec(name="items", primary_key=["order_id", "item_id"], description=None)
    statements = primary_key_ddl("cat.schema.items", spec)
    assert statements == [
        "ALTER TABLE cat.schema.items DROP CONSTRAINT IF EXISTS items_pk",
        "ALTER TABLE cat.schema.items ADD CONSTRAINT items_pk "
        "PRIMARY KEY (order_id, item_id) NOT ENFORCED",
    ]


def test_create_table_ddl_marks_primary_key_not_null(spark, fake_domain: Path):
    df = spark.read.parquet(str(fake_domain / "domains" / "widgets" / "data" / "orders.parquet"))
    ddl = _create_table_ddl("cat.schema.orders", df, ["order_id", "item_id"])
    assert ddl == (
        "CREATE TABLE cat.schema.orders "
        "(order_id bigint NOT NULL, item_id bigint NOT NULL, qty bigint) USING DELTA"
    )


def test_ingest_table_writes_data_comment_and_not_null_schema(spark, fake_domain: Path):
    # apply_primary_key_constraint=False: ADD CONSTRAINT ... PRIMARY KEY is
    # a Unity Catalog SQL extension the open-source Spark/Delta used for
    # local tests can't even parse. primary_key_ddl() above covers the SQL
    # this skips; the real thing is exercised by actually deploying and
    # running the job, see docs/PLAN.md notes.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.widgets")
    spec = TableSpec(name="items", primary_key=["item_id"], description="One row per item")
    ingest_table(
        spark,
        fake_domain / "domains" / "widgets" / "data" / "items.parquet",
        catalog=CATALOG,
        schema="widgets",
        spec=spec,
        apply_primary_key_constraint=False,
    )

    rows = spark.sql(f"SELECT * FROM {CATALOG}.widgets.items ORDER BY item_id").collect()
    assert [r.item_id for r in rows] == [1, 2, 3]

    table = spark.catalog.getTable(f"{CATALOG}.widgets.items")
    assert table.name == "items"
    item_id_field = spark.table(f"{CATALOG}.widgets.items").schema["item_id"]
    assert item_id_field.nullable is False

    comment = spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.widgets.items").collect()
    assert any(row.col_name == "Comment" and row.data_type == "One row per item" for row in comment)


def test_ingest_table_missing_parquet_raises(spark, fake_domain: Path):
    spec = TableSpec(name="missing", primary_key=["id"], description=None)
    with pytest.raises(FileNotFoundError):
        ingest_table(
            spark,
            fake_domain / "domains" / "widgets" / "data" / "missing.parquet",
            catalog=CATALOG,
            schema="widgets",
            spec=spec,
        )


def test_ingest_domain_loads_every_table(spark, fake_domain: Path):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.widgets2")
    ingest_domain(
        spark,
        "widgets",
        catalog=CATALOG,
        schema="widgets2",
        repo_root=fake_domain,
        apply_primary_key_constraint=False,
    )

    assert spark.sql(f"SELECT count(*) AS n FROM {CATALOG}.widgets2.items").collect()[0].n == 3
    assert spark.sql(f"SELECT count(*) AS n FROM {CATALOG}.widgets2.orders").collect()[0].n == 3

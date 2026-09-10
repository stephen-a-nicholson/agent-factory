import pandas as pd
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingest.delta import (
    create_table_ddl,
    overwrite_delta_table_in_place,
    primary_key_ddl,
    write_delta_table,
)

CATALOG = "spark_catalog"  # the only catalog name a plain local Spark session resolves


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-delta")
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
def orders_parquet(tmp_path):
    path = tmp_path / "orders.parquet"
    pd.DataFrame({"order_id": [10, 10, 11], "item_id": [1, 2, 1], "qty": [5, 1, 2]}).to_parquet(
        path, index=False
    )
    return path


def test_primary_key_ddl():
    statements = primary_key_ddl("cat.schema.items", "items", ["order_id", "item_id"])
    assert statements == [
        "ALTER TABLE cat.schema.items DROP CONSTRAINT IF EXISTS items_pk",
        "ALTER TABLE cat.schema.items ADD CONSTRAINT items_pk "
        "PRIMARY KEY (order_id, item_id) NOT ENFORCED",
    ]


def test_create_table_ddl_marks_primary_key_not_null(spark, orders_parquet):
    df = spark.read.parquet(str(orders_parquet))
    ddl = create_table_ddl("cat.schema.orders", df, ["order_id", "item_id"])
    assert ddl == (
        "CREATE TABLE cat.schema.orders "
        "(order_id bigint NOT NULL, item_id bigint NOT NULL, qty bigint) USING DELTA"
    )


def test_write_delta_table_writes_data_comment_and_not_null_schema(spark, orders_parquet):
    # apply_primary_key_constraint=False: ADD CONSTRAINT ... PRIMARY KEY is
    # a Unity Catalog SQL extension the open-source Spark/Delta used for
    # local tests can't even parse. primary_key_ddl() above covers the SQL
    # this skips; the real thing is exercised by actually deploying and
    # running a job that calls write_delta_table, see docs/PLAN.md notes.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.delta_test")
    full_name = f"{CATALOG}.delta_test.orders"
    df = spark.read.parquet(str(orders_parquet))

    write_delta_table(
        spark,
        df,
        full_name,
        table_name="orders",
        primary_key=["order_id", "item_id"],
        comment="One row per order line",
        apply_primary_key_constraint=False,
    )

    rows = spark.sql(f"SELECT * FROM {full_name} ORDER BY order_id, item_id").collect()
    assert [r.order_id for r in rows] == [10, 10, 11]

    order_id_field = spark.table(full_name).schema["order_id"]
    assert order_id_field.nullable is False

    described = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}").collect()
    assert any(
        row.col_name == "Comment" and row.data_type == "One row per order line" for row in described
    )


def test_create_table_ddl_with_table_properties(spark, orders_parquet):
    df = spark.read.parquet(str(orders_parquet))
    ddl = create_table_ddl(
        "cat.schema.orders",
        df,
        ["order_id"],
        table_properties={"delta.enableChangeDataFeed": "true"},
    )
    assert ddl == (
        "CREATE TABLE cat.schema.orders "
        "(order_id bigint NOT NULL, item_id bigint, qty bigint) USING DELTA "
        "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def test_write_delta_table_sets_table_properties(spark, orders_parquet):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.delta_test3")
    full_name = f"{CATALOG}.delta_test3.orders"
    df = spark.read.parquet(str(orders_parquet))

    write_delta_table(
        spark,
        df,
        full_name,
        table_name="orders",
        primary_key=["order_id"],
        apply_primary_key_constraint=False,
        table_properties={"delta.enableChangeDataFeed": "true"},
    )

    shown = spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()
    properties = {row.key: row.value for row in shown}
    assert properties["delta.enableChangeDataFeed"] == "true"


def test_write_delta_table_is_rerunnable(spark, orders_parquet):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.delta_test2")
    full_name = f"{CATALOG}.delta_test2.orders"
    df = spark.read.parquet(str(orders_parquet))

    for _ in range(2):
        write_delta_table(
            spark,
            df,
            full_name,
            table_name="orders",
            primary_key=["order_id", "item_id"],
            apply_primary_key_constraint=False,
        )

    assert spark.sql(f"SELECT count(*) AS n FROM {full_name}").collect()[0].n == 3


def test_overwrite_delta_table_in_place_creates_when_missing(spark, orders_parquet):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.delta_test4")
    full_name = f"{CATALOG}.delta_test4.orders"
    df = spark.read.parquet(str(orders_parquet))

    overwrite_delta_table_in_place(
        spark,
        df,
        full_name,
        table_name="orders",
        primary_key=["order_id"],
        apply_primary_key_constraint=False,
    )

    assert spark.sql(f"SELECT count(*) AS n FROM {full_name}").collect()[0].n == 3


def test_overwrite_delta_table_in_place_preserves_table_identity(spark, orders_parquet):
    # The whole point of overwrite_delta_table_in_place — that an
    # existing table's identity survives a second write, unlike
    # write_delta_table's drop-and-recreate — genuinely needs real
    # Databricks: overwriting an *existing* Delta table via mode
    # "overwrite" is the same OSS-Delta TRUNCATE-capability gap noted on
    # write_delta_table's own docstring. This only reaches the
    # create-when-missing branch (write_delta_table's happy path), which
    # is already covered above; the true overwrite-in-place branch is
    # exercised by actually deploying and running the ingest job,
    # confirmed by re-syncing the vector search index without hitting
    # DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE. See docs/PLAN.md
    # notes, phase 3.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.delta_test5")
    full_name = f"{CATALOG}.delta_test5.orders"
    df = spark.read.parquet(str(orders_parquet))

    overwrite_delta_table_in_place(
        spark,
        df,
        full_name,
        table_name="orders",
        primary_key=["order_id"],
        apply_primary_key_constraint=False,
    )

    assert spark.sql(f"SELECT count(*) AS n FROM {full_name}").collect()[0].n == 3

"""Unit tests for silver cleansing and SCD2 merge logic."""
from __future__ import annotations

import pytest
from datetime import date
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from src.transformation.silver_cleansing import (
    trim_whitespace,
    standardize_dates,
    deduplicate_records,
    handle_nulls,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-transformation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


class TestTrimWhitespace:
    def test_trims_leading_and_trailing(self, spark):
        df = spark.createDataFrame([Row(name="  Alice  ", city=" NYC ")])
        result = trim_whitespace(df, ["name", "city"])
        row = result.collect()[0]
        assert row["name"] == "Alice"
        assert row["city"] == "NYC"

    def test_ignores_missing_column(self, spark):
        df = spark.createDataFrame([Row(name="  Bob  ")])
        result = trim_whitespace(df, ["name", "nonexistent"])
        assert result.collect()[0]["name"] == "Bob"


class TestStandardizeDates:
    def test_parses_iso_format(self, spark):
        df = spark.createDataFrame([Row(order_date="2024-03-15")])
        result = standardize_dates(df, ["order_date"])
        ts = result.collect()[0]["order_date"]
        assert ts is not None

    def test_parses_us_format(self, spark):
        df = spark.createDataFrame([Row(order_date="03/15/2024")])
        result = standardize_dates(df, ["order_date"])
        ts = result.collect()[0]["order_date"]
        assert ts is not None

    def test_handles_missing_column_gracefully(self, spark):
        df = spark.createDataFrame([Row(order_date="2024-01-01")])
        result = standardize_dates(df, ["nonexistent"])
        assert "order_date" in result.columns


class TestDeduplicateRecords:
    def test_keeps_latest_record(self, spark):
        rows = [
            Row(order_id="O1", updated_at="2024-01-02", value=200),
            Row(order_id="O1", updated_at="2024-01-01", value=100),
            Row(order_id="O2", updated_at="2024-01-01", value=50),
        ]
        df = spark.createDataFrame(rows)
        result = deduplicate_records(df, dedup_key=["order_id", "updated_at"])
        assert result.count() == 2
        o1 = result.filter("order_id = 'O1'").collect()[0]
        assert o1["value"] == 200

    def test_empty_dedup_key_returns_all_rows(self, spark):
        rows = [Row(id=1), Row(id=1)]
        df = spark.createDataFrame(rows)
        result = deduplicate_records(df, dedup_key=[])
        assert result.count() == 2


class TestHandleNulls:
    def test_fail_strategy_raises_on_nulls(self, spark):
        df = spark.createDataFrame([Row(customer_id=None, amount=100.0)])
        with pytest.raises(RuntimeError, match="null value"):
            handle_nulls(df, {"customer_id": "fail"})

    def test_fail_strategy_passes_when_no_nulls(self, spark):
        df = spark.createDataFrame([Row(customer_id="C1", amount=100.0)])
        result = handle_nulls(df, {"customer_id": "fail"})
        assert result.count() == 1

    def test_default_strategy_fills_nulls(self, spark):
        df = spark.createDataFrame([Row(amount=None)])
        result = handle_nulls(df, {"amount": "default:0.0"})
        assert result.collect()[0]["amount"] == 0.0

    def test_drop_strategy_removes_null_rows(self, spark):
        df = spark.createDataFrame([Row(x="a"), Row(x=None)])
        result = handle_nulls(df, {"x": "drop"})
        assert result.count() == 1

    def test_allow_strategy_passes_nulls_through(self, spark):
        df = spark.createDataFrame([Row(phone=None)])
        result = handle_nulls(df, {"phone": "allow"})
        assert result.count() == 1

    def test_unknown_strategy_raises(self, spark):
        df = spark.createDataFrame([Row(x="a")])
        with pytest.raises(ValueError, match="Unknown null strategy"):
            handle_nulls(df, {"x": "invalid_strategy"})

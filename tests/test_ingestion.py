"""Unit tests for ingestion modules using a local PySpark session."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.ingestion.autoloader_ingest import configure_autoloader, _default_glob
from src.ingestion.schema_inference import _compare_schemas


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


class TestAutoLoaderConfig:
    def test_configure_autoloader_returns_reader(self, spark, tmp_path):
        schema_loc = str(tmp_path / "schema")
        source_path = str(tmp_path / "source")
        (tmp_path / "source").mkdir()

        reader = configure_autoloader(
            spark=spark,
            source_path=source_path,
            source_format="json",
            schema_location=schema_loc,
        )
        assert reader is not None

    def test_unsupported_format_raises(self, spark, tmp_path):
        with pytest.raises(ValueError, match="Unsupported source format"):
            configure_autoloader(
                spark=spark,
                source_path=str(tmp_path),
                source_format="xlsx",
                schema_location=str(tmp_path / "schema"),
            )

    @pytest.mark.parametrize("fmt,expected_glob", [
        ("json",    "*.json"),
        ("csv",     "*.csv"),
        ("parquet", "*.parquet"),
        ("avro",    "*.avro"),
        ("text",    "*.txt"),
    ])
    def test_default_glob_patterns(self, fmt, expected_glob):
        assert _default_glob(fmt) == expected_glob


class TestSchemaInference:
    def test_no_drift_when_schemas_match(self):
        schema = StructType([
            StructField("id",     IntegerType(), True),
            StructField("name",   StringType(),  True),
            StructField("amount", DoubleType(),  True),
        ])
        result = _compare_schemas(schema, schema)
        assert result == {}

    def test_detects_missing_column(self):
        expected = StructType([
            StructField("id",   IntegerType(), True),
            StructField("name", StringType(),  True),
        ])
        actual = StructType([
            StructField("id", IntegerType(), True),
        ])
        result = _compare_schemas(expected, actual)
        assert "missing_columns" in result
        assert "name" in result["missing_columns"]

    def test_detects_added_column(self):
        expected = StructType([StructField("id", IntegerType(), True)])
        actual = StructType([
            StructField("id",    IntegerType(), True),
            StructField("extra", StringType(),  True),
        ])
        result = _compare_schemas(expected, actual)
        assert "added_columns" in result
        assert "extra" in result["added_columns"]

    def test_detects_type_change(self):
        expected = StructType([StructField("amount", IntegerType(), True)])
        actual   = StructType([StructField("amount", DoubleType(),  True)])
        result = _compare_schemas(expected, actual)
        assert "type_changes" in result
        assert "amount" in result["type_changes"]

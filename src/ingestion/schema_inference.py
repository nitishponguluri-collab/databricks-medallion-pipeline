from __future__ import annotations

from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from loguru import logger


@dataclass
class SchemaInferenceResult:
    inferred_schema: StructType
    has_drift: bool = False
    drift_details: dict = field(default_factory=dict)


def infer_and_validate_schema(
    spark: SparkSession,
    sample_path: str,
    source_format: str,
    schema_location: str,
    expected_schema: StructType | None = None,
) -> SchemaInferenceResult:
    """Infer schema from a sample of files and optionally compare against an expected schema.

    Args:
        spark: Active SparkSession.
        sample_path: Cloud path containing sample files to infer schema from.
        source_format: File format ("json", "csv", "parquet", etc.).
        schema_location: Where the previously persisted schema is stored (Auto Loader location).
        expected_schema: Optional StructType to compare against.  Drift is flagged when
                         the inferred schema differs from this baseline.

    Returns:
        SchemaInferenceResult with the inferred schema and any drift details.
    """
    logger.info(f"Inferring schema from {sample_path} (format={source_format})")

    read_opts: dict = {}
    if source_format == "csv":
        read_opts = {"header": "true", "inferSchema": "true"}

    inferred = (
        spark.read.format(source_format)
        .options(**read_opts)
        .load(sample_path)
        .schema
    )

    result = SchemaInferenceResult(inferred_schema=inferred)

    if expected_schema is not None:
        result.drift_details = _compare_schemas(expected_schema, inferred)
        result.has_drift = bool(result.drift_details)
        if result.has_drift:
            logger.warning(f"Schema drift detected: {result.drift_details}")
        else:
            logger.info("Schema validation passed — no drift detected.")
    else:
        logger.info("No expected schema provided; skipping drift comparison.")

    return result


def _compare_schemas(expected: StructType, actual: StructType) -> dict:
    expected_fields = {f.name: f.dataType for f in expected.fields}
    actual_fields   = {f.name: f.dataType for f in actual.fields}

    missing  = {k: str(v) for k, v in expected_fields.items() if k not in actual_fields}
    added    = {k: str(v) for k, v in actual_fields.items() if k not in expected_fields}
    type_changed = {
        k: {"expected": str(expected_fields[k]), "actual": str(actual_fields[k])}
        for k in expected_fields
        if k in actual_fields and expected_fields[k] != actual_fields[k]
    }

    drift: dict = {}
    if missing:
        drift["missing_columns"] = missing
    if added:
        drift["added_columns"] = added
    if type_changed:
        drift["type_changes"] = type_changed
    return drift

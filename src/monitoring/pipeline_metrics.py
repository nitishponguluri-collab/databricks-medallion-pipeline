from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from loguru import logger

_METRICS_TABLE_DEFAULT = "medallion_prod.monitoring.pipeline_metrics"


def track_pipeline_run(
    spark: SparkSession,
    pipeline_name: str,
    layer: str,
    row_count: int,
    duration_seconds: float,
    target_table: str,
    status: str = "success",
    error_message: str | None = None,
    metrics_table: str = _METRICS_TABLE_DEFAULT,
) -> None:
    """Append a pipeline run record to the monitoring Delta table.

    Creates the monitoring table automatically on first use.

    Args:
        spark:            Active SparkSession.
        pipeline_name:    Identifier for the pipeline stage (e.g., 'bronze_ingestion').
        layer:            Medallion layer ('bronze', 'silver', 'gold', 'quality').
        row_count:        Number of records processed.
        duration_seconds: Wall-clock seconds for the pipeline stage.
        target_table:     Fully qualified name of the table that was written to.
        status:           'success' or 'failure'.
        error_message:    Exception message string on failure.
        metrics_table:    Destination monitoring table (overridable per environment).
    """
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    record = Row(
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer=layer,
        target_table=target_table,
        row_count=row_count,
        duration_seconds=round(duration_seconds, 3),
        status=status,
        error_message=error_message,
        recorded_at=now,
    )

    metrics_df = spark.createDataFrame([record])

    try:
        _ensure_metrics_table(spark, metrics_table)
        metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)
        logger.info(
            f"Metric recorded | run_id={run_id} pipeline={pipeline_name} "
            f"layer={layer} rows={row_count} duration={duration_seconds:.1f}s status={status}"
        )
    except Exception as exc:
        # Metric write failures must never break the main pipeline
        logger.error(f"Failed to write pipeline metric: {exc}")


def get_recent_runs(
    spark: SparkSession,
    pipeline_name: str | None = None,
    limit: int = 50,
    metrics_table: str = _METRICS_TABLE_DEFAULT,
):
    """Return recent pipeline run metrics as a DataFrame."""
    df = spark.table(metrics_table).orderBy("recorded_at", ascending=False)
    if pipeline_name:
        df = df.filter(f"pipeline_name = '{pipeline_name}'")
    return df.limit(limit)


def _ensure_metrics_table(spark: SparkSession, metrics_table: str) -> None:
    try:
        spark.table(metrics_table)
    except Exception:
        logger.info(f"Creating monitoring table: {metrics_table}")
        catalog, schema, table = metrics_table.split(".")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {metrics_table} (
                run_id         STRING,
                pipeline_name  STRING,
                layer          STRING,
                target_table   STRING,
                row_count      BIGINT,
                duration_seconds DOUBLE,
                status         STRING,
                error_message  STRING,
                recorded_at    STRING
            )
            USING DELTA
            COMMENT 'Medallion pipeline run metrics'
        """)

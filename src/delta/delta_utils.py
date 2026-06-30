from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from loguru import logger


def get_table_history(spark: SparkSession, table_name: str, limit: int = 10) -> DataFrame:
    """Return the Delta transaction log history for a table.

    Args:
        spark:      Active SparkSession.
        table_name: Fully qualified table name.
        limit:      Maximum number of history entries to return.

    Returns:
        DataFrame with one row per Delta operation.
    """
    logger.info(f"Fetching history for {table_name} (limit={limit})")
    return spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")


def time_travel_query(
    spark: SparkSession,
    table_name: str,
    version: int | None = None,
    timestamp: str | None = None,
) -> DataFrame:
    """Read a historical snapshot of a Delta table via time travel.

    Args:
        spark:      Active SparkSession.
        table_name: Fully qualified table name.
        version:    Delta version number (mutually exclusive with timestamp).
        timestamp:  ISO-8601 timestamp string (e.g. '2024-01-15T08:00:00').

    Returns:
        DataFrame representing the table at the requested point in time.
    """
    if version is not None:
        logger.info(f"Time-travel read: {table_name} @ version={version}")
        return spark.read.format("delta").option("versionAsOf", version).table(table_name)
    elif timestamp is not None:
        logger.info(f"Time-travel read: {table_name} @ timestamp={timestamp}")
        return spark.read.format("delta").option("timestampAsOf", timestamp).table(table_name)
    else:
        raise ValueError("Provide either version or timestamp for time travel.")


def table_size_metrics(spark: SparkSession, table_name: str) -> dict:
    """Return size and file-count metrics for a Delta table.

    Args:
        spark:      Active SparkSession.
        table_name: Fully qualified table name.

    Returns:
        Dictionary with keys: row_count, num_files, size_bytes, size_gb.
    """
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    row_count = spark.table(table_name).count()
    size_bytes = detail["sizeInBytes"]

    metrics = {
        "table": table_name,
        "row_count": row_count,
        "num_files": detail["numFiles"],
        "size_bytes": size_bytes,
        "size_gb": round(size_bytes / (1024 ** 3), 4),
    }
    logger.info(f"Table metrics for {table_name}: {metrics}")
    return metrics


def compact_small_files(spark: SparkSession, table_name: str, target_file_size_mb: int = 128) -> None:
    """Run a targeted OPTIMIZE to merge small files into the target size.

    This is a lightweight compaction without Z-Ordering — use optimize_zorder.py
    when column ordering for query pruning is also needed.
    """
    logger.info(f"Compacting {table_name} (target file size ≈ {target_file_size_mb} MB)")
    spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", f"{target_file_size_mb}m")
    spark.sql(f"OPTIMIZE {table_name}")
    logger.info(f"Compaction complete for {table_name}")

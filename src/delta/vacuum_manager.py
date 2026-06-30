from __future__ import annotations

import time
from pyspark.sql import SparkSession
from loguru import logger


_MINIMUM_SAFE_RETENTION_HOURS = 168  # Delta default: 7 days


def vacuum_table(
    spark: SparkSession,
    table_name: str,
    retention_hours: int = _MINIMUM_SAFE_RETENTION_HOURS,
    dry_run: bool = True,
) -> dict:
    """Remove obsolete Delta files that are no longer referenced and older than retention_hours.

    Safety rules:
      - Dry-run mode is ON by default.  Pass dry_run=False to actually delete files.
      - Retention cannot be set below 168 hours (7 days) unless the Spark conf
        spark.databricks.delta.retentionDurationCheck.enabled is explicitly disabled —
        this function refuses to do that and raises ValueError instead.

    Args:
        spark:           Active SparkSession.
        table_name:      Fully qualified Delta table name.
        retention_hours: Minimum age of files to delete (default: 168h / 7 days).
        dry_run:         If True, log what WOULD be deleted but take no action.

    Returns:
        Dict summarising the operation: mode, files_to_remove, elapsed_seconds.
    """
    if retention_hours < _MINIMUM_SAFE_RETENTION_HOURS:
        raise ValueError(
            f"retention_hours={retention_hours} is below the safe minimum of "
            f"{_MINIMUM_SAFE_RETENTION_HOURS}h.  Reducing retention risks data loss "
            "for active streaming readers and time-travel queries."
        )

    mode_label = "DRY RUN" if dry_run else "EXECUTE"
    logger.info(f"VACUUM {mode_label} | {table_name} | retention={retention_hours}h")

    t0 = time.time()

    if dry_run:
        result_df = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
        files_to_remove = result_df.count()
        logger.info(f"Dry run: {files_to_remove} file(s) would be removed from {table_name}")

        if files_to_remove > 0:
            logger.info("Sample files that would be removed:")
            result_df.show(20, truncate=False)

        elapsed = round(time.time() - t0, 2)
        return {
            "table": table_name,
            "mode": "dry_run",
            "retention_hours": retention_hours,
            "files_to_remove": files_to_remove,
            "elapsed_seconds": elapsed,
        }
    else:
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        elapsed = round(time.time() - t0, 2)
        logger.info(f"VACUUM complete for {table_name} in {elapsed}s")
        return {
            "table": table_name,
            "mode": "execute",
            "retention_hours": retention_hours,
            "elapsed_seconds": elapsed,
        }

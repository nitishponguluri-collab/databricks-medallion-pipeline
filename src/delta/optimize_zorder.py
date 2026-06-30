from __future__ import annotations

import time
from pyspark.sql import SparkSession
from loguru import logger

from delta.delta_utils import table_size_metrics


def optimize_table(
    spark: SparkSession,
    table_name: str,
    zorder_columns: list[str],
    where_clause: str | None = None,
) -> dict:
    """Run OPTIMIZE with ZORDER BY on a Delta table and report before/after metrics.

    ZORDER co-locates related data in the same set of files, dramatically improving
    query performance for high-cardinality filter columns (e.g., customer_id, order_date).

    Args:
        spark:          Active SparkSession.
        table_name:     Fully qualified Delta table name.
        zorder_columns: Columns to include in the ZORDER BY clause (max 32 recommended).
        where_clause:   Optional partition predicate to scope the OPTIMIZE
                        (e.g., "order_date >= '2024-01-01'").

    Returns:
        Dict with before/after file counts and sizes, and elapsed time.
    """
    if not zorder_columns:
        logger.warning(f"No ZORDER columns specified for {table_name} — running plain OPTIMIZE.")

    before = table_size_metrics(spark, table_name)
    logger.info(f"OPTIMIZE start | {table_name} | files_before={before['num_files']} size_before={before['size_gb']:.4f} GB")

    zorder_clause = f"ZORDER BY ({', '.join(zorder_columns)})" if zorder_columns else ""
    where_part = f"WHERE {where_clause}" if where_clause else ""

    sql = f"OPTIMIZE {table_name} {where_part} {zorder_clause}".strip()
    logger.debug(f"Executing: {sql}")

    t0 = time.time()
    spark.sql(sql)
    elapsed = round(time.time() - t0, 2)

    after = table_size_metrics(spark, table_name)

    result = {
        "table": table_name,
        "zorder_columns": zorder_columns,
        "files_before": before["num_files"],
        "files_after": after["num_files"],
        "files_removed": before["num_files"] - after["num_files"],
        "size_gb_before": before["size_gb"],
        "size_gb_after": after["size_gb"],
        "size_reduction_pct": round(
            (1 - after["size_gb"] / before["size_gb"]) * 100 if before["size_gb"] > 0 else 0, 2
        ),
        "elapsed_seconds": elapsed,
    }

    logger.info(
        f"OPTIMIZE complete | {table_name} | "
        f"files {result['files_before']} → {result['files_after']} "
        f"({result['files_removed']} removed) | "
        f"size {result['size_gb_before']:.4f} → {result['size_gb_after']:.4f} GB "
        f"({result['size_reduction_pct']}% reduction) | "
        f"{elapsed}s elapsed"
    )
    return result

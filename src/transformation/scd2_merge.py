from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from delta.tables import DeltaTable
from loguru import logger


def scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    business_key_cols: list[str],
    effective_date_col: str = "effective_date",
    end_date_col: str = "end_date",
    is_current_col: str = "is_current",
    surrogate_key_col: str = "sk_id",
    end_date_sentinel: str = "9999-12-31",
) -> None:
    """Implement SCD Type 2 merge into a Delta table.

    For each incoming record whose business-key already exists in the target:
      - If any non-key attribute has changed → close the existing current row
        (set end_date and is_current=false) and insert the new version.
      - If nothing changed → no-op.

    New business keys are inserted as new current rows.

    Args:
        spark:               Active SparkSession.
        source_df:           Incoming (already cleansed and deduped) DataFrame.
        target_table:        Fully qualified Delta table name (catalog.schema.table).
        business_key_cols:   Columns that uniquely identify a business entity.
        effective_date_col:  Column name for the row start date.
        end_date_col:        Column name for the row end date.
        is_current_col:      Boolean/byte flag for the currently active row.
        surrogate_key_col:   Surrogate primary key generated via md5 hash.
        end_date_sentinel:   Sentinel date representing 'open' rows (9999-12-31).
    """
    now = F.current_timestamp()
    sentinel = F.to_date(F.lit(end_date_sentinel), "yyyy-MM-dd")

    # Build a hash of all non-key, non-metadata columns to detect changes efficiently
    non_key_cols = [c for c in source_df.columns if c not in business_key_cols]
    change_hash_expr = F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in non_key_cols]))

    staged = (
        source_df
        .withColumn("_change_hash", change_hash_expr)
        .withColumn(effective_date_col, F.coalesce(F.col(effective_date_col), now))
        .withColumn(end_date_col, sentinel)
        .withColumn(is_current_col, F.lit(True))
        .withColumn(
            surrogate_key_col,
            F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in business_key_cols], F.col(effective_date_col).cast("string")))
        )
    )

    # Ensure target table exists
    if not _table_exists(spark, target_table):
        logger.info(f"Target table {target_table} not found — creating from source schema.")
        staged.drop("_change_hash").write.format("delta").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)

    join_condition = " AND ".join([f"target.{c} = source.{c}" for c in business_key_cols])
    full_condition = f"({join_condition}) AND target.{is_current_col} = true"

    (
        target.alias("target")
        .merge(staged.alias("source"), full_condition)
        # Close stale current rows when a business-key match has a changed hash
        .whenMatchedUpdate(
            condition="source._change_hash <> target._change_hash",
            set={
                end_date_col:    "source." + effective_date_col,
                is_current_col:  "false",
            },
        )
        # Insert the new version (including brand-new business keys)
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Insert the new version of changed records (the UPDATE above only closed old rows)
    changed = (
        staged.alias("source")
        .join(
            spark.table(target_table).filter(f"{is_current_col} = false").alias("target"),
            on=business_key_cols,
        )
        .select([F.col(f"source.{c}") for c in staged.columns if c != "_change_hash"])
    )

    if changed.count() > 0:
        (
            changed
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )
        logger.info(f"SCD2: inserted {changed.count()} new versions into {target_table}")

    logger.info(f"SCD2 merge complete on {target_table}")


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False

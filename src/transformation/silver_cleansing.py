from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from loguru import logger


def trim_whitespace(df: DataFrame, columns: list[str]) -> DataFrame:
    """Strip leading/trailing whitespace from the specified string columns."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
    return df


def standardize_dates(df: DataFrame, date_columns: list[str]) -> DataFrame:
    """Cast date/timestamp columns to TimestampType using common ISO and US formats."""
    date_formats = ["yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyy-MM-dd HH:mm:ss"]
    for col in date_columns:
        if col not in df.columns:
            continue
        parsed = None
        for fmt in date_formats:
            attempt = F.to_timestamp(F.col(col), fmt)
            parsed = attempt if parsed is None else F.coalesce(parsed, attempt)
        df = df.withColumn(col, parsed)
    return df


def deduplicate_records(df: DataFrame, dedup_key: list[str]) -> DataFrame:
    """Retain only the latest record per dedup_key ordered by the last element (usually a timestamp).

    Assumes the last column in dedup_key is the ordering field (e.g., updated_at).
    """
    if not dedup_key:
        return df

    order_col = dedup_key[-1]
    partition_cols = dedup_key[:-1] if len(dedup_key) > 1 else dedup_key

    from pyspark.sql.window import Window

    window = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    deduped = (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter("_row_num = 1")
        .drop("_row_num")
    )
    original_count = df.count()
    deduped_count = deduped.count()
    logger.info(f"Deduplicated {original_count} → {deduped_count} records (removed {original_count - deduped_count})")
    return deduped


def handle_nulls(df: DataFrame, null_strategies: dict[str, str]) -> DataFrame:
    """Apply per-column null strategies.

    Strategy format:
      - "fail"              → raises RuntimeError if any nulls found
      - "allow"             → do nothing (pass through)
      - "default:<value>"   → fill nulls with the specified default value
      - "drop"              → drop rows where this column is null
    """
    for col_name, strategy in null_strategies.items():
        if col_name not in df.columns:
            continue

        null_count = df.filter(F.col(col_name).isNull()).count()

        if strategy == "fail":
            if null_count > 0:
                raise RuntimeError(
                    f"Data quality failure: column '{col_name}' has {null_count} null value(s). Strategy=fail."
                )

        elif strategy == "allow":
            pass

        elif strategy == "drop":
            before = df.count()
            df = df.filter(F.col(col_name).isNotNull())
            logger.info(f"Dropped {before - df.count()} rows with null '{col_name}'")

        elif strategy.startswith("default:"):
            fill_value: str | int | float = strategy.split(":", 1)[1]
            try:
                fill_value = float(fill_value) if "." in fill_value else int(fill_value)
            except ValueError:
                pass
            df = df.fillna({col_name: fill_value})
            logger.info(f"Filled {null_count} nulls in '{col_name}' with default={fill_value}")

        else:
            raise ValueError(f"Unknown null strategy for column '{col_name}': {strategy}")

    return df

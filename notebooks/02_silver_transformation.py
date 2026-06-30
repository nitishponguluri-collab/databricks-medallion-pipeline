# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleansing, Deduplication & SCD Type 2 Merge
# MAGIC Reads from the bronze Delta table, applies standardised cleansing rules, deduplicates
# MAGIC records, and merges into the silver Delta table using SCD Type 2 semantics to preserve
# MAGIC full change history.  Late-arriving data is handled via watermark-aware ordering.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/medallion-pipeline/src")

from pyspark.sql import SparkSession, functions as F
from transformation.silver_cleansing import (
    trim_whitespace, standardize_dates, deduplicate_records, handle_nulls
)
from transformation.scd2_merge import scd2_merge
from monitoring.pipeline_metrics import track_pipeline_run
from data_quality_rules import DataQualityEngine
import yaml, time
from loguru import logger

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("source_name", "orders", "Source Name")

env = dbutils.widgets.get("env")
source_name = dbutils.widgets.get("source_name")

with open("/Workspace/Repos/medallion-pipeline/config/pipeline_config.example.yaml") as f:
    config = yaml.safe_load(f)

env_cfg = config["environments"][env]
CATALOG = env_cfg["catalog"]
BRONZE_TABLE = f"{CATALOG}.{env_cfg['schemas']['bronze']}.{source_name}"
SILVER_TABLE = f"{CATALOG}.{env_cfg['schemas']['silver']}.{source_name}"

silver_cfg = config["silver"]["transformations"][source_name]
BUSINESS_KEYS = silver_cfg["business_keys"]
DEDUP_KEY = silver_cfg["dedup_key"]
NULL_STRATEGIES = silver_cfg["null_strategies"]
DATE_COLUMNS = silver_cfg.get("date_columns", [])
TRIM_COLUMNS = silver_cfg.get("trim_columns", [])

logger.info(f"Silver transform | env={env} source={BRONZE_TABLE} target={SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md ## Read Bronze (watermark for late-arriving data)

# COMMAND ----------

start_ts = time.time()

bronze_df = spark.readStream.format("delta").table(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md ## Cleansing Pipeline

# COMMAND ----------

def cleanse(df):
    df = trim_whitespace(df, TRIM_COLUMNS)
    df = standardize_dates(df, DATE_COLUMNS)
    df = handle_nulls(df, NULL_STRATEGIES)
    return df

# COMMAND ----------

# MAGIC %md ## Deduplication & SCD2 Merge (foreachBatch)

# COMMAND ----------

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping.")
        return

    cleaned = cleanse(batch_df)
    deduped = deduplicate_records(cleaned, DEDUP_KEY)

    logger.info(f"Batch {batch_id}: {deduped.count()} records after dedup")

    scd2_merge(
        spark=spark,
        source_df=deduped,
        target_table=SILVER_TABLE,
        business_key_cols=BUSINESS_KEYS,
    )

query = (
    bronze_df
    .withWatermark("_ingested_at", "2 hours")
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation",
            f"abfss://checkpoints@storageaccount.dfs.core.windows.net/silver/{source_name}/")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md ## Post-merge metrics

# COMMAND ----------

row_count = spark.table(SILVER_TABLE).filter("is_current = true").count()
duration = time.time() - start_ts
logger.info(f"Silver complete | active_rows={row_count} duration={duration:.1f}s")
track_pipeline_run(spark, "silver_transformation", "silver", row_count, duration, SILVER_TABLE)

display(spark.sql(f"SELECT * FROM {SILVER_TABLE} WHERE is_current = true LIMIT 20"))

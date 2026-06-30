# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Auto Loader Ingestion
# MAGIC Ingests raw files from cloud storage (ADLS Gen2 / S3) into Delta bronze tables using
# MAGIC Databricks Auto Loader with schema evolution and incremental checkpoint tracking.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/medallion-pipeline/src")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from ingestion.autoloader_ingest import configure_autoloader
from ingestion.schema_inference import infer_and_validate_schema
from monitoring.pipeline_metrics import track_pipeline_run
import yaml, os
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
BRONZE_SCHEMA = env_cfg["schemas"]["bronze"]
TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.{source_name}"

source_cfg = next(s for s in config["bronze"]["sources"] if s["name"] == source_name)
SOURCE_PATH = source_cfg["path"]
SOURCE_FORMAT = source_cfg["format"]
SCHEMA_LOCATION = source_cfg["schema_location"]
CHECKPOINT_LOCATION = source_cfg["checkpoint_location"]

logger.info(f"Bronze ingestion | env={env} source={source_name} target={TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md ## Schema Inference & Drift Detection

# COMMAND ----------

schema_result = infer_and_validate_schema(spark, SOURCE_PATH, SOURCE_FORMAT, SCHEMA_LOCATION)
if schema_result.has_drift:
    logger.warning(f"Schema drift detected: {schema_result.drift_details}")

# COMMAND ----------

# MAGIC %md ## Auto Loader Streaming Ingest

# COMMAND ----------

import time
start_ts = time.time()

reader = configure_autoloader(
    spark=spark,
    source_path=SOURCE_PATH,
    source_format=SOURCE_FORMAT,
    schema_location=SCHEMA_LOCATION,
)

bronze_df = (
    reader.load()
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_batch_id", F.lit(int(time.time())))
    .withColumn("_env", F.lit(env))
)

# COMMAND ----------

# MAGIC %md ## Write to Bronze Delta Table (trigger once for batch, or continuous for streaming)

# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

row_count = spark.table(TARGET_TABLE).count()
duration = time.time() - start_ts

logger.info(f"Bronze load complete | rows={row_count} duration={duration:.1f}s")
track_pipeline_run(spark, "bronze_ingestion", "bronze", row_count, duration, TARGET_TABLE)

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY _ingested_at DESC LIMIT 20"))
spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").show(5, truncate=False)

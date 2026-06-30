# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations & Dimensional Model
# MAGIC Builds query-optimised fact and dimension tables from silver Delta tables.
# MAGIC Tables are partitioned and Z-Ordered for Power BI / Tableau direct query performance.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/medallion-pipeline/src")

from pyspark.sql import SparkSession, functions as F
from delta.optimize_zorder import optimize_table
from monitoring.pipeline_metrics import track_pipeline_run
import yaml, time
from loguru import logger

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("aggregation", "daily_sales_summary", "Aggregation Name")

env = dbutils.widgets.get("env")
agg_name = dbutils.widgets.get("aggregation")

with open("/Workspace/Repos/medallion-pipeline/config/pipeline_config.example.yaml") as f:
    config = yaml.safe_load(f)

env_cfg = config["environments"][env]
CATALOG = env_cfg["catalog"]
SILVER_SCHEMA = env_cfg["schemas"]["silver"]
GOLD_SCHEMA = env_cfg["schemas"]["gold"]

agg_cfg = next(a for a in config["gold"]["aggregations"] if a["name"] == agg_name)
GOLD_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.{agg_name}"
PARTITION_BY = agg_cfg.get("partition_by", [])
ZORDER_BY = agg_cfg.get("zorder_by", [])

logger.info(f"Gold aggregation | env={env} agg={agg_name} target={GOLD_TABLE}")
start_ts = time.time()

# COMMAND ----------

# MAGIC %md ## Build Gold: Daily Sales Summary

# COMMAND ----------

if agg_name == "daily_sales_summary":
    silver_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders").filter("is_current = true")

    gold_df = (
        silver_orders
        .groupBy(
            F.to_date("order_date").alias("order_date"),
            "product_category",
            "region",
        )
        .agg(
            F.sum("order_amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
    )

# COMMAND ----------

# MAGIC %md ## Build Gold: Customer 360

# COMMAND ----------

elif agg_name == "customer_360":
    silver_customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers").filter("is_current = true")
    silver_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders").filter("is_current = true")

    order_summary = (
        silver_orders
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("lifetime_value"),
            F.max("order_date").alias("last_order_date"),
            F.min("order_date").alias("first_order_date"),
        )
    )

    gold_df = (
        silver_customers
        .join(order_summary, "customer_id", "left")
        .withColumn("_gold_updated_at", F.current_timestamp())
        .withColumn("signup_year", F.year("signup_date"))
    )

else:
    raise ValueError(f"Unknown aggregation: {agg_name}")

# COMMAND ----------

# MAGIC %md ## Write Gold Table (full overwrite by partition for idempotency)

# COMMAND ----------

writer = (
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
)

if PARTITION_BY:
    writer = writer.partitionBy(PARTITION_BY)

writer.saveAsTable(GOLD_TABLE)

# COMMAND ----------

# MAGIC %md ## Optimize for BI Tool Query Performance

# COMMAND ----------

optimize_table(spark, GOLD_TABLE, ZORDER_BY)
logger.info(f"Z-Order complete on {GOLD_TABLE} by {ZORDER_BY}")

row_count = spark.table(GOLD_TABLE).count()
duration = time.time() - start_ts
logger.info(f"Gold complete | rows={row_count} duration={duration:.1f}s")
track_pipeline_run(spark, f"gold_{agg_name}", "gold", row_count, duration, GOLD_TABLE)

display(spark.sql(f"SELECT * FROM {GOLD_TABLE} LIMIT 20"))

# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks — Cross-Layer Validation Suite
# MAGIC Runs Great Expectations-style validations across bronze → silver → gold.
# MAGIC Critical failures raise an exception to halt the pipeline job.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/medallion-pipeline/src")

from pyspark.sql import SparkSession, functions as F
from monitoring.data_quality_rules import DataQualityEngine
from monitoring.pipeline_metrics import track_pipeline_run
import yaml, time, json
from loguru import logger

spark = SparkSession.builder.getOrCreate()

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
GOLD_TABLE = f"{CATALOG}.{env_cfg['schemas']['gold']}.daily_sales_summary"

start_ts = time.time()
all_results = []

# COMMAND ----------

# MAGIC %md ## 1. Row Count Reconciliation (Bronze → Silver)

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).filter("is_current = true").count()
gold_count = spark.table(GOLD_TABLE).count()

logger.info(f"Row counts | bronze={bronze_count} silver_active={silver_count} gold={gold_count}")

if bronze_count == 0:
    raise RuntimeError("CRITICAL: Bronze table is empty — upstream ingestion may have failed.")

# COMMAND ----------

# MAGIC %md ## 2. Null Checks on Critical Fields

# COMMAND ----------

engine = DataQualityEngine(spark)

null_rules = [
    {"table": BRONZE_TABLE, "column": "_ingested_at", "severity": "critical"},
    {"table": BRONZE_TABLE, "column": "_source_file",  "severity": "critical"},
    {"table": SILVER_TABLE, "column": "is_current",    "severity": "critical"},
    {"table": SILVER_TABLE, "column": "effective_date","severity": "critical"},
]

null_results = engine.run_null_checks(null_rules)
all_results.extend(null_results)

for r in null_results:
    lvl = logger.error if r["severity"] == "critical" and not r["passed"] else logger.info
    lvl(f"Null check | {r['table']}.{r['column']} passed={r['passed']} null_count={r['null_count']}")

# COMMAND ----------

# MAGIC %md ## 3. Schema Drift Detection (Silver vs expected)

# COMMAND ----------

expected_silver_cols = {
    "order_id", "customer_id", "order_amount", "order_status",
    "order_date", "effective_date", "end_date", "is_current",
    "_ingested_at", "_source_file",
}
actual_silver_cols = set(spark.table(SILVER_TABLE).columns)
missing = expected_silver_cols - actual_silver_cols
extra   = actual_silver_cols - expected_silver_cols

schema_drift_result = {
    "check": "schema_drift_silver",
    "passed": len(missing) == 0,
    "severity": "critical",
    "missing_columns": list(missing),
    "extra_columns": list(extra),
}
all_results.append(schema_drift_result)

if missing:
    logger.error(f"Schema drift: missing columns in silver={missing}")
else:
    logger.info("Schema drift check: PASSED")

# COMMAND ----------

# MAGIC %md ## 4. Business Rule Validations

# COMMAND ----------

biz_rules = [
    {
        "name": "no_negative_order_amount",
        "table": SILVER_TABLE,
        "sql": "order_amount >= 0",
        "severity": "critical",
        "filter": "is_current = true",
    },
    {
        "name": "order_status_in_allowed_values",
        "table": SILVER_TABLE,
        "sql": "order_status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED','UNKNOWN')",
        "severity": "warning",
        "filter": "is_current = true",
    },
    {
        "name": "gold_revenue_non_negative",
        "table": GOLD_TABLE,
        "sql": "total_revenue >= 0",
        "severity": "critical",
    },
]

biz_results = engine.run_sql_rules(biz_rules)
all_results.extend(biz_results)

for r in biz_results:
    status = "PASSED" if r["passed"] else "FAILED"
    logger.info(f"Biz rule [{r['severity'].upper()}] {r['name']}: {status} (violations={r.get('violation_count', 0)})")

# COMMAND ----------

# MAGIC %md ## 5. Summary & Pipeline Gate

# COMMAND ----------

critical_failures = [r for r in all_results if r.get("severity") == "critical" and not r["passed"]]

summary = {
    "total_checks": len(all_results),
    "passed": sum(1 for r in all_results if r["passed"]),
    "failed": sum(1 for r in all_results if not r["passed"]),
    "critical_failures": len(critical_failures),
}

print(json.dumps(summary, indent=2))

duration = time.time() - start_ts
track_pipeline_run(spark, "data_quality_checks", "quality", len(all_results), duration, SILVER_TABLE)

if critical_failures:
    failed_names = [r.get("name") or r.get("check") for r in critical_failures]
    raise RuntimeError(
        f"PIPELINE HALTED: {len(critical_failures)} critical DQ failure(s): {failed_names}"
    )

logger.info("All critical data quality checks PASSED.")

# Databricks Medallion Architecture Pipeline

Production-grade data pipeline implementing the **medallion (bronze → silver → gold)** lakehouse pattern on Databricks with Unity Catalog governance, Auto Loader ingestion, SCD Type 2 historisation, Delta Lake optimisation, and a configurable data quality framework.

---

## Problem Statement

Raw data arriving from transactional systems, SaaS APIs, and event streams is inherently dirty: schema drift, late-arriving records, duplicates, and null critical fields. Without a governed transformation layer this raw data cannot be safely consumed by BI tools, ML models, or downstream applications.

This pipeline solves that by progressively refining raw files through three Delta Lake layers — each with explicit quality guarantees — while Unity Catalog enforces fine-grained access control across all consumers.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     CLOUD STORAGE (ADLS / S3)                       │
│              Raw JSON / CSV / Parquet landing zone                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Auto Loader (cloudFiles)
                            │  Schema evolution + checkpoint
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                                 │
│  ● Raw data preserved as-is (append-only)                           │
│  ● Metadata columns: _ingested_at, _source_file, _batch_id          │
│  ● Delta table with OPTIMIZE WRITE + AUTO COMPACT                   │
│  catalog.bronze.{entity}                                            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  foreachBatch streaming merge
                            │  Cleanse → Dedup → SCD2 MERGE INTO
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                                 │
│  ● Validated, deduplicated, standardised records                    │
│  ● SCD Type 2: effective_date / end_date / is_current               │
│  ● Full change history preserved                                    │
│  ● Late-arriving data handled via watermark                         │
│  catalog.silver.{entity}                                            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Batch aggregation
                            │  OPTIMIZE + ZORDER BY BI filter columns
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                  │
│  ● Business-level fact & dimension tables                           │
│  ● Partitioned + Z-Ordered for Power BI / Tableau direct query      │
│  ● daily_sales_summary, customer_360, …                             │
│  catalog.gold.{aggregation}                                         │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│               UNITY CATALOG GOVERNANCE LAYER                        │
│  ● Catalog per environment: medallion_dev / staging / prod          │
│  ● Role-based access: data_engineer / analyst / viewer              │
│  ● Column-level security & row filters (configurable)               │
│  ● Data lineage tracked automatically                               │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│               DATA QUALITY & MONITORING                             │
│  ● Cross-layer row count reconciliation                             │
│  ● Null checks on critical columns                                  │
│  ● Business rule SQL predicates                                     │
│  ● Schema drift detection                                           │
│  ● Pipeline metrics written to monitoring.pipeline_metrics          │
│  ● Critical failures halt the Databricks Job via exception          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
databricks-medallion-pipeline/
├── notebooks/                     # Databricks notebooks (Python source format)
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_aggregation.py
│   └── 04_data_quality_checks.py
├── src/                           # Importable Python modules
│   ├── ingestion/                 # Auto Loader config, schema inference
│   ├── transformation/            # Cleansing, SCD2 merge
│   ├── delta/                     # OPTIMIZE, ZORDER, VACUUM helpers
│   ├── unity_catalog/             # Catalog setup, RBAC grants
│   └── monitoring/                # Pipeline metrics, DQ rule engine
├── jobs/                          # Databricks Job JSON definition
├── infra/                         # Cluster config & cluster policy
├── tests/                         # pytest unit tests (local PySpark)
├── config/                        # Pipeline configuration YAML
└── .github/workflows/             # CI/CD — GitHub Actions
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Databricks workspace | Runtime 14.3 LTS or later |
| Unity Catalog enabled | Workspace must be UC-attached |
| Cloud storage | ADLS Gen2 or S3 for raw landing zone and checkpoints |
| Service principal | Recommended for CI/CD; PAT acceptable for dev |
| Python 3.11+ | For local test execution |

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/nitishponguluri-collab/databricks-medallion-pipeline.git
cd databricks-medallion-pipeline
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
```

```bash
cp config/pipeline_config.example.yaml config/pipeline_config.yaml
# Edit config/pipeline_config.yaml — set storage paths, catalog names, cluster sizing
```

### 3. Create Unity Catalog structure

Run this once per environment from a Databricks notebook or the SDK:

```python
from src.unity_catalog.catalog_setup import create_catalog_structure

create_catalog_structure(
    spark,
    catalog_name="medallion_dev",
    schemas=["bronze", "silver", "gold", "monitoring"],
    storage_root="abfss://catalog@storageaccount.dfs.core.windows.net/dev/",
)
```

### 4. Apply RBAC

```python
from src.unity_catalog.access_grants import apply_role_based_access

apply_role_based_access(
    spark,
    catalog_name="medallion_dev",
    schemas=["bronze", "silver", "gold"],
    role_assignments={
        "data-engineers-group": "data_engineer",
        "analysts-group":       "analyst",
        "bi-viewers-group":     "viewer",
    },
)
```

### 5. Deploy notebooks and job config

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure
databricks configure --token

# Deploy notebooks
databricks workspace import-dir notebooks /Workspace/Repos/medallion-pipeline/notebooks \
  --overwrite --format SOURCE

# Deploy job (replace {{env}} with target environment)
sed 's/{{env}}/dev/g' jobs/databricks_job_config.json > /tmp/job_dev.json
databricks jobs create --json-file /tmp/job_dev.json
```

---

## Running the Pipeline

### End-to-end via Databricks Job UI

1. Navigate to **Workflows → Jobs** in the Databricks UI.
2. Find `medallion-pipeline-{env}`.
3. Click **Run now** (or let the 02:00 UTC cron trigger fire).
4. Task execution order: `bronze_ingestion → silver_transformation → gold_sales + gold_customer360 → data_quality_checks`.

### Notebook-by-notebook (development)

```python
# In any notebook attached to a cluster with src/ on the path:
# Bronze
dbutils.notebook.run("01_bronze_ingestion", 3600, {"env": "dev", "source_name": "orders"})

# Silver
dbutils.notebook.run("02_silver_transformation", 3600, {"env": "dev", "source_name": "orders"})

# Gold
dbutils.notebook.run("03_gold_aggregation", 3600, {"env": "dev", "aggregation": "daily_sales_summary"})

# Quality
dbutils.notebook.run("04_data_quality_checks", 1800, {"env": "dev", "source_name": "orders"})
```

### Running unit tests locally

```bash
pytest tests/ -v
```

---

## Data Quality Framework

Rules are evaluated in `04_data_quality_checks.py` using `DataQualityEngine`:

| Check Type | Behaviour on Failure |
|---|---|
| **Null check** (critical) | Raises `RuntimeError` — job task fails |
| **Null check** (warning) | Logged, pipeline continues |
| **SQL predicate rule** (critical) | Raises `RuntimeError` |
| **SQL predicate rule** (warning) | Logged, pipeline continues |
| **Schema drift** | Critical — raises `RuntimeError` if expected columns are missing |
| **Row count reconciliation** | Critical — fails if target count drops >5% vs source |

Add custom rules as dicts passed to `engine.run_sql_rules()` or define them in `pipeline_config.yaml`.

---

## Cost Optimisation

### Cluster autoscaling & spot instances
- Bronze and silver clusters use `SPOT_WITH_FALLBACK_AZURE` — up to 60–70% cost reduction vs on-demand.
- Gold clusters for query-critical aggregations use `ON_DEMAND_AZURE` to avoid spot interruptions mid-merge.
- All job clusters auto-terminate when the task completes.

### Z-Ordering
Gold tables are Z-Ordered by the most common BI filter columns (e.g., `order_date`, `product_category`). This co-locates related data in fewer files, reducing the data scanned per query by 60–90% for selective filters.

### Auto Optimize
`optimizeWrite` and `autoCompact` are enabled on all clusters so Delta continuously manages small-file accumulation during streaming ingestion without requiring manual OPTIMIZE runs on bronze and silver.

### Retention & Vacuum
`vacuum_manager.py` defaults to a 7-day (168-hour) retention window. Run in dry-run mode first to see which files would be deleted, then execute. Do not reduce below 168 hours while active streaming queries are running against the table.

### Photon acceleration
All cluster configs specify the `PHOTON` runtime engine, which accelerates Delta MERGE INTO (critical for SCD2) and vectorised reads for gold aggregations by 2–5×.

---

## CI/CD

GitHub Actions workflow (`.github/workflows/deploy_to_databricks.yml`):

| Trigger | Jobs |
|---|---|
| Pull request to `main` | Lint, unit tests, deploy to dev, smoke test bronze notebook |
| Push to `main` | Lint, unit tests, deploy to prod, update prod job config |

Required GitHub secrets:
- `DATABRICKS_HOST_DEV` / `DATABRICKS_HOST_PROD`
- `DATABRICKS_TOKEN_DEV` / `DATABRICKS_TOKEN_PROD`
- `DATABRICKS_CLUSTER_ID_DEV` (for smoke test)

---

## Contributing

1. Branch from `main`.
2. Add/update unit tests for any changed `src/` module.
3. Run `pytest tests/ -v` locally before pushing.
4. Open a PR — the CI pipeline will deploy to dev and run a smoke test automatically.

---

## Author

**Nitish Chowdari Ponguluri** — Senior Cloud Platform Architect

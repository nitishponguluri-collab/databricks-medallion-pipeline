from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from loguru import logger


class DataQualityEngine:
    """YAML-configurable rule engine for data quality validation.

    Rules are defined in pipeline_config.yaml or passed directly as dicts.
    Each rule returns a result dict with:
        name, table, passed, severity, violation_count (or null_count), details.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ------------------------------------------------------------------
    # Null checks
    # ------------------------------------------------------------------

    def run_null_checks(self, rules: list[dict]) -> list[dict]:
        """Check for null values in critical columns.

        Rule dict keys: table, column, severity ('critical' | 'warning').
        """
        results = []
        for rule in rules:
            table = rule["table"]
            col = rule["column"]
            severity = rule.get("severity", "warning")

            try:
                null_count = (
                    self.spark.table(table)
                    .filter(F.col(col).isNull())
                    .count()
                )
                passed = null_count == 0
                results.append({
                    "check": f"null_check.{col}",
                    "table": table,
                    "column": col,
                    "severity": severity,
                    "passed": passed,
                    "null_count": null_count,
                })
                log = logger.error if (severity == "critical" and not passed) else logger.info
                log(f"Null check [{severity}] {table}.{col}: passed={passed} nulls={null_count}")
            except Exception as exc:
                logger.error(f"Null check failed with exception for {table}.{col}: {exc}")
                results.append({
                    "check": f"null_check.{col}",
                    "table": table,
                    "column": col,
                    "severity": severity,
                    "passed": False,
                    "null_count": -1,
                    "error": str(exc),
                })
        return results

    # ------------------------------------------------------------------
    # SQL predicate rules
    # ------------------------------------------------------------------

    def run_sql_rules(self, rules: list[dict]) -> list[dict]:
        """Evaluate boolean SQL predicates; violations are rows where the predicate is FALSE.

        Rule dict keys: name, table, sql (predicate expression), severity, filter (optional WHERE).
        """
        results = []
        for rule in rules:
            name = rule["name"]
            table = rule["table"]
            predicate = rule["sql"]
            severity = rule.get("severity", "warning")
            row_filter = rule.get("filter")

            try:
                base = self.spark.table(table)
                if row_filter:
                    base = base.filter(row_filter)

                violation_count = base.filter(f"NOT ({predicate})").count()
                passed = violation_count == 0

                results.append({
                    "name": name,
                    "table": table,
                    "predicate": predicate,
                    "severity": severity,
                    "passed": passed,
                    "violation_count": violation_count,
                })
                log = logger.error if (severity == "critical" and not passed) else logger.info
                log(f"SQL rule [{severity}] {name}: passed={passed} violations={violation_count}")
            except Exception as exc:
                logger.error(f"SQL rule '{name}' raised exception: {exc}")
                results.append({
                    "name": name,
                    "table": table,
                    "severity": severity,
                    "passed": False,
                    "violation_count": -1,
                    "error": str(exc),
                })
        return results

    # ------------------------------------------------------------------
    # Row count reconciliation
    # ------------------------------------------------------------------

    def run_count_reconciliation(
        self,
        source_table: str,
        target_table: str,
        tolerance_pct: float = 5.0,
        target_filter: str | None = None,
    ) -> dict:
        """Assert that the target table row count is within tolerance_pct of the source.

        Args:
            source_table:   Upstream table (e.g., bronze).
            target_table:   Downstream table (e.g., silver).
            tolerance_pct:  Allowed percentage drop between source and target.
            target_filter:  Optional SQL filter applied to the target (e.g., 'is_current=true').
        """
        source_count = self.spark.table(source_table).count()
        target_df = self.spark.table(target_table)
        if target_filter:
            target_df = target_df.filter(target_filter)
        target_count = target_df.count()

        if source_count == 0:
            return {"check": "count_reconciliation", "passed": False,
                    "source_count": 0, "target_count": target_count,
                    "error": "Source table is empty"}

        drop_pct = (1 - target_count / source_count) * 100
        passed = drop_pct <= tolerance_pct

        result = {
            "check": "count_reconciliation",
            "source_table": source_table,
            "target_table": target_table,
            "source_count": source_count,
            "target_count": target_count,
            "drop_pct": round(drop_pct, 2),
            "tolerance_pct": tolerance_pct,
            "passed": passed,
            "severity": "critical",
        }
        log = logger.error if not passed else logger.info
        log(f"Count recon {source_table}→{target_table}: drop={drop_pct:.1f}% passed={passed}")
        return result

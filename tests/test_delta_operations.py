"""Unit tests for Delta utility functions (optimize, vacuum dry-run)."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from src.delta.vacuum_manager import vacuum_table, _MINIMUM_SAFE_RETENTION_HOURS
from src.delta.optimize_zorder import optimize_table


class TestVacuumManager:
    def test_dry_run_is_default(self):
        spark = MagicMock()
        spark.sql.return_value = MagicMock(count=MagicMock(return_value=5))

        result = vacuum_table(spark, "catalog.schema.table", retention_hours=168)

        assert result["mode"] == "dry_run"
        assert result["files_to_remove"] == 5
        call_sql = spark.sql.call_args[0][0]
        assert "DRY RUN" in call_sql.upper()

    def test_execute_mode_does_not_dry_run(self):
        spark = MagicMock()

        result = vacuum_table(spark, "catalog.schema.table", retention_hours=336, dry_run=False)

        assert result["mode"] == "execute"
        call_sql = spark.sql.call_args[0][0]
        assert "DRY RUN" not in call_sql.upper()

    def test_retention_below_minimum_raises(self):
        spark = MagicMock()
        with pytest.raises(ValueError, match="safe minimum"):
            vacuum_table(spark, "catalog.schema.table", retention_hours=24)

    def test_minimum_boundary_is_accepted(self):
        spark = MagicMock()
        spark.sql.return_value = MagicMock(count=MagicMock(return_value=0))
        result = vacuum_table(spark, "catalog.schema.table", retention_hours=_MINIMUM_SAFE_RETENTION_HOURS)
        assert result["retention_hours"] == _MINIMUM_SAFE_RETENTION_HOURS


class TestOptimizeZorder:
    def _make_metrics_side_effect(self, before, after):
        """Return a side_effect for table_size_metrics that alternates before/after."""
        calls = [before, after]
        iterator = iter(calls)
        return lambda spark, table_name: next(iterator)

    def test_optimize_calls_sql_with_zorder(self):
        spark = MagicMock()
        before = {"num_files": 100, "size_gb": 1.0, "row_count": 1000, "size_bytes": 1_000_000_000, "table": "t"}
        after  = {"num_files": 10,  "size_gb": 0.8, "row_count": 1000, "size_bytes": 800_000_000,   "table": "t"}

        with patch("src.delta.optimize_zorder.table_size_metrics", side_effect=self._make_metrics_side_effect(before, after)):
            result = optimize_table(spark, "catalog.schema.orders", ["customer_id", "order_date"])

        sql_called = spark.sql.call_args[0][0]
        assert "ZORDER BY" in sql_called.upper()
        assert "customer_id" in sql_called
        assert result["files_removed"] == 90
        assert result["size_reduction_pct"] == 20.0

    def test_optimize_without_zorder_columns_runs_plain_optimize(self):
        spark = MagicMock()
        metrics = {"num_files": 50, "size_gb": 0.5, "row_count": 500, "size_bytes": 500_000_000, "table": "t"}

        with patch("src.delta.optimize_zorder.table_size_metrics", return_value=metrics):
            result = optimize_table(spark, "catalog.schema.small_table", zorder_columns=[])

        sql_called = spark.sql.call_args[0][0]
        assert "ZORDER" not in sql_called.upper()
        assert result["files_removed"] == 0

    def test_optimize_with_where_clause(self):
        spark = MagicMock()
        metrics = {"num_files": 10, "size_gb": 0.1, "row_count": 100, "size_bytes": 100_000_000, "table": "t"}

        with patch("src.delta.optimize_zorder.table_size_metrics", return_value=metrics):
            optimize_table(
                spark,
                "catalog.schema.orders",
                ["order_date"],
                where_clause="order_date >= '2024-01-01'",
            )

        sql_called = spark.sql.call_args[0][0]
        assert "WHERE" in sql_called.upper()
        assert "2024-01-01" in sql_called

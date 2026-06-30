from __future__ import annotations

from pyspark.sql import SparkSession
from loguru import logger


def create_catalog_structure(
    spark: SparkSession,
    catalog_name: str,
    schemas: list[str],
    storage_root: str | None = None,
    comment: str = "Medallion architecture catalog managed by pipeline infra",
) -> None:
    """Create a Unity Catalog catalog and its child schemas if they do not already exist.

    Args:
        spark:        Active SparkSession with Unity Catalog access.
        catalog_name: Name of the catalog to create (e.g., 'medallion_prod').
        schemas:      List of schema names to create under the catalog
                      (e.g., ['bronze', 'silver', 'gold', 'monitoring']).
        storage_root: Optional external storage location for the catalog
                      (e.g., 'abfss://catalog@storageaccount.dfs.core.windows.net/').
                      If None, the workspace default metastore storage is used.
        comment:      Human-readable description attached to the catalog.
    """
    # Create catalog
    storage_clause = f"MANAGED LOCATION '{storage_root}'" if storage_root else ""
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        {storage_clause}
        COMMENT '{comment}'
    """)
    logger.info(f"Catalog '{catalog_name}' ready.")

    # Create schemas
    for schema in schemas:
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}
            COMMENT 'Medallion {schema} layer — managed by pipeline infra'
        """)
        logger.info(f"Schema '{catalog_name}.{schema}' ready.")

    logger.info(f"Unity Catalog structure complete: catalog={catalog_name} schemas={schemas}")


def drop_catalog_structure(
    spark: SparkSession,
    catalog_name: str,
    schemas: list[str],
    cascade: bool = False,
) -> None:
    """Drop schemas and catalog (use with extreme caution — intended for dev teardown only).

    Args:
        spark:        Active SparkSession.
        catalog_name: Catalog to drop.
        schemas:      Schemas to drop first.
        cascade:      If True, drop schemas even if they contain tables.  Defaults to False
                      to prevent accidental data loss.
    """
    cascade_clause = "CASCADE" if cascade else ""
    for schema in schemas:
        logger.warning(f"Dropping schema {catalog_name}.{schema} {cascade_clause}")
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema} {cascade_clause}")

    logger.warning(f"Dropping catalog {catalog_name}")
    spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} {cascade_clause}")

from __future__ import annotations

from pyspark.sql import SparkSession
from loguru import logger


# Canonical privilege bundles per role
ROLE_PRIVILEGES: dict[str, list[str]] = {
    "data_engineer": ["SELECT", "MODIFY", "CREATE TABLE", "CREATE VIEW"],
    "analyst":       ["SELECT"],
    "viewer":        ["SELECT"],
    "pipeline_sa":   ["SELECT", "MODIFY", "CREATE TABLE", "CREATE VIEW", "CREATE SCHEMA"],
}


def grant_table_access(
    spark: SparkSession,
    principal: str,
    table_name: str,
    privileges: list[str],
) -> None:
    """Grant the specified privileges to a principal on a single table or view.

    Args:
        spark:      Active SparkSession.
        principal:  User email, group name, or service principal name.
        table_name: Fully qualified table name (catalog.schema.table).
        privileges: List of SQL privileges (e.g., ['SELECT', 'MODIFY']).
    """
    privilege_list = ", ".join(privileges)
    sql = f"GRANT {privilege_list} ON TABLE {table_name} TO `{principal}`"
    logger.info(f"Granting [{privilege_list}] on {table_name} → {principal}")
    spark.sql(sql)


def grant_schema_access(
    spark: SparkSession,
    principal: str,
    schema_name: str,
    privileges: list[str],
) -> None:
    """Grant privileges on all current and future tables in a schema.

    Args:
        spark:       Active SparkSession.
        principal:   User email, group name, or service principal name.
        schema_name: Fully qualified schema (catalog.schema).
        privileges:  List of SQL privileges.
    """
    privilege_list = ", ".join(privileges)
    sql = f"GRANT {privilege_list} ON SCHEMA {schema_name} TO `{principal}`"
    logger.info(f"Granting [{privilege_list}] on schema {schema_name} → {principal}")
    spark.sql(sql)


def apply_role_based_access(
    spark: SparkSession,
    catalog_name: str,
    schemas: list[str],
    role_assignments: dict[str, str],
) -> None:
    """Apply role-based access control across all schemas in a catalog.

    Args:
        spark:           Active SparkSession.
        catalog_name:    Target catalog.
        schemas:         List of schema names to apply grants to.
        role_assignments: Mapping of principal → role name.
                          Role names must match keys in ROLE_PRIVILEGES.
                          Example: {"data-engineers-group": "data_engineer",
                                    "analysts-group": "analyst"}
    """
    for principal, role in role_assignments.items():
        if role not in ROLE_PRIVILEGES:
            raise ValueError(f"Unknown role '{role}'. Valid roles: {list(ROLE_PRIVILEGES.keys())}")

        privileges = ROLE_PRIVILEGES[role]

        # Catalog-level USE access
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{principal}`")

        for schema in schemas:
            full_schema = f"{catalog_name}.{schema}"
            spark.sql(f"GRANT USE SCHEMA ON SCHEMA {full_schema} TO `{principal}`")
            grant_schema_access(spark, principal, full_schema, privileges)

        logger.info(f"Role '{role}' applied to principal '{principal}' across {len(schemas)} schemas.")


def revoke_table_access(
    spark: SparkSession,
    principal: str,
    table_name: str,
    privileges: list[str],
) -> None:
    """Revoke the specified privileges from a principal on a table."""
    privilege_list = ", ".join(privileges)
    sql = f"REVOKE {privilege_list} ON TABLE {table_name} FROM `{principal}`"
    logger.info(f"Revoking [{privilege_list}] on {table_name} from {principal}")
    spark.sql(sql)

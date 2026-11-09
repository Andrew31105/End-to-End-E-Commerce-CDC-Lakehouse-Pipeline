"""Static dimension mappings and Iceberg table definitions."""
from __future__ import annotations

from dataclasses import dataclass


BOOTSTRAP_VALID_FROM = "1900-01-01 00:00:00"
OPEN_ENDED_VALID_TO = "9999-12-31 00:00:00"


@dataclass(frozen=True)
class FieldMapping:
    """Map one Debezium record field to a typed Silver column."""

    target: str
    source: str
    spark_type: str


@dataclass(frozen=True)
class DimensionSpec:
    """Mapping between one PostgreSQL dimension and its history table."""

    name: str
    bronze_table: str
    history_table: str
    business_key: str
    surrogate_key: str
    tracked_columns: tuple[str, ...]
    fields: tuple[FieldMapping, ...]
    ddl: str


DIMENSIONS: tuple[DimensionSpec, ...] = (
    DimensionSpec(
        name="categories",
        bronze_table="iceberg.bronze.categories_raw",
        history_table="iceberg.silver.dim_categories_hist",
        business_key="category_id",
        surrogate_key="category_sk",
        tracked_columns=("category_name", "description"),
        fields=(
            FieldMapping("category_id", "id", "int"),
            FieldMapping("category_name", "name", "string"),
            FieldMapping("description", "description", "string"),
        ),
        ddl="""
            CREATE TABLE IF NOT EXISTS iceberg.silver.dim_categories_hist (
                category_sk BIGINT,
                category_id INT,
                category_name STRING,
                description STRING,
                source_ts TIMESTAMP,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN,
                row_hash STRING,
                cdc_op STRING,
                ingested_at TIMESTAMP,
                source_event_id STRING,
                closed_by_event_id STRING
            ) USING iceberg
        """,
    ),
    DimensionSpec(
        name="products",
        bronze_table="iceberg.bronze.products_raw",
        history_table="iceberg.silver.dim_products_hist",
        business_key="product_id",
        surrogate_key="product_sk",
        tracked_columns=(
            "product_name",
            "category_id",
            "base_price",
            "is_active",
        ),
        fields=(
            FieldMapping("product_id", "id", "bigint"),
            FieldMapping("product_name", "name", "string"),
            FieldMapping("category_id", "category_id", "int"),
            FieldMapping("base_price", "base_price", "decimal(12,2)"),
            FieldMapping("is_active", "is_active", "boolean"),
        ),
        ddl="""
            CREATE TABLE IF NOT EXISTS iceberg.silver.dim_products_hist (
                product_sk BIGINT,
                product_id BIGINT,
                product_name STRING,
                category_id INT,
                base_price DECIMAL(12,2),
                is_active BOOLEAN,
                source_ts TIMESTAMP,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN,
                row_hash STRING,
                cdc_op STRING,
                ingested_at TIMESTAMP,
                source_event_id STRING,
                closed_by_event_id STRING
            ) USING iceberg
        """,
    ),
    DimensionSpec(
        name="users",
        bronze_table="iceberg.bronze.users_raw",
        history_table="iceberg.silver.dim_users_hist",
        business_key="user_id",
        surrogate_key="user_sk",
        tracked_columns=("full_name", "email", "phone", "city"),
        fields=(
            FieldMapping("user_id", "id", "bigint"),
            FieldMapping("full_name", "full_name", "string"),
            FieldMapping("email", "email", "string"),
            FieldMapping("phone", "phone", "string"),
            FieldMapping("city", "city", "string"),
        ),
        ddl="""
            CREATE TABLE IF NOT EXISTS iceberg.silver.dim_users_hist (
                user_sk BIGINT,
                user_id BIGINT,
                full_name STRING,
                email STRING,
                phone STRING,
                city STRING,
                source_ts TIMESTAMP,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN,
                row_hash STRING,
                cdc_op STRING,
                ingested_at TIMESTAMP,
                source_event_id STRING,
                closed_by_event_id STRING
            ) USING iceberg
        """,
    ),
)

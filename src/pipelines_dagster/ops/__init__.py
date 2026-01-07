"""Pipeline operation functions for Dagster assets."""

from pipelines_dagster.ops.s3_extract import s3_extract_op
from pipelines_dagster.ops.trino_insert_select import trino_insert_select_op
from pipelines_dagster.ops.trino_extract import trino_extract_op
from pipelines_dagster.ops.trino_load import trino_load_op
from pipelines_dagster.ops.dataframe_to_s3 import dataframe_to_s3_op
from pipelines_dagster.ops.snowflake_extract import snowflake_extract_op
from pipelines_dagster.ops.snowflake_load import snowflake_load_op
from pipelines_dagster.ops.snowflake_insert_select import snowflake_insert_select_op
from pipelines_dagster.ops.cleanup import cleanup_sources_op

# Source classes for OO-style data extraction
from pipelines_dagster.sources import (
    Source,
    TrinoSource,
    SnowflakeSource,
    S3Source,
    create_source_from_config,
)

__all__ = [
    # Ops (function-based)
    "trino_insert_select_op",
    "dataframe_to_s3_op",
    "s3_extract_op",
    "trino_extract_op",
    "trino_load_op",
    "snowflake_extract_op",
    "snowflake_load_op",
    "snowflake_insert_select_op",
    "cleanup_sources_op",
    # Source classes (OO-style)
    "Source",
    "TrinoSource",
    "SnowflakeSource",
    "S3Source",
    "create_source_from_config",
]

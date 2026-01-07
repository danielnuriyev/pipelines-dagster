"""Snowflake ETL pipeline with pandas DataFrame as intermediate.

This module provides backward-compatible functions that delegate to the SnowflakeSource class.
For new code, prefer using SnowflakeSource directly from pipelines_dagster.sources.

This pipeline has two distinct steps:
1. Extract: SELECT from Snowflake into pandas DataFrame
2. Load: INSERT DataFrame into Snowflake target table
"""

from dagster import OpExecutionContext

from pipelines_dagster.sources import SnowflakeSource


def snowflake_extract_op(context: OpExecutionContext, config: dict):
    """
    Extract data from Snowflake source table.

    SQL can be specified either inline (select_query/sql_query) or from a file (sql_file).

    If batch_size and pk are specified, returns a generator yielding (batch_key, DataFrame) tuples.
    Otherwise, returns a single DataFrame.
    
    This function delegates to SnowflakeSource.extract() for the actual implementation.
    """
    source = SnowflakeSource.from_config(config)
    return source.extract(context)


def snowflake_extract_batch_generator(context: OpExecutionContext, config: dict):
    """
    Deprecated: Use snowflake_extract_op instead. It now handles batching internally.

    This function is kept for backward compatibility.
    """
    source = SnowflakeSource.from_config(config)
    return source.extract(context)

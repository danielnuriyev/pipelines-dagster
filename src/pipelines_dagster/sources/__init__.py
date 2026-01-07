"""Data sources for extracting data from various systems.

This package provides a unified interface for data extraction from:
- Trino: Distributed SQL query engine
- Snowflake: Cloud data warehouse
- S3/MinIO: Object storage (CSV files)
"""

from .source import Source, _load_sql_query
from .trino import TrinoSource
from .snowflake import SnowflakeSource
from .s3 import S3Source
from .sources import create_source_from_config


# Wrapper functions for backward compatibility with existing ops
def trino_extract_op(context, config: dict):
    """Extract data from Trino source table (backward compatible wrapper)."""
    source = TrinoSource.from_config(config)
    return source.extract(context)


def snowflake_extract_op(context, config: dict):
    """Extract data from Snowflake source table (backward compatible wrapper)."""
    source = SnowflakeSource.from_config(config)
    return source.extract(context)


def s3_extract_op(context, config: dict):
    """Extract CSV data from S3 (backward compatible wrapper)."""
    source = S3Source.from_config(config)
    return source.extract(context)


__all__ = [
    # Base classes and utilities
    "Source",
    "_load_sql_query",
    # Source implementations
    "TrinoSource",
    "SnowflakeSource",
    "S3Source",
    # Factory function
    "create_source_from_config",
    # Backward compatibility wrappers
    "trino_extract_op",
    "snowflake_extract_op",
    "s3_extract_op",
]

"""DuckDB SQL operations for dataframes."""

import os
import duckdb
import pandas as pd
from pathlib import Path
from dagster import OpExecutionContext


def _load_sql_query(config: dict, context: OpExecutionContext) -> str:
    """
    Load SQL query from either inline config or file.

    Args:
        config: Step configuration
        context: Dagster execution context

    Returns:
        SQL query string

    Raises:
        ValueError: If neither sql_query nor sql_file is specified, or both are specified
    """
    sql_query = config.get("sql_query")
    sql_file = config.get("sql_file")

    if sql_query and sql_file:
        raise ValueError("Cannot specify both 'sql_query' and 'sql_file' in config")
    elif sql_query:
        # Inline SQL query (can be string or multi-line YAML)
        if isinstance(sql_query, list):
            # Handle YAML multi-line strings
            return "\n".join(sql_query)
        return sql_query
    elif sql_file:
        # Load from file
        sql_file_path = Path(sql_file)

        # If relative path and we have pipeline directory, try same directory first
        pipeline_dir = config.get("_pipeline_dir")
        if not sql_file_path.is_absolute() and pipeline_dir:
            candidate_path = pipeline_dir / sql_file
            if candidate_path.exists():
                sql_file_path = candidate_path
            else:
                # Fall back to old behavior: relative to pipelines directory
                pipelines_dir = Path(__file__).parent.parent.parent / "pipelines"
                sql_file_path = pipelines_dir / sql_file

        # If still relative and no pipeline_dir, resolve relative to pipelines directory
        if not sql_file_path.is_absolute():
            pipelines_dir = Path(__file__).parent.parent.parent / "pipelines"
            sql_file_path = pipelines_dir / sql_file

        if not sql_file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        context.log.info(f"Loading SQL from file: {sql_file_path}")
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    else:
        raise ValueError("Must specify either 'sql_query' or 'sql_file' in config")


def duckdb_sql_op(context: OpExecutionContext, config: dict, df: pd.DataFrame) -> pd.DataFrame:
    """
    Execute SQL queries on pandas DataFrames using DuckDB.

    SQL can be specified either inline (sql_query) or from a file (sql_file).

    Args:
        context: Dagster execution context
        config: Configuration with SQL query or file path
        df: Input DataFrame

    Returns:
        DataFrame with query results
    """
    sql_query = _load_sql_query(config, context)

    context.log.info(f"Executing DuckDB SQL query: {sql_query[:100]}{'...' if len(sql_query) > 100 else ''}")

    # Create DuckDB connection and register the dataframe
    con = duckdb.connect()

    # Register the input dataframe as a table
    con.register("input_df", df)

    # Execute the query
    result_df = con.execute(sql_query).fetchdf()

    con.close()

    context.log.info(f"DuckDB query returned {len(result_df)} rows with columns: {list(result_df.columns)}")

    return result_df

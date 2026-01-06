"""Snowflake ETL pipeline with pandas DataFrame as intermediate.

This pipeline has two distinct steps:
1. Extract: SELECT from Snowflake into pandas DataFrame
2. Load: INSERT DataFrame into Snowflake target table
"""

import pandas as pd
import snowflake.connector
from pathlib import Path
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    retry_with_backoff,
    is_retryable_snowflake_error,
    get_retry_config_from_yaml
)


def _load_sql_query(config: dict, context: OpExecutionContext, pipeline_dir: Path = None) -> str:
    """
    Load SQL query from either inline config or file.

    Args:
        config: Step configuration
        context: Dagster execution context
        pipeline_dir: Directory containing the pipeline YAML (for relative SQL file resolution)

    Returns:
        SQL query string

    Raises:
        ValueError: If neither sql_query nor sql_file is specified, or both are specified
    """
    sql_query = config.get("sql_query") or config.get("select_query")  # Support both field names
    sql_file = config.get("sql_file")

    if sql_query and sql_file:
        raise ValueError("Cannot specify both 'sql_query'/'select_query' and 'sql_file' in config")
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
        raise ValueError("Must specify either 'sql_query'/'select_query' or 'sql_file' in config")


def snowflake_extract_op(context: OpExecutionContext, config: dict):
    """
    Extract data from Snowflake source table.

    SQL can be specified either inline (select_query/sql_query) or from a file (sql_file).

    If batch_size and pk are specified, returns a generator yielding (batch_key, DataFrame) tuples.
    Otherwise, returns a single DataFrame.
    """
    batch_size = config.get("batch_size")
    pk_column = config.get("pk")
    select_query = _load_sql_query(config, context)

    context.log.info(f"Connecting to Snowflake at {config['account']}")

    # If batching is requested, use batch generator
    if batch_size is not None and pk_column is not None:
        return _extract_batches(context, config)

    # Non-batched: fetch all data
    def connect_snowflake():
        return snowflake.connector.connect(
            account=config["account"],
            user=config["user"],
            password=config["password"],
            warehouse=config.get("warehouse"),
            database=config.get("database"),
            schema=config.get("schema")
        )

    retry_config = get_retry_config_from_yaml(config, "snowflake")
    try:
        conn = retry_with_backoff(
            connect_snowflake,
            retry_config,
            context
        )
    except Exception as e:
        if not is_retryable_snowflake_error(e):
            raise
        # If it's retryable but still failed, re-raise
        raise

    cursor = conn.cursor()

    context.log.info(f"Executing query: {select_query}")
    cursor.execute(select_query)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    df = pd.DataFrame(rows, columns=columns)
    context.log.info(f"Extracted {len(df)} rows with columns: {list(df.columns)}")
    context.log.info(f"Returning DataFrame with type: {type(df)}")

    return df


def _extract_batches(context: OpExecutionContext, config: dict):
    """Generate DataFrames for each batch."""
    batch_size = config.get("batch_size")
    pk_column = config.get("pk")
    select_query = _load_sql_query(config, context)

    def connect_snowflake():
        return snowflake.connector.connect(
            account=config["account"],
            user=config["user"],
            password=config["password"],
            warehouse=config.get("warehouse"),
            database=config.get("database"),
            schema=config.get("schema")
        )

    retry_config = get_retry_config_from_yaml(config, "snowflake")
    try:
        conn = retry_with_backoff(
            connect_snowflake,
            retry_config,
            context
        )
    except Exception as e:
        if not is_retryable_snowflake_error(e):
            raise
        raise

    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT MIN({pk_column}), MAX({pk_column})
        FROM ({select_query}) AS base
        """
    )
    bounds = cursor.fetchone()
    if bounds is None or bounds[0] is None or bounds[1] is None:
        cursor.close()
        conn.close()
        return

    min_pk, max_pk = bounds
    cursor.close()

    current = min_pk
    while current <= max_pk:
        upper = current + batch_size
        batch_query = f"""
            SELECT *
            FROM ({select_query}) AS base
            WHERE {pk_column} >= {current}
              AND {pk_column} < {upper}
        """
        cursor = conn.cursor()
        context.log.info(f"Executing batch query for {current}-{upper - 1}: {batch_query}")
        cursor.execute(batch_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(rows, columns=columns)
        yield current, df

        cursor.close()
        current = upper

    conn.close()


def snowflake_extract_batch_generator(context: OpExecutionContext, config: dict):
    """
    Deprecated: Use snowflake_extract_op instead. It now handles batching internally.

    This function is kept for backward compatibility but delegates to _extract_batches.
    """
    return _extract_batches(context, config)

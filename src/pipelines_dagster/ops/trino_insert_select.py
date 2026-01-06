"""Trino INSERT SELECT operation."""

import trino
from pathlib import Path
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    retry_with_backoff,
    is_retryable_trino_error,
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


def trino_insert_select_op(context: OpExecutionContext, config: dict) -> None:
    """Execute INSERT INTO target_table SELECT ... with configurable source and target.

    SQL can be specified either inline (select_query/sql_query) or from a file (sql_file).
    If temp: True is specified, creates a temporary table with unique naming.
    """
    context.log.info(f"Connecting to Trino at {config['host']}:{config['port']}")

    # Load SQL query from config or file
    select_query = _load_sql_query(config, context)

    # Use actual table name if it was preprocessed for temp tables
    actual_table = config.get("actual_target_table", config.get("target_table"))

    def connect_trino():
        return trino.dbapi.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
        )

    retry_config = get_retry_config_from_yaml(config, "trino")
    try:
        conn = retry_with_backoff(
            connect_trino,
            retry_config,
            context
        )
    except Exception as e:
        if not is_retryable_trino_error(e):
            raise
        raise
    cursor = conn.cursor()

    target_full_name = f"{config['target_catalog']}.{config['target_schema']}.{actual_table}"

    # Check if target table exists
    cursor.execute(f"""
        SELECT table_name FROM {config['target_catalog']}.information_schema.tables
        WHERE table_catalog = '{config['target_catalog']}'
        AND table_schema = '{config['target_schema']}'
        AND table_name = '{actual_table}'
    """)
    table_exists = len(cursor.fetchall()) > 0

    if table_exists:
        context.log.info(f"Table {target_full_name} exists. Inserting data...")
        cursor.execute(f"INSERT INTO {target_full_name} {select_query}")
    else:
        context.log.info(f"Table {target_full_name} does not exist. Creating...")
        cursor.execute(f"CREATE TABLE {target_full_name} AS {select_query}")

    cursor.fetchall()  # Wait for query to complete
    context.log.info("Insert complete")

    cursor.close()
    conn.close()

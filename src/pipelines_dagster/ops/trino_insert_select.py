"""Trino INSERT SELECT operation."""

import trino
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    retry_with_backoff,
    is_retryable_trino_error,
    get_retry_config_from_yaml
)


def trino_insert_select_op(context: OpExecutionContext, config: dict) -> None:
    """Execute INSERT INTO target_table SELECT ... with configurable source and target."""
    context.log.info(f"Connecting to Trino at {config['host']}:{config['port']}")

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

    target_full_name = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"

    # Check if target table exists
    cursor.execute(f"""
        SELECT table_name FROM {config['target_catalog']}.information_schema.tables
        WHERE table_catalog = '{config['target_catalog']}'
        AND table_schema = '{config['target_schema']}'
        AND table_name = '{config['target_table']}'
    """)
    table_exists = len(cursor.fetchall()) > 0

    if table_exists:
        context.log.info(f"Table {target_full_name} exists. Inserting data...")
        cursor.execute(f"INSERT INTO {target_full_name} {config['select_query']}")
    else:
        context.log.info(f"Table {target_full_name} does not exist. Creating...")
        cursor.execute(f"CREATE TABLE {target_full_name} AS {config['select_query']}")

    cursor.fetchall()  # Wait for query to complete
    context.log.info("Insert complete")

    cursor.close()
    conn.close()

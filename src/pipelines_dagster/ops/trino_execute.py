"""Trino general SQL execution operation."""

import trino
from dagster import OpExecutionContext
from pipelines_dagster.retry_utils import (
    retry_with_backoff,
    is_retryable_trino_error,
    get_retry_config_from_yaml
)

def trino_execute_op(context: OpExecutionContext, config: dict) -> str:
    """Execute a general SQL query in Trino."""
    sql_query = config.get("select_query") or config.get("sql_query")
    if not sql_query:
        raise ValueError("No SQL query provided")

    context.log.info(f"Connecting to Trino at {config['host']}:{config['port']}")

    def connect_trino():
        return trino.dbapi.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
        )

    retry_config = get_retry_config_from_yaml(config, "trino")
    conn = retry_with_backoff(
        connect_trino,
        retry_config,
        context
    )
    
    cursor = conn.cursor()
    
    # Handle multiple statements separated by semicolon
    queries = [q.strip() for q in sql_query.split(";") if q.strip()]
    
    for query in queries:
        context.log.info(f"Executing: {query}")
        cursor.execute(query)
        # For non-SELECT queries, we don't necessarily need fetchall
        # but Trino client sometimes requires it or closing cursor
        try:
            cursor.fetchall()
        except:
            pass

    cursor.close()
    conn.close()
    
    return "Success"


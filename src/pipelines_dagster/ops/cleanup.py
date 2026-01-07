"""Cleanup operations for temporary resources."""

from dagster import OpExecutionContext
from pipelines_dagster.sources import TrinoSource, SnowflakeSource


def cleanup_sources_op(context: OpExecutionContext, config: dict):
    """Cleanup temporary tables using Source classes.
    
    Expected config:
    {
        "db_type": "trino" | "snowflake",
        "connection_config": { ... },
        "temp_tables": ["temp_table_1", "temp_table_2", ...]
    }
    """
    db_type = config.get("db_type")
    connection_config = config.get("connection_config", {})
    temp_tables = config.get("temp_tables", [])
    
    if not temp_tables:
        context.log.info("No temp tables to clean up.")
        return

    context.log.info(f"Cleaning up {len(temp_tables)} temp tables for {db_type}")

    if db_type == "snowflake":
        source_class = SnowflakeSource
    elif db_type == "trino":
        source_class = TrinoSource
    else:
        context.log.warning(f"Unknown db_type for cleanup: {db_type}")
        return

    for temp_table in temp_tables:
        # Reconstruct source instance
        # We pass a dummy 'table' and 'temp=True' so get_temp_table_name works
        # but then we override _temp_table_name with the actual name used
        source_config = {**connection_config, "temp": True, "table": "dummy"}
        source = source_class.from_config(source_config)
        source._temp_table_name = temp_table
        
        try:
            source.cleanup(context)
        except Exception as e:
            context.log.error(f"Failed to cleanup temp table {temp_table}: {str(e)}")
            # Continue with other tables even if one fails
            continue


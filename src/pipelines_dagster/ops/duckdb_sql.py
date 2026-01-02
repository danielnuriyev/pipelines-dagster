"""DuckDB SQL operations for dataframes."""

import duckdb
import pandas as pd
from dagster import OpExecutionContext


def duckdb_sql_op(context: OpExecutionContext, config: dict, df: pd.DataFrame) -> pd.DataFrame:
    """
    Execute SQL queries on pandas DataFrames using DuckDB.

    Args:
        context: Dagster execution context
        config: Configuration with SQL query
        df: Input DataFrame

    Returns:
        DataFrame with query results
    """
    sql_query = config["sql_query"]

    context.log.info(f"Executing DuckDB SQL query: {sql_query}")

    # Create DuckDB connection and register the dataframe
    con = duckdb.connect()

    # Register the input dataframe as a table
    con.register("input_df", df)

    # Execute the query
    result_df = con.execute(sql_query).fetchdf()

    con.close()

    context.log.info(f"DuckDB query returned {len(result_df)} rows with columns: {list(result_df.columns)}")

    return result_df

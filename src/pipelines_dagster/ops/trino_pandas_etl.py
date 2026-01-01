"""Trino ETL pipeline with pandas DataFrame as intermediate.

This pipeline has two distinct steps:
1. Extract: SELECT from Trino into pandas DataFrame
2. Load: INSERT DataFrame into Trino target table
"""

import pandas as pd
import trino
from dagster import OpExecutionContext


def trino_extract_op(context: OpExecutionContext, config: dict) -> pd.DataFrame:
    """Step 1: Extract data from Trino source table into pandas DataFrame."""
    context.log.info(f"Connecting to Trino at {config['host']}:{config['port']}")

    conn = trino.dbapi.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
    )
    cursor = conn.cursor()

    select_query = config["select_query"]
    context.log.info(f"Executing query: {select_query}")
    cursor.execute(select_query)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    df = pd.DataFrame(rows, columns=columns)
    context.log.info(f"Extracted {len(df)} rows with columns: {list(df.columns)}")

    return df


def trino_load_op(context: OpExecutionContext, config: dict, df: pd.DataFrame) -> None:
    """Step 2: Load pandas DataFrame into Trino target table."""
    context.log.info(f"Connecting to Trino at {config['host']}:{config['port']}")

    conn = trino.dbapi.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
    )
    cursor = conn.cursor()

    target_catalog = config["target_catalog"]
    target_schema = config["target_schema"]
    target_table = config["target_table"]
    target_full_name = f"{target_catalog}.{target_schema}.{target_table}"

    # Check if we should recreate the table
    recreate_table = config.get("recreate_table", False)

    # Check if target table exists
    cursor.execute(f"""
        SELECT table_name FROM {target_catalog}.information_schema.tables
        WHERE table_catalog = '{target_catalog}'
        AND table_schema = '{target_schema}'
        AND table_name = '{target_table}'
    """)
    table_exists = len(cursor.fetchall()) > 0

    # Drop table if recreate_table is True
    if table_exists and recreate_table:
        context.log.info(f"Dropping table {target_full_name} (recreate_table=True)...")
        cursor.execute(f"DROP TABLE {target_full_name}")
        cursor.fetchall()
        table_exists = False

    # Create table if it doesn't exist
    if not table_exists:
        context.log.info(f"Creating table {target_full_name}...")
        column_defs = []
        for col in df.columns:
            dtype = df[col].dtype
            dtype_str = str(dtype)
            if dtype == "int64":
                trino_type = "BIGINT"
            elif dtype == "float64":
                trino_type = "DOUBLE"
            elif dtype == "bool":
                trino_type = "BOOLEAN"
            elif "datetime" in dtype_str:
                trino_type = "TIMESTAMP"
            else:
                trino_type = "VARCHAR"
            column_defs.append(f'"{col}" {trino_type}')

        create_sql = f"CREATE TABLE {target_full_name} ({', '.join(column_defs)})"
        context.log.info(f"Executing: {create_sql}")
        cursor.execute(create_sql)
        cursor.fetchall()

    # Insert data row by row
    context.log.info(f"Inserting {len(df)} rows into {target_full_name}...")
    columns = ", ".join([f'"{col}"' for col in df.columns])

    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isna(val):
                values.append("NULL")
            elif isinstance(val, str):
                escaped = val.replace("'", "''")
                values.append(f"'{escaped}'")
            elif isinstance(val, (int, float)):
                values.append(str(val))
            elif hasattr(val, "strftime"):
                # Handle datetime/timestamp values
                ts_str = val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                values.append(f"TIMESTAMP '{ts_str}'")
            else:
                escaped = str(val).replace("'", "''")
                values.append(f"'{escaped}'")
        values_str = ", ".join(values)
        insert_sql = f"INSERT INTO {target_full_name} ({columns}) VALUES ({values_str})"
        cursor.execute(insert_sql)
        cursor.fetchall()

    context.log.info(f"Successfully loaded {len(df)} rows into {target_full_name}")

    cursor.close()
    conn.close()

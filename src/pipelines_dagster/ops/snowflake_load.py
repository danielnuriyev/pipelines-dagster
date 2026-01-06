"""Snowflake load operations for inserting DataFrames into Snowflake tables."""

import pandas as pd
import snowflake.connector
from dagster import OpExecutionContext


def snowflake_load_op(context: OpExecutionContext, config: dict, df: pd.DataFrame) -> str:
    """Step 2: Load pandas DataFrame into Snowflake target table."""
    context.log.info(f"Connecting to Snowflake at {config['account']}")

    conn = snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        warehouse=config.get("warehouse"),
        database=config.get("database"),
        schema=config.get("schema")
    )
    cursor = conn.cursor()

    target_database = config["target_database"]
    target_schema = config["target_schema"]
    target_table = config["target_table"]
    target_full_name = f"{target_database}.{target_schema}.{target_table}"

    # Check if we should recreate the table
    recreate_table = config.get("recreate_table", False)

    # Check if target table exists
    cursor.execute(f"""
        SELECT table_name FROM {target_database}.information_schema.tables
        WHERE table_catalog = '{target_database}'
        AND table_schema = '{target_schema}'
        AND table_name = '{target_table}'
    """)
    table_exists = len(cursor.fetchall()) > 0

    # Drop table if recreate_table is True
    if table_exists and recreate_table:
        context.log.info(f"Dropping table {target_full_name} (recreate_table=True)...")
        cursor.execute(f"DROP TABLE {target_full_name}")
        table_exists = False

    # Create table if it doesn't exist
    if not table_exists:
        context.log.info(f"Creating table {target_full_name}...")
        column_defs = []
        for col in df.columns:
            dtype = df[col].dtype
            dtype_str = str(dtype)
            if dtype == "int64":
                snowflake_type = "BIGINT"
            elif dtype == "float64":
                snowflake_type = "FLOAT"
            elif dtype == "bool":
                snowflake_type = "BOOLEAN"
            elif "datetime" in dtype_str:
                snowflake_type = "TIMESTAMP"
            else:
                snowflake_type = "VARCHAR"
            column_defs.append(f'"{col}" {snowflake_type}')

        create_sql = f"CREATE TABLE {target_full_name} ({', '.join(column_defs)})"
        context.log.info(f"Executing: {create_sql}")
        cursor.execute(create_sql)

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
                values.append(f"'{ts_str}'")
            else:
                escaped = str(val).replace("'", "''")
                values.append(f"'{escaped}'")
        values_str = ", ".join(values)
        insert_sql = f"INSERT INTO {target_full_name} ({columns}) VALUES ({values_str})"
        cursor.execute(insert_sql)

    context.log.info(f"Successfully loaded {len(df)} rows into {target_full_name}")

    cursor.close()
    conn.close()

    # Return success indicator for asset materialization
    return f"snowflake://{target_full_name}"

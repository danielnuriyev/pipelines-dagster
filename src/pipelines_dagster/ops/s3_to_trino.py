"""S3 to Trino load operation."""

import os

import boto3
import pandas as pd
import trino
from botocore.client import Config as BotoConfig
from dagster import OpExecutionContext


def execute_s3_to_trino(context: OpExecutionContext, config: dict):
    """Load a CSV file from S3 into a Trino table."""
    # Get S3 secret from environment
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    # Download CSV from S3
    s3_client = boto3.client(
        "s3",
        endpoint_url=config["s3_endpoint"],
        aws_access_key_id=config["s3_access_key"],
        aws_secret_access_key=s3_secret_key,
        config=BotoConfig(signature_version="s3v4"),
    )

    context.log.info(f"Downloading CSV from s3://{config['s3_bucket']}/{config['s3_key']}")
    response = s3_client.get_object(Bucket=config["s3_bucket"], Key=config["s3_key"])
    csv_content = response["Body"].read().decode("utf-8")

    # Parse CSV with pandas
    from io import StringIO

    df = pd.read_csv(StringIO(csv_content))
    context.log.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

    # Connect to Trino
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
        context.log.info(f"Table {target_full_name} does not exist. Creating...")
        # Infer column types from pandas DataFrame
        column_defs = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype == "int64":
                trino_type = "BIGINT"
            elif dtype == "float64":
                trino_type = "DOUBLE"
            elif dtype == "bool":
                trino_type = "BOOLEAN"
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
                # Escape single quotes
                escaped = val.replace("'", "''")
                values.append(f"'{escaped}'")
            else:
                values.append(str(val))
        values_str = ", ".join(values)
        insert_sql = f"INSERT INTO {target_full_name} ({columns}) VALUES ({values_str})"
        cursor.execute(insert_sql)
        cursor.fetchall()

    context.log.info(f"Successfully loaded {len(df)} rows into {target_full_name}")

    cursor.close()
    conn.close()


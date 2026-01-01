"""Trino to S3 export operation."""

import os

import boto3
import pandas as pd
import trino
from botocore.client import Config as BotoConfig
from dagster import OpExecutionContext


def trino_to_s3_op(context: OpExecutionContext, config: dict) -> None:
    """Execute a SELECT query in Trino and export results to S3 as CSV."""
    context.log.info(f"Connecting to Trino at {config['host']}:{config['port']}")

    conn = trino.dbapi.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
    )
    cursor = conn.cursor()

    context.log.info(f"Executing query: {config['select_query']}")
    cursor.execute(config["select_query"])

    # Fetch results and column names
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    context.log.info(f"Fetched {len(rows)} rows with columns: {columns}")

    cursor.close()
    conn.close()

    # Convert to CSV using pandas
    df = pd.DataFrame(rows, columns=columns)
    csv_content = df.to_csv(index=False)

    # Get S3 secret from environment
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    # Upload to S3/MinIO
    context.log.info(f"Uploading to S3: {config['s3_endpoint']}/{config['s3_bucket']}/{config['s3_key']}")
    s3_client = boto3.client(
        "s3",
        endpoint_url=config["s3_endpoint"],
        aws_access_key_id=config["s3_access_key"],
        aws_secret_access_key=s3_secret_key,
        config=BotoConfig(signature_version="s3v4"),
    )

    s3_client.put_object(
        Bucket=config["s3_bucket"],
        Key=config["s3_key"],
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )

    context.log.info(f"Uploaded CSV to s3://{config['s3_bucket']}/{config['s3_key']}")

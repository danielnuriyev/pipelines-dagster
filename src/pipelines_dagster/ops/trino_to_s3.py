"""Trino to S3 export operation."""

import os

import boto3
import pandas as pd
import trino
from botocore.client import Config as BotoConfig
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    retry_with_backoff,
    is_retryable_s3_error,
    is_retryable_trino_error,
    get_retry_config_from_yaml
)


def dataframe_to_s3_op(context: OpExecutionContext, config: dict, df: pd.DataFrame) -> str:
    """Upload a pandas DataFrame to S3 as CSV."""
    # Debug: Check if df is None
    if df is None:
        context.log.error("ERROR: DataFrame is None in dataframe_to_s3_op!")
        raise ValueError("DataFrame input is None")

    context.log.info(f"DataFrame type: {type(df)}, shape: {df.shape if hasattr(df, 'shape') else 'no shape'}")

    # Make a copy of the DataFrame to avoid any sharing issues
    df_copy = df.copy()
    context.log.info(f"DataFrame copy created, shape: {df_copy.shape}")

    # Convert DataFrame to CSV
    csv_content = df_copy.to_csv(index=False)

    # Get S3 secret from environment
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    # Upload to S3/MinIO with retry logic
    context.log.info(f"Uploading DataFrame ({len(df)} rows) to S3: {config['s3_endpoint']}/{config['s3_bucket']}/{config['s3_key']}")

    def create_s3_client():
        return boto3.client(
            "s3",
            endpoint_url=config["s3_endpoint"],
            aws_access_key_id=config["s3_access_key"],
            aws_secret_access_key=s3_secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def upload_to_s3():
        s3_client = create_s3_client()
        s3_client.put_object(
            Bucket=config["s3_bucket"],
            Key=config["s3_key"],
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )

    retry_config = get_retry_config_from_yaml(config, "s3")
    try:
        retry_with_backoff(
            upload_to_s3,
            retry_config,
            context
        )
    except Exception as e:
        if not is_retryable_s3_error(e):
            raise
        raise

    context.log.info(f"Uploaded DataFrame to s3://{config['s3_bucket']}/{config['s3_key']}")

    # Return success indicator for asset materialization
    return f"s3://{config['s3_bucket']}/{config['s3_key']}"


def trino_to_s3_op(context: OpExecutionContext, config: dict) -> None:
    """Execute a SELECT query in Trino and export results to S3 as CSV."""
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

    # Upload to S3/MinIO with retry logic
    context.log.info(f"Uploading to S3: {config['s3_endpoint']}/{config['s3_bucket']}/{config['s3_key']}")

    def create_s3_client():
        return boto3.client(
            "s3",
            endpoint_url=config["s3_endpoint"],
            aws_access_key_id=config["s3_access_key"],
            aws_secret_access_key=s3_secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def upload_to_s3():
        s3_client = create_s3_client()
        s3_client.put_object(
            Bucket=config["s3_bucket"],
            Key=config["s3_key"],
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )

    retry_config = get_retry_config_from_yaml(config, "s3")
    try:
        retry_with_backoff(
            upload_to_s3,
            retry_config,
            context
        )
    except Exception as e:
        if not is_retryable_s3_error(e):
            raise
        raise

    context.log.info(f"Uploaded CSV to s3://{config['s3_bucket']}/{config['s3_key']}")

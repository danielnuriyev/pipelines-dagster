"""S3 extract operation - download CSV from S3 and return pandas DataFrame."""

import os
from io import StringIO

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    retry_with_backoff,
    is_retryable_s3_error,
    get_retry_config_from_yaml
)


def s3_extract_op(context: OpExecutionContext, config: dict) -> pd.DataFrame:
    """Step 1: Extract CSV data from S3 and return as pandas DataFrame."""
    # Get S3 secret from environment
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    # Download CSV from S3 with retry logic
    context.log.info(f"Downloading from S3: {config['s3_endpoint']}/{config['s3_bucket']}/{config['s3_key']}")

    def create_s3_client():
        return boto3.client(
            "s3",
            endpoint_url=config["s3_endpoint"],
            aws_access_key_id=config["s3_access_key"],
            aws_secret_access_key=s3_secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def download_from_s3():
        s3_client = create_s3_client()
        return s3_client.get_object(Bucket=config["s3_bucket"], Key=config["s3_key"])

    retry_config = get_retry_config_from_yaml(config, "s3")
    try:
        response = retry_with_backoff(
            download_from_s3,
            retry_config,
            context
        )
    except Exception as e:
        if not is_retryable_s3_error(e):
            raise
        raise

    csv_content = response["Body"].read().decode("utf-8")

    # Parse CSV with pandas
    df = pd.read_csv(StringIO(csv_content))
    context.log.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

    return df

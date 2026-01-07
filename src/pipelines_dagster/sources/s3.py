"""S3 data source for extracting data from S3/MinIO."""

import os
from io import StringIO

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    get_retry_config_from_yaml,
    is_retryable_s3_error,
    retry_with_backoff,
)

from .source import Source


class S3Source(Source):
    """Source for extracting data from S3/MinIO.

    YAML Configuration:
        executor: s3_extract
        config:
          endpoint: http://minio:9000
          bucket: my-bucket
          key: path/to/file.csv
          access_key: minioaccess
          # s3_secret_key is provided via environment variable S3_SECRET_KEY
          retry:
            max_attempts: 3
            base_delay: 1s
            max_delay: 1m
    """
    def __init__(self, config: dict):
        """Initialize the S3 source from a configuration dictionary."""
        super().__init__(config)
        self.type = "s3"
        self.endpoint = config.get("endpoint", "")
        self.bucket = config.get("bucket", "")
        self.key = config.get("key", "")
        self.access_key = config.get("access_key", "")
        # Note: secret_key is read from S3_SECRET_KEY environment variable

    def _create_client(self):
        """Create an S3 client."""
        s3_secret_key = os.environ.get("S3_SECRET_KEY", "")
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=s3_secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def extract(self, context: OpExecutionContext) -> pd.DataFrame:
        """Extract CSV data from S3 and return as pandas DataFrame."""
        context.log.info(f"Downloading from S3: {self.endpoint}/{self.bucket}/{self.key}")

        def download_from_s3():
            s3_client = self._create_client()
            return s3_client.get_object(Bucket=self.bucket, Key=self.key)

        retry_config = get_retry_config_from_yaml({"retry": self.retry}, "s3")
        try:
            response = retry_with_backoff(download_from_s3, retry_config, context)
        except Exception as e:
            if not is_retryable_s3_error(e):
                raise
            raise

        csv_content = response["Body"].read().decode("utf-8")

        # Parse CSV with pandas using PyArrow data types
        df = pd.read_csv(StringIO(csv_content), dtype_backend='pyarrow')
        context.log.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

        return df

    def cleanup(self, context: OpExecutionContext) -> None:
        """S3 sources don't create temp tables, so cleanup is a no-op."""
        pass

    def get_schema_prefix(self) -> str:
        """Return the S3 path prefix (bucket/key prefix)."""
        return f"s3://{self.bucket}"

    def get_cleanup_executor(self) -> str:
        """S3 doesn't need cleanup, return empty string."""
        return ""

    def get_connection_config(self) -> dict:
        """Return the connection configuration."""
        return {
            "endpoint": self.endpoint,
            "bucket": self.bucket,
            "access_key": self.access_key,
        }

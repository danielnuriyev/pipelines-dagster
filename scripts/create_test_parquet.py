#!/usr/bin/env python3
"""
Create a Parquet file on S3 with 10 records matching the lakehouse.test.test_d schema.

The Parquet file will have columns:
- id: INTEGER (pandas int64)
- ts: TIMESTAMP (pandas datetime64)

Usage:
    python create_test_parquet.py
"""

import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig


def create_test_parquet():
    """Create and upload a Parquet file with test data to S3."""

    # Create test data matching Trino table schema
    base_timestamp = datetime(2026, 1, 1, 0, 0, 0)

    # Create 10 records with id and timestamp
    data = []
    for i in range(1, 11):  # 1 to 10 inclusive
        record = {
            'id': i,  # INTEGER -> pandas int64
            'ts': base_timestamp + timedelta(seconds=i)  # TIMESTAMP -> pandas datetime64
        }
        data.append(record)

    # Create DataFrame with proper dtypes
    df = pd.DataFrame(data)
    df['id'] = df['id'].astype('int64')  # Ensure INTEGER compatibility
    df['ts'] = pd.to_datetime(df['ts'])  # Ensure TIMESTAMP compatibility

    print(f"Created DataFrame with {len(df)} records:")
    print(df)
    print(f"\nData types:")
    print(df.dtypes)

    # Convert to Parquet
    parquet_buffer = df.to_parquet(index=False)

    # S3 configuration
    s3_endpoint = "http://localhost:9000"  # MinIO endpoint
    s3_bucket = "warehouse"
    s3_key = "test_data/test_d_sample.parquet"
    s3_access_key = "admin"
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    if not s3_secret_key:
        raise ValueError("S3_SECRET_KEY environment variable is required")

    print(f"\nUploading to S3: {s3_endpoint}/{s3_bucket}/{s3_key}")

    # Create S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        config=BotoConfig(signature_version="s3v4"),
    )

    # Upload Parquet file
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=parquet_buffer,
        ContentType="application/octet-stream",
    )

    print(f"Successfully uploaded Parquet file to s3://{s3_bucket}/{s3_key}")
    print(f"File size: {len(parquet_buffer)} bytes")


if __name__ == "__main__":
    create_test_parquet()

"""Integration test for the trino_to_s3 pipeline."""

import os
import time

import boto3
import pytest
import requests
import trino
from botocore.client import Config as BotoConfig

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
GRAPHQL_ENDPOINT = f"{DAGSTER_URL}/graphql"

# Trino configuration
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "test")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "lakehouse")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "test")

# S3/MinIO configuration
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:30900")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "warehouse")
S3_KEY = os.environ.get("S3_KEY", "exports/trino_export.csv")

# Timeout settings
MAX_WAIT_SECONDS = 120
POLL_INTERVAL_SECONDS = 5


def setup_test_data():
    """Create test_a table with sample data if it doesn't exist."""
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cursor = conn.cursor()

    # Create schema if not exists
    print(f"Ensuring schema {TRINO_SCHEMA} exists...")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
    cursor.fetchall()

    # Check if test_a exists
    cursor.execute(f"""
        SELECT table_name FROM {TRINO_CATALOG}.information_schema.tables
        WHERE table_catalog = '{TRINO_CATALOG}'
        AND table_schema = '{TRINO_SCHEMA}'
        AND table_name = 'test_a'
    """)
    table_exists = len(cursor.fetchall()) > 0

    if not table_exists:
        print("Creating test_a table...")
        cursor.execute(
            f"CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.test_a (id INTEGER, ts TIMESTAMP)"
        )
        cursor.fetchall()

        print("Inserting sample data into test_a...")
        cursor.execute(
            f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.test_a VALUES (1, CURRENT_TIMESTAMP)"
        )
        cursor.fetchall()
        print("test_a table created with sample data")
    else:
        print("test_a table already exists")

    cursor.close()
    conn.close()


def graphql_query(query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against Dagster."""
    response = requests.post(
        GRAPHQL_ENDPOINT,
        json={"query": query, "variables": variables or {}},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if not response.ok:
        print(f"GraphQL error: {response.status_code} - {response.text}")
        response.raise_for_status()
    return response.json()


def launch_job(job_name: str) -> str:
    """Launch a job and return the run ID."""
    query = """
    mutation LaunchRun($jobName: String!) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: "pipelines-dagster"
          repositoryName: "__repository__"
          jobName: $jobName
        }
      }) {
        __typename
        ... on LaunchRunSuccess {
          run {
            runId
          }
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    result = graphql_query(query, {"jobName": job_name})

    launch_result = result["data"]["launchRun"]
    if launch_result["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"Failed to launch job: {launch_result}")

    return launch_result["run"]["runId"]


def get_run_status(run_id: str) -> str:
    """Get the status of a run."""
    query = """
    query RunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        __typename
        ... on Run {
          status
        }
        ... on RunNotFoundError {
          message
        }
      }
    }
    """
    result = graphql_query(query, {"runId": run_id})

    run_result = result["data"]["runOrError"]
    if run_result["__typename"] != "Run":
        raise RuntimeError(f"Run not found: {run_result}")

    return run_result["status"]


def wait_for_run_completion(run_id: str, timeout: int = MAX_WAIT_SECONDS) -> str:
    """Wait for a run to complete and return the final status."""
    start_time = time.time()
    terminal_statuses = {"SUCCESS", "FAILURE", "CANCELED"}

    while time.time() - start_time < timeout:
        status = get_run_status(run_id)
        if status in terminal_statuses:
            return status
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Run {run_id} did not complete within {timeout} seconds")


def get_s3_client():
    """Create an S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


def delete_s3_object(bucket: str, key: str):
    """Delete an object from S3 if it exists."""
    s3 = get_s3_client()
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted s3://{bucket}/{key}")
    except Exception as e:
        print(f"Could not delete s3://{bucket}/{key}: {e}")


def get_s3_object(bucket: str, key: str) -> str | None:
    """Get an object from S3, return None if not found."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"Error getting s3://{bucket}/{key}: {e}")
        return None


@pytest.fixture
def test_data():
    """Fixture to set up test data in Trino."""
    print("Setting up test data...")
    setup_test_data()
    print("Test data ready")
    yield


@pytest.fixture
def clean_s3():
    """Fixture to clean up S3 object before and after test."""
    # Clean before test
    delete_s3_object(S3_BUCKET, S3_KEY)
    yield
    # Optionally clean after test (comment out to inspect result)
    # delete_s3_object(S3_BUCKET, S3_KEY)


@pytest.mark.integration
def test_trino_to_s3_exports_csv(test_data, clean_s3):
    """
    Test that trino_to_s3 exports query results to S3 as CSV.

    This test:
    1. Sets up test_a table in Trino (if not exists)
    2. Cleans up any existing CSV file in S3
    3. Launches trino_to_s3 job
    4. Waits for it to complete successfully
    5. Verifies the CSV file was created in S3
    6. Validates the CSV content
    """
    # Launch trino_to_s3 job
    print("Launching trino_to_s3...")
    run_id = launch_job("trino_to_s3")
    print(f"Launched run: {run_id}")

    # Wait for completion
    print("Waiting for trino_to_s3 to complete...")
    status = wait_for_run_completion(run_id)
    print(f"trino_to_s3 completed with status: {status}")
    assert status == "SUCCESS", f"trino_to_s3 failed with status: {status}"

    # Verify CSV was created in S3
    print(f"Checking for CSV at s3://{S3_BUCKET}/{S3_KEY}...")
    csv_content = get_s3_object(S3_BUCKET, S3_KEY)
    assert csv_content is not None, f"CSV file not found at s3://{S3_BUCKET}/{S3_KEY}"

    # Validate CSV content
    print("CSV content:")
    print(csv_content)

    lines = csv_content.strip().split("\n")
    assert len(lines) >= 2, "CSV should have header and at least one data row"

    # Check header
    header = lines[0]
    assert "id" in header.lower(), "CSV should have 'id' column"
    assert "ts" in header.lower(), "CSV should have 'ts' column"

    print("trino_to_s3 integration test passed!")


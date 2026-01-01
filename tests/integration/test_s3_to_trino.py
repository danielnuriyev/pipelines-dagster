"""Integration test for the s3_to_trino pipeline."""

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
S3_KEY = "imports/data.csv"

# Target table
TARGET_TABLE = "s3_data"

# Timeout settings
MAX_WAIT_SECONDS = 120
POLL_INTERVAL_SECONDS = 5


def get_s3_client():
    """Create an S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


def upload_test_csv():
    """Upload a test CSV file to S3."""
    s3 = get_s3_client()

    csv_content = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
"""

    print(f"Uploading test CSV to s3://{S3_BUCKET}/{S3_KEY}...")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )
    print("Test CSV uploaded")


def delete_s3_object():
    """Delete the test CSV from S3."""
    s3 = get_s3_client()
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=S3_KEY)
        print(f"Deleted s3://{S3_BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"Could not delete s3://{S3_BUCKET}/{S3_KEY}: {e}")


def drop_target_table():
    """Drop the target table if it exists."""
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cursor = conn.cursor()

    print(f"Dropping table {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE} if exists...")
    cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE}")
    cursor.fetchall()

    cursor.close()
    conn.close()


def get_table_data() -> list[dict]:
    """Get data from the target table."""
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE}")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]


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
          repositoryLocationName: "s3"
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


@pytest.fixture
def test_data():
    """Fixture to set up test data."""
    print("Setting up test data...")
    # Upload test CSV
    upload_test_csv()
    # Drop target table for clean test
    drop_target_table()
    print("Test data ready")
    yield
    # Cleanup (optional)
    # delete_s3_object()
    # drop_target_table()


@pytest.mark.integration
def test_s3_to_trino_loads_csv(test_data):
    """
    Test that s3_to_trino loads CSV data into Trino.

    This test:
    1. Uploads a test CSV file to S3
    2. Drops the target table if it exists
    3. Launches the s3_to_trino job
    4. Waits for it to complete successfully
    5. Verifies the data was loaded into the Trino table
    """
    # Launch s3_to_trino job
    print("Launching s3_to_trino...")
    run_id = launch_job("s3_to_trino")
    print(f"Launched run: {run_id}")

    # Wait for completion
    print("Waiting for s3_to_trino to complete...")
    status = wait_for_run_completion(run_id)
    print(f"s3_to_trino completed with status: {status}")
    assert status == "SUCCESS", f"s3_to_trino failed with status: {status}"

    # Verify data was loaded
    print(f"Checking data in {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE}...")
    data = get_table_data()
    print(f"Found {len(data)} rows: {data}")

    assert len(data) == 3, f"Expected 3 rows, got {len(data)}"

    # Verify content
    names = {row["name"] for row in data}
    assert names == {"Alice", "Bob", "Charlie"}, f"Unexpected names: {names}"

    print("s3_to_trino integration test passed!")


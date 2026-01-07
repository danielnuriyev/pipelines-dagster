"""
Comprehensive integration test for the full asset pipeline.

This test:
1. Deploys the code to Dagster
2. Disables the schedule of test_a asset
3. Materializes test_a asset and verifies data in test_b table
4. Waits for auto-materialization to trigger test_trino_s3 and verifies S3 data
    5. Waits for auto-materialization to trigger test_s3_trino and verifies data
6. Re-enables the schedule of test_a asset
"""

import csv
import io
import os
import subprocess
import time

import boto3
import pytest
import requests
import trino
from botocore.client import Config as BotoConfig

# Dagster configuration
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
S3_EXPORT_KEY = "exports/test_trino_s3.csv"
S3_IMPORT_KEY = "imports/data.csv"

# Deployment configuration
KIND_CLUSTER = os.environ.get("KIND_CLUSTER", "trino")
DOCKER_IMAGE = "pipelines-dagster:latest"

# Repository configuration
TRINO_LOCATION = "trino"
S3_LOCATION = "s3"
REPOSITORY_NAME = "__repository__"

# Asset keys
ASSET_TEST_B = ["lakehouse", "test", "test_b"]
ASSET_TRINO_EXPORT = ["s3", "warehouse", "exports", "test_trino_s3"]
ASSET_S3_DATA = ["lakehouse", "test", "test_s3_trino"]

# Schedule name
TEST_TRINO_TO_TRINO_SCHEDULE = "test_trino_insert_select_schedule"

# Timeout settings
MAX_WAIT_SECONDS = 180
POLL_INTERVAL_SECONDS = 5


# =============================================================================
# Deployment Functions
# =============================================================================


def deploy_to_dagster():
    """Build Docker image, load to kind, and restart deployments."""
    print("\n=== Deploying to Dagster ===")

    # Build Docker image
    print("Building Docker image...")
    result = subprocess.run(
        ["docker", "build", "-t", DOCKER_IMAGE, "."],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    )
    if result.returncode != 0:
        print(f"Docker build failed: {result.stderr}")
        raise RuntimeError(f"Docker build failed: {result.stderr}")
    print("Docker image built successfully")

    # Load to kind cluster
    print(f"Loading image to kind cluster '{KIND_CLUSTER}'...")
    result = subprocess.run(
        ["kind", "load", "docker-image", DOCKER_IMAGE, "--name", KIND_CLUSTER],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Kind load failed: {result.stderr}")
        raise RuntimeError(f"Kind load failed: {result.stderr}")
    print("Image loaded to kind cluster")

    # Delete existing pods to force restart with new image
    print("Restarting user code deployments...")
    subprocess.run(
        [
            "kubectl",
            "delete",
            "pods",
            "-n",
            "dagster",
            "-l",
            "app.kubernetes.io/name=dagster-user-deployments",
        ],
        capture_output=True,
        text=True,
    )

    # Wait for deployments to be ready
    print("Waiting for deployments to be ready...")
    result = subprocess.run(
        [
            "kubectl",
            "rollout",
            "status",
            "deployment",
            "-n",
            "dagster",
            "-l",
            "app.kubernetes.io/name=dagster-user-deployments",
            "--timeout=180s",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Rollout status: {result.stderr}")
        # Continue anyway, might just be a timeout

    # Wait for pods to be ready
    time.sleep(10)

    # Reload Dagster workspace
    print("Reloading Dagster workspace...")
    graphql_query('mutation { reloadWorkspace { __typename } }')
    time.sleep(5)

    print("Deployment complete!")


# =============================================================================
# GraphQL Functions
# =============================================================================


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


def get_schedule_state_id(schedule_name: str, location: str = TRINO_LOCATION) -> str | None:
    """Get the schedule state ID for stopping a schedule."""
    query = """
    query GetSchedule($scheduleSelector: ScheduleSelector!) {
      scheduleOrError(scheduleSelector: $scheduleSelector) {
        __typename
        ... on Schedule {
          scheduleState {
            id
            status
          }
        }
      }
    }
    """
    variables = {
        "scheduleSelector": {
            "repositoryLocationName": location,
            "repositoryName": REPOSITORY_NAME,
            "scheduleName": schedule_name,
        }
    }
    result = graphql_query(query, variables)
    schedule_result = result["data"]["scheduleOrError"]
    if schedule_result["__typename"] == "Schedule":
        return schedule_result["scheduleState"]["id"]
    return None


def stop_schedule(schedule_name: str, location: str = TRINO_LOCATION) -> bool:
    """Stop a schedule and return True if successful."""
    # First get the schedule state ID
    state_id = get_schedule_state_id(schedule_name, location)
    if not state_id:
        print(f"Could not find schedule {schedule_name}")
        return False

    query = """
    mutation StopSchedule($id: String!) {
      stopRunningSchedule(id: $id) {
        __typename
        ... on ScheduleStateResult {
          scheduleState {
            status
          }
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    variables = {"id": state_id}
    result = graphql_query(query, variables)
    stop_result = result["data"]["stopRunningSchedule"]
    if stop_result["__typename"] == "ScheduleStateResult":
        return stop_result["scheduleState"]["status"] == "STOPPED"
    print(f"Failed to stop schedule: {stop_result}")
    return False


def start_schedule(schedule_name: str, location: str = TRINO_LOCATION) -> bool:
    """Start a schedule and return True if successful."""
    query = """
    mutation StartSchedule($scheduleSelector: ScheduleSelector!) {
      startSchedule(scheduleSelector: $scheduleSelector) {
        __typename
        ... on ScheduleStateResult {
          scheduleState {
            status
          }
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    variables = {
        "scheduleSelector": {
            "repositoryLocationName": location,
            "repositoryName": REPOSITORY_NAME,
            "scheduleName": schedule_name,
        }
    }
    result = graphql_query(query, variables)
    start_result = result["data"]["startSchedule"]
    if start_result["__typename"] == "ScheduleStateResult":
        return start_result["scheduleState"]["status"] == "RUNNING"
    print(f"Failed to start schedule: {start_result}")
    return False


def materialize_asset(asset_key: list[str], location: str) -> str:
    """Materialize an asset and return the run ID."""
    query = """
    mutation LaunchRun($repositoryLocationName: String!, $repositoryName: String!, $assetKeys: [AssetKeyInput!]!) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: $repositoryLocationName
          repositoryName: $repositoryName
          jobName: "__ASSET_JOB"
          assetSelection: $assetKeys
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
          stack
        }
        ... on InvalidSubsetError {
          message
        }
      }
    }
    """
    variables = {
        "repositoryLocationName": location,
        "repositoryName": REPOSITORY_NAME,
        "assetKeys": [{"path": asset_key}],
    }
    result = graphql_query(query, variables)
    launch_result = result["data"]["launchRun"]
    if launch_result["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"Failed to materialize asset: {launch_result}")
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
        print(f"  Run {run_id[:8]}... status: {status}")
        if status in terminal_statuses:
            return status
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Run {run_id} did not complete within {timeout} seconds")


def get_latest_materialization(asset_key: list[str]) -> dict | None:
    """Get the latest materialization event for an asset."""
    query = """
    query AssetMaterialization($assetKey: AssetKeyInput!) {
      assetOrError(assetKey: $assetKey) {
        __typename
        ... on Asset {
          assetMaterializations(limit: 1) {
            runId
            timestamp
          }
        }
      }
    }
    """
    result = graphql_query(query, {"assetKey": {"path": asset_key}})
    asset_result = result["data"]["assetOrError"]
    if asset_result["__typename"] != "Asset":
        return None
    materializations = asset_result.get("assetMaterializations", [])
    return materializations[0] if materializations else None


def wait_for_asset_materialization(
    asset_key: list[str],
    after_timestamp: float,
    timeout: int = MAX_WAIT_SECONDS,
) -> str:
    """Wait for an asset to be materialized after a given timestamp."""
    start_time = time.time()
    asset_name = "/".join(asset_key)

    print(f"Waiting for asset {asset_name} to be materialized...")

    while time.time() - start_time < timeout:
        mat = get_latest_materialization(asset_key)
        if mat:
            # Timestamp is in milliseconds
            mat_time = float(mat["timestamp"]) / 1000
            if mat_time > after_timestamp:
                run_id = mat["runId"]
                print(f"  Found materialization for {asset_name}, run: {run_id[:8]}...")
                # Wait for the run to complete
                status = wait_for_run_completion(run_id, timeout - int(time.time() - start_time))
                return status

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Asset {asset_name} was not materialized within {timeout} seconds")


# =============================================================================
# Trino Functions
# =============================================================================


def get_trino_connection():
    """Get a Trino database connection."""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def setup_test_data():
    """Set up test data in Trino."""
    print("\n=== Setting up test data ===")
    conn = get_trino_connection()
    cursor = conn.cursor()

    # Create schema if not exists
    print(f"Ensuring schema {TRINO_SCHEMA} exists...")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
    cursor.fetchall()

    # Drop and recreate test_a with fresh data
    print("Recreating test_a table with sample data...")
    cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.test_a")
    cursor.fetchall()
    cursor.execute(
        f"CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.test_a (id INTEGER, ts TIMESTAMP)"
    )
    cursor.fetchall()
    cursor.execute(
        f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.test_a VALUES (1, CURRENT_TIMESTAMP), (2, CURRENT_TIMESTAMP)"
    )
    cursor.fetchall()

    # Clean up downstream tables
    print("Cleaning up downstream tables...")
    for table in ["test_b", "test_c", "test_s3_trino"]:
        cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{table}")
        cursor.fetchall()

    cursor.close()
    conn.close()
    print("Test data ready")


def verify_table_data(table_name: str, expected_row_count: int) -> list:
    """Verify data exists in a Trino table and return the rows."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    assert len(rows) >= expected_row_count, (
        f"Expected at least {expected_row_count} rows in {table_name}, got {len(rows)}"
    )
    return rows


# =============================================================================
# S3 Functions
# =============================================================================


def get_s3_client():
    """Get an S3 client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


def cleanup_s3():
    """Clean up S3 test files."""
    print("\n=== Cleaning up S3 ===")
    s3 = get_s3_client()

    for key in [S3_EXPORT_KEY, S3_IMPORT_KEY]:
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            print(f"Deleted s3://{S3_BUCKET}/{key}")
        except Exception:
            pass  # File might not exist


def verify_s3_csv(key: str, expected_row_count: int) -> list:
    """Verify CSV file exists in S3 and return parsed rows."""
    s3 = get_s3_client()

    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    assert len(rows) >= expected_row_count, (
        f"Expected at least {expected_row_count} rows in S3 CSV, got {len(rows)}"
    )
    return rows


# =============================================================================
# Main Test
# =============================================================================


@pytest.fixture(scope="module")
def deployed_dagster():
    """Deploy code to Dagster once for the test module."""
    deploy_to_dagster()
    yield
    # No cleanup needed


@pytest.fixture
def test_environment(deployed_dagster):
    """Set up test environment with fresh data."""
    setup_test_data()
    cleanup_s3()
    yield


@pytest.mark.integration
def test_full_pipeline_chain(test_environment):
    """
    Test the complete asset pipeline:
    1. Stop test_a schedule
    2. Materialize test_a (produces test_b)
    3. Verify test_b data
    4. Wait for auto-materialization of test_trino_s3
    5. Verify S3 export data
    6. Wait for auto-materialization of test_s3_trino
    7. Verify test_s3_trino table
    8. Re-enable test_a schedule
    """
    print("\n" + "=" * 60)
    print("FULL PIPELINE INTEGRATION TEST")
    print("=" * 60)

    # Record timestamp before starting
    before_test = time.time()

    # Step 1: Stop the test_trino_insert_select schedule
    print("\n--- Step 1: Stop test_trino_insert_select schedule ---")
    stopped = stop_schedule(TEST_TRINO_TO_TRINO_SCHEDULE)
    print(f"Schedule stopped: {stopped}")
    # Don't assert - schedule might already be stopped

    try:
        # Step 2: Materialize test_trino_insert_select asset (produces test_b)
        print("\n--- Step 2: Materialize test_trino_insert_select asset ---")
        run_id = materialize_asset(ASSET_TEST_B, TRINO_LOCATION)
        print(f"Launched materialization run: {run_id}")

        status = wait_for_run_completion(run_id)
        assert status == "SUCCESS", f"test_trino_insert_select materialization failed with status: {status}"
        print("test_trino_insert_select materialization completed successfully!")

        # Step 3: Verify test_b data
        print("\n--- Step 3: Verify test_b data ---")
        rows = verify_table_data("test_b", expected_row_count=2)
        print(f"Found {len(rows)} rows in test_b: {rows}")

        # Step 4: Wait for auto-materialization of test_trino_s3
        print("\n--- Step 4: Wait for test_trino_s3 auto-materialization ---")
        status = wait_for_asset_materialization(ASSET_TRINO_EXPORT, before_test)
        assert status == "SUCCESS", f"test_trino_s3 auto-materialization failed with status: {status}"
        print("test_trino_s3 auto-materialization completed successfully!")

        # Step 5: Verify S3 export data
        print("\n--- Step 5: Verify S3 export data ---")
        s3_rows = verify_s3_csv(S3_EXPORT_KEY, expected_row_count=2)
        print(f"Found {len(s3_rows)} rows in S3 export: {s3_rows}")

        # Step 6: Wait for auto-materialization of test_s3_trino
        print("\n--- Step 6: Wait for test_s3_trino auto-materialization ---")
        status = wait_for_asset_materialization(ASSET_S3_DATA, before_test)
        assert status == "SUCCESS", f"test_s3_trino failed with status: {status}"
        print("test_s3_trino completed successfully!")

        # Step 7: Verify test_s3_trino table
        print("\n--- Step 7: Verify test_s3_trino table ---")
        final_rows = verify_table_data("test_s3_trino", expected_row_count=2)
        print(f"Found {len(final_rows)} rows in test_s3_trino: {final_rows}")

        print("\n" + "=" * 60)
        print("ALL VERIFICATIONS PASSED!")
        print("=" * 60)

    finally:
        # Step 8: Re-enable test_trino_insert_select schedule
        print("\n--- Step 8: Re-enable test_trino_insert_select schedule ---")
        started = start_schedule(TEST_TRINO_TO_TRINO_SCHEDULE)
        print(f"Schedule re-enabled: {started}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-m", "integration"])


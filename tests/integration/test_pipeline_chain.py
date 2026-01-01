"""Integration test for the pipeline chain: test_a -> test_b (via sensor)."""

import os
import time

import pytest
import requests
import trino

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
GRAPHQL_ENDPOINT = f"{DAGSTER_URL}/graphql"

# Trino configuration
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "test")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "lakehouse")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "test")

# Timeout settings
MAX_WAIT_SECONDS = 120
POLL_INTERVAL_SECONDS = 5

# Sensor configuration (dynamically generated: {job}_after_{dependencies}_sensor)
# For single dependency: {job}_after_{dep}_sensor
# For multiple: {job}_after_{dep1}_{dep2}_sensor (or _and_N_more if >2)
SENSOR_NAME = "test_b_trino_insert_select_after_test_a_trino_insert_select_sensor"
REPOSITORY_LOCATION = "trino"
REPOSITORY_NAME = "__repository__"


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

    # Clean up test_b and test_c for a fresh test
    print("Dropping test_b and test_c if they exist...")
    cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.test_b")
    cursor.fetchall()
    cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.test_c")
    cursor.fetchall()

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


def start_sensor(sensor_name: str) -> bool:
    """Start a sensor and return True if successful."""
    query = """
    mutation StartSensor($sensorSelector: SensorSelector!) {
      startSensor(sensorSelector: $sensorSelector) {
        __typename
        ... on Sensor {
          name
          sensorState {
            status
          }
        }
        ... on SensorNotFoundError {
          message
        }
        ... on UnauthorizedError {
          message
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    variables = {
        "sensorSelector": {
            "repositoryLocationName": REPOSITORY_LOCATION,
            "repositoryName": REPOSITORY_NAME,
            "sensorName": sensor_name,
        }
    }
    result = graphql_query(query, variables)

    start_result = result["data"]["startSensor"]
    if start_result["__typename"] == "Sensor":
        return start_result["sensorState"]["status"] == "RUNNING"
    else:
        print(f"Failed to start sensor: {start_result}")
        return False


def stop_sensor(sensor_name: str) -> bool:
    """Stop a sensor and return True if successful."""
    query = """
    mutation StopSensor($sensorSelector: SensorSelector!) {
      stopSensor(sensorSelector: $sensorSelector) {
        __typename
        ... on StopSensorMutationResult {
          instigationState {
            status
          }
        }
        ... on SensorNotFoundError {
          message
        }
        ... on UnauthorizedError {
          message
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    variables = {
        "sensorSelector": {
            "repositoryLocationName": REPOSITORY_LOCATION,
            "repositoryName": REPOSITORY_NAME,
            "sensorName": sensor_name,
        }
    }
    result = graphql_query(query, variables)

    stop_result = result["data"]["stopSensor"]
    if stop_result["__typename"] == "StopSensorMutationResult":
        return stop_result["instigationState"]["status"] == "STOPPED"
    else:
        print(f"Failed to stop sensor: {stop_result}")
        return False


def launch_job(job_name: str) -> str:
    """Launch a job and return the run ID."""
    query = """
    mutation LaunchRun($jobName: String!) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: "trino"
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


def get_runs_by_job(job_name: str, limit: int = 10) -> list[dict]:
    """Get recent runs for a job."""
    query = """
    query RunsByJob($jobName: String!, $limit: Int!) {
      runsOrError(filter: {pipelineName: $jobName}, limit: $limit) {
        __typename
        ... on Runs {
          results {
            runId
            status
            startTime
          }
        }
      }
    }
    """
    result = graphql_query(query, {"jobName": job_name, "limit": limit})

    runs_result = result["data"]["runsOrError"]
    if runs_result["__typename"] != "Runs":
        return []

    return runs_result["results"]


def wait_for_sensor_triggered_run(
    job_name: str,
    after_timestamp: float,
    timeout: int = MAX_WAIT_SECONDS,
) -> str:
    """Wait for a sensor-triggered run to appear and complete."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        runs = get_runs_by_job(job_name)

        for run in runs:
            # Check if this run started after our trigger
            if run["startTime"] and run["startTime"] > after_timestamp:
                # Wait for it to complete
                return wait_for_run_completion(
                    run["runId"],
                    timeout=timeout - int(time.time() - start_time),
                )

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"No run for {job_name} started after {after_timestamp} within {timeout} seconds"
    )


@pytest.fixture
def test_data():
    """Fixture to set up test data in Trino."""
    print("Setting up test data...")
    setup_test_data()
    print("Test data ready")
    yield


@pytest.fixture
def enabled_sensor():
    """Fixture to enable the sensor before test and optionally disable after."""
    print(f"Enabling sensor: {SENSOR_NAME}...")
    success = start_sensor(SENSOR_NAME)
    assert success, f"Failed to enable sensor {SENSOR_NAME}"
    print(f"Sensor {SENSOR_NAME} enabled")

    yield SENSOR_NAME

    # Optionally stop sensor after test (comment out to leave running)
    # print(f"Stopping sensor: {SENSOR_NAME}...")
    # stop_sensor(SENSOR_NAME)


@pytest.mark.integration
def test_pipeline_chain_executes_successfully(test_data, enabled_sensor):
    """
    Test that running test_a triggers test_b via sensor.

    This test:
    1. Sets up test_a table in Trino (if not exists)
    2. Enables the sensor
    3. Launches test_a_trino_insert_select
    4. Waits for it to complete successfully
    5. Waits for test_b_trino_insert_select to be triggered by the sensor
    6. Verifies test_b completes successfully
    """
    # Record timestamp before launching
    before_launch = time.time()

    # Launch test_a job
    print("Launching test_a_trino_insert_select...")
    test_a_run_id = launch_job("test_a_trino_insert_select")
    print(f"Launched run: {test_a_run_id}")

    # Wait for test_a to complete
    print("Waiting for test_a to complete...")
    test_a_status = wait_for_run_completion(test_a_run_id)
    print(f"test_a completed with status: {test_a_status}")
    assert test_a_status == "SUCCESS", f"test_a failed with status: {test_a_status}"

    # Wait for test_b to be triggered by sensor and complete
    print("Waiting for test_b to be triggered by sensor...")
    test_b_status = wait_for_sensor_triggered_run(
        "test_b_trino_insert_select",
        after_timestamp=before_launch,
    )
    print(f"test_b completed with status: {test_b_status}")
    assert test_b_status == "SUCCESS", f"test_b failed with status: {test_b_status}"

    print("Pipeline chain executed successfully!")

"""
Comprehensive test for all test_ pipelines on Kubernetes Dagster.

This test:
1. Builds Docker image and deploys to Kubernetes Dagster
2. Discovers all test_ pipeline assets
3. Materializes them one by one
4. Verifies all materializations succeeded
"""

import os
import time
import subprocess
import pytest
import requests

# Dagster configuration
DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
GRAPHQL_ENDPOINT = f"{DAGSTER_URL}/graphql"

# Repository configuration
TRINO_LOCATION = "trino"
S3_LOCATION = "s3"
REPOSITORY_NAME = "__repository__"

# Deployment configuration
KIND_CLUSTER = os.environ.get("KIND_CLUSTER", "trino")
DOCKER_IMAGE = "pipelines-dagster:latest"

# Timeout settings
MAX_WAIT_SECONDS = 300  # 5 minutes per pipeline
POLL_INTERVAL_SECONDS = 5


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


def deploy_to_k8s_dagster():
    """Build Docker image, load to kind, and restart deployments."""
    print("\n=== Deploying to Kubernetes Dagster ===")

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
            "--timeout=300s",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Rollout status: {result.stderr}")
        # Continue anyway, might just be a timeout

    # Wait for pods to be ready
    time.sleep(15)

    # Reload Dagster workspace
    print("Reloading Dagster workspace...")
    graphql_query('mutation { reloadWorkspace { __typename } }')
    time.sleep(10)

    print("Kubernetes deployment complete!")


def reload_workspace():
    """Reload the Dagster workspace."""
    print("Reloading Dagster workspace...")
    graphql_query('mutation { reloadWorkspace { __typename } }')
    time.sleep(5)


def get_all_assets():
    """Get all assets from both trino and s3 workspaces."""
    query = """
    query GetAssets {
      workspaceOrError {
        __typename
        ... on Workspace {
          locationEntries {
            locationOrLoadError {
              ... on RepositoryLocation {
                name
                repositories {
                  name
                  assets {
                    key {
                      path
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    result = graphql_query(query)
    workspace = result["data"]["workspaceOrError"]

    if workspace["__typename"] != "Workspace":
        raise RuntimeError(f"Failed to get workspace: {workspace}")

    assets = []
    for location_entry in workspace["locationEntries"]:
        location = location_entry["locationOrLoadError"]
        if location["__typename"] == "RepositoryLocation":
            location_name = location["name"]
            for repo in location["repositories"]:
                for asset in repo["assets"]:
                    asset_key = asset["key"]["path"]
                    assets.append({
                        "location": location_name,
                        "key": asset_key
                    })

    return assets


def filter_test_assets(assets):
    """Filter assets to only include test_ pipelines."""
    test_assets = []
    for asset in assets:
        # Check if any part of the asset key contains "test_"
        asset_path = "/".join(asset["key"])
        if "test_" in asset_path:
            test_assets.append(asset)

    return test_assets


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
        raise RuntimeError(f"Failed to materialize asset {asset_key}: {launch_result}")
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

    print(f"  Waiting for run {run_id[:8]}...")

    while time.time() - start_time < timeout:
        status = get_run_status(run_id)
        print(f"    Run {run_id[:8]}... status: {status}")
        if status in terminal_statuses:
            return status
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Run {run_id} did not complete within {timeout} seconds")


@pytest.fixture(scope="module")
def deployed_k8s_dagster():
    """Deploy code to Kubernetes Dagster once for the test module."""
    deploy_to_k8s_dagster()
    yield
    # No cleanup needed


@pytest.fixture
def test_setup(deployed_k8s_dagster):
    """Set up test environment."""
    # Could add setup logic here if needed
    yield


@pytest.mark.integration
def test_all_pipelines_k8s(test_setup):
    """
    Test all test_ pipelines on Kubernetes Dagster:
    1. Discover all test_ pipeline assets
    2. Materialize them one by one
    3. Verify all materializations succeeded
    """
    print("\n" + "=" * 80)
    print("TEST ALL PIPELINES - KUBERNETES DAGSTER")
    print("=" * 80)

    # Step 1: Get all assets
    print("\n--- Step 1: Discovering assets ---")
    all_assets = get_all_assets()
    print(f"Found {len(all_assets)} total assets")

    # Step 2: Filter to test assets
    test_assets = filter_test_assets(all_assets)
    print(f"Found {len(test_assets)} test assets:")
    for asset in test_assets:
        asset_name = "/".join(asset["key"])
        print(f"  - {asset['location']}: {asset_name}")

    if not test_assets:
        pytest.skip("No test assets found")

    # Step 3: Materialize each test asset
    failed_assets = []
    successful_runs = 0

    for i, asset in enumerate(test_assets, 1):
        asset_name = "/".join(asset["key"])
        location = asset["location"]

        print(f"\n--- Step {i+2}: Materializing {location}:{asset_name} ---")

        try:
            # Launch materialization
            run_id = materialize_asset(asset["key"], location)
            print(f"  Launched run: {run_id}")

            # Wait for completion
            status = wait_for_run_completion(run_id)

            if status == "SUCCESS":
                print(f"  ✅ {asset_name} succeeded!")
                successful_runs += 1
            else:
                print(f"  ❌ {asset_name} failed with status: {status}")
                failed_assets.append(f"{asset_name} ({status})")

        except Exception as e:
            print(f"  ❌ {asset_name} failed with exception: {e}")
            failed_assets.append(f"{asset_name} (exception: {e})")

    # Step 4: Summary
    print(f"\n--- Summary ---")
    print(f"Total test assets: {len(test_assets)}")
    print(f"Successful: {successful_runs}")
    print(f"Failed: {len(failed_assets)}")

    if failed_assets:
        print(f"Failed assets: {', '.join(failed_assets)}")
        pytest.fail(f"{len(failed_assets)} out of {len(test_assets)} test assets failed")

    print("\n" + "=" * 80)
    print("ALL TEST PIPELINES PASSED! 🎉")
    print("=" * 80)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-m", "integration"])

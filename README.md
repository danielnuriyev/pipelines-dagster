# pipelines-dagster

Dagster pipelines deployed to Kubernetes.

This repository contains user code deployments for [Dagster](https://dagster.io/), which is deployed to Kubernetes using [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster).

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                          │
│                                                                     │
│  ┌──────────────────┐  ┌─────────────────────────────────────────┐ │
│  │  Dagster Core    │  │  User Code Deployments (this repo)      │ │
│  │  (pulumi-dagster)│  │                                          │ │
│  │  ├─ Webserver    │  │  ┌─────────────┐  ┌─────────────────┐   │ │
│  │  ├─ Daemon       │◄─┼──│ trino       │  │ s3              │   │ │
│  │  └─ PostgreSQL   │  │  │ workspace   │  │ workspace       │   │ │
│  │                   │  │  │ (assets)   │  │ (assets)        │   │ │
│  └──────────────────┘  │  └─────────────┘  └─────────────────┘   │ │
│                        └─────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

### Asset Graph

This project uses **Dagster Assets** with auto-materialization for cross-workspace dependencies:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Global Asset Graph                            │
│                                                                         │
│  trino workspace:                           s3 workspace:               │
│                                                                         │
│  ┌──────────────────────┐    ┌─────────────────────────┐                │
│  │ lakehouse/test/test_b│    │ lakehouse/test/         │                │
│  │ ◄── schedule (1 min) │    │ test_c                  │                │
│  └──────────┬───────────┘    └─────────────────────────┘                │
│             │                                                           │
│             ▼                                                           │
│  ┌─────────────────────────┐                                            │
│  │ s3/warehouse/exports/   │                                            │
│  │ trino_export_csv        │                                            │
│  └────────────┬────────────┘                                            │
│               │ (cross-workspace)                                       │
│               ▼                                                         │
│  ┌─────────────────────────┐                                            │
│  │ lakehouse/test/s3_data  │                                            │
│  └─────────────────────────┘                                            │
└─────────────────────────────────────────────────────────────────────────┘
```

- **Auto-materialization**: When `test_b` is materialized, `trino_export_csv` automatically materializes
- **Cross-workspace**: `s3_data` (in s3 workspace) depends on `trino_export_csv` (in trino workspace)

## Setup

### Instance Configuration

Create `dagster.yaml` in your project root to configure global concurrency limits:

```yaml
# Global concurrency limits
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 5

# Operation-level concurrency limits
concurrency:
  default_limit:
    - key: "trino_reads"
      limit: 2
    - key: "trino_writes"
      limit: 2
    - key: "s3_operations"
      limit: 3
```

```bash
# Install dependencies
uv sync

# Install dev dependencies (linting, testing)
uv sync --extra dev

# Install pre-commit hooks
uv run pre-commit install
```


## Pipeline Configuration

Pipelines are defined in YAML files under the `pipelines/` directory. Each pipeline resides in its own subdirectory with `pipeline.yaml` and any associated SQL files co-located for better organization.

### Basic Structure

```yaml
# Asset this pipeline produces
asset_key: ["lakehouse", "test", "table_name"]

# Dependencies (other assets this depends on)
depends_on:
  - ["lakehouse", "test", "upstream_table"]

# Pipeline steps (execute in dependency order)
steps:
  - name: step_name
    executor: executor_name
    inputs: ["input_name"]        # Optional: data from previous step
    outputs: ["output_name"]       # Optional: data for next step
    depends_on: ["other_step"]     # Optional: steps this step depends on
    config:
      # SQL can be specified inline or from file:
      sql_query: SELECT * FROM table  # Inline SQL
      sql_file: sql/my_query.sql     # SQL from file
      # Other executor-specific configuration
      ...

# Optional: Schedule (cron expression)
schedule: "* * * * *"  # Run every minute
```

### Retry Configuration

**Configurable retries** are built into all Trino and S3 operations. Each step can override default retry behavior:

```yaml
steps:
  - name: extract
    executor: trino_extract
    config:
      # ... other config ...
      retry:
        max_attempts: 5      # Number of retry attempts (default: 3)
        base_delay: 3.0      # Initial delay in seconds (default: 2.0 for Trino, 1.0 for S3)
        max_delay: 60.0      # Maximum delay cap in seconds (default: 30.0 for Trino, 20.0 for S3)
        backoff_factor: 2.0  # Exponential backoff multiplier (default: 2.0)
        jitter: true         # Add random jitter to prevent thundering herd (default: true)
```

**Defaults:**
- **Trino operations**: 3 retries, 2s base delay, 30s max delay
- **S3 operations**: 3 retries, 1s base delay, 20s max delay

**Retryable errors:**
- Connection issues and timeouts
- Service temporary unavailability
- Rate limiting and throttling
- Network connectivity problems

### Job Retry Configuration

**Intelligent job-level retries** with failure-aware scaling:

```yaml
# Job retry configuration - simplified YAML
job_retry:
  max_attempts: 3          # Maximum retry attempts (default: 3)
  max_delay: 3600          # Maximum delay cap in seconds (default: 3600)
```

**Hardcoded defaults:**
- `base_delay`: 60 seconds (1 minute)
- `backoff_factor`: 2.0 (exponential backoff)
- `retry_on_memory_failure`: true (OOM failures get memory scaling)
- `memory_multiplier`: 2.0 (double memory for OOM failures)

**Intelligent Failure Detection:**
- **Out of Memory (OOM)**: Automatically retries with doubled memory allocation
- **Pod Deletion**: Retries normally (infrastructure-level issue)
- **Other Failures**: Standard exponential backoff retry

**Sensor-Based Retries:**
- Monitors failed job runs
- Analyzes failure logs to determine cause
- Applies appropriate retry strategy
- Sensors are created automatically for pipelines with `job_retry` config
- **Sensors start in STOPPED status** - enable them in Dagster UI or via API

**Example:**
```yaml
job_retry:
  max_attempts: 5        # Override default 3 attempts
  max_delay: 7200        # Allow up to 2 hours between retries
```

**All other settings use sensible hardcoded defaults for intelligent retry behavior.**

### Concurrency Configuration

Control how many operations run simultaneously:

```yaml
# Job-level concurrency (max concurrent ops in this job)
job_concurrency: 3

# Pipeline steps with concurrency controls
steps:
  - name: extract
    executor: trino_extract
    concurrency_key: "trino_reads"  # Limits concurrent Trino reads
    config: ...

  - name: load
    executor: trino_load
    concurrency_key: "trino_writes"  # Limits concurrent Trino writes
    config: ...
```

### Step Dependencies

**Multi-step pipelines support complex dependency graphs:**

```yaml
steps:
  # Independent steps execute in parallel (when possible)
  - name: extract_users
    executor: trino_extract
    outputs: ["users_df"]

  - name: extract_orders
    executor: trino_extract
    outputs: ["orders_df"]

  # Dependent steps wait for their dependencies
  - name: join_data
    executor: trino_load
    inputs: ["users_df", "orders_df"]
    depends_on: ["extract_users", "extract_orders"]  # Explicit dependencies
    config:
      # Join logic here
```

**Dependency Resolution:**
- Steps without dependencies execute first
- Topological sort ensures proper execution order
- Circular dependencies are detected and rejected
- Steps can depend on multiple other steps

### Available Executors

- **`trino_insert_select`**: Execute INSERT...SELECT in Trino
- **`trino_extract`**: Extract data from Trino into pandas DataFrame
- **`trino_load`**: Load pandas DataFrame into Trino table
- **`trino_to_s3`**: Query Trino and export results to S3 as CSV
- **`s3_to_trino`**: Load CSV from S3 into Trino table
- **`batch_splitter`**: Subdivide a DataFrame into smaller batches
- **`duckdb_sql`**: Execute SQL queries on DataFrames using DuckDB

    ```yaml
    # Inline SQL
    - name: transform
      executor: duckdb_sql
      inputs: ["input_df"]
      outputs: ["output_df"]
      config:
        sql_query: |
          SELECT column1, column2, column1 + column2 as sum_col
          FROM input_df
          WHERE column1 > 10

    # SQL from file (relative to pipeline directory)
    - name: transform
      executor: duckdb_sql
      inputs: ["input_df"]
      outputs: ["output_df"]
      config:
        sql_file: transform_data.sql
    ```

- **`trino_extract`**: Extract data from Trino into pandas DataFrame

    ```yaml
    # Inline SQL
    - name: extract
      executor: trino_extract
      outputs: ["df"]
      config:
        select_query: SELECT * FROM my_table WHERE active = true

    # SQL from file (relative to pipeline directory)
    - name: extract
      executor: trino_extract
      outputs: ["df"]
      config:
        sql_file: extract_active_users.sql
    ```

- **`trino_insert_select`**: Execute INSERT...SELECT in Trino

    ```yaml
    # Inline SQL
    - name: load
      executor: trino_insert_select
      config:
        select_query: SELECT * FROM source_table
        target_catalog: lakehouse
        target_schema: test
        target_table: target_table

    # SQL from file (relative to pipeline directory)
    - name: load
      executor: trino_insert_select
      config:
        sql_file: load_transformed_data.sql
        target_catalog: lakehouse
        target_schema: test
        target_table: target_table
    ```




## Local Development

```bash
# Run Dagster dev server locally
uv run dagster dev -m pipelines_dagster.definitions
```

## Linting

### Python (ruff)

```bash
# Check for linting errors
uv run ruff check src/

# Auto-fix linting errors
uv run ruff check --fix src/

# Format code
uv run ruff format src/
```

### YAML (yamllint)

```bash
# Lint all YAML files recursively
uv run yamllint pipelines/
```

### Pre-commit (runs on git commit)

```bash
# Run all linters on staged files
uv run pre-commit run

# Run all linters on all files
uv run pre-commit run --all-files
```

## Build Docker Image

```bash
# Build the image
docker build -t pipelines-dagster:latest .

# For kind: load the image into the cluster
kind load docker-image pipelines-dagster:latest --name <cluster-name>
```

## Deploy to Kubernetes

This image is deployed as a user code deployment via [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster).

### Full Deployment (first time or infrastructure changes)

```bash
# 1. Build the Docker image
docker build -t pipelines-dagster:latest .

# 2. Load the image into kind cluster
kind load docker-image pipelines-dagster:latest --name <cluster-name>

# 3. Deploy with Pulumi (deploys Dagster + user code)
cd ../pulumi-dagster
PULUMI_CONFIG_PASSPHRASE="" pulumi up --yes
```

### Code-only Deployment (update pipelines without Pulumi)

When you only change pipeline code or YAML configs, you can skip Pulumi and just reload the workspace:

```bash
# 1. Build the new image
docker build -t pipelines-dagster:latest .

# 2. Load the image into kind cluster (replace <cluster-name> with your cluster, e.g., 'trino')
kind load docker-image pipelines-dagster:latest --name <cluster-name>

# 3. Delete user code deployment pods to pick up new image
kubectl delete pods -n dagster -l app.kubernetes.io/name=dagster-user-deployments

# 4. Wait for pods to restart (about 20-30 seconds)
sleep 25

# 5. Reload Dagster workspace
# (Assumes port-forward to localhost:3000 is already running)
curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { reloadWorkspace { __typename } }"}'
```

**One-liner for quick deployments (replace `<cluster-name>`):**

```bash
cd /path/to/pipelines-dagster && \
docker build -t pipelines-dagster:latest . && \
kind load docker-image pipelines-dagster:latest --name <cluster-name> && \
kubectl delete pods -n dagster -l app.kubernetes.io/name=dagster-user-deployments && \
sleep 25 && \
curl -s -X POST http://localhost:3000/graphql -H "Content-Type: application/json" -d '{"query":"mutation { reloadWorkspace { __typename } }"}'
```

**Note:** 
- Replace `<cluster-name>` with your kind cluster name (typically `trino` for this project)
- If port-forward is not running, start it first:
  ```bash
  kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &
  ```

### Verify Deployment

```bash
# Check workspace status
curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ workspaceOrError { __typename ... on Workspace { locationEntries { name } } } }"}' | python3 -m json.tool
```

You should see two workspace locations: `trino` and `s3`.

### Enable Automation (Schedules and Auto-Materialization)

This project uses **Dagster Assets** with auto-materialization policies. Downstream assets are automatically triggered when their dependencies are materialized.

**Auto-Materialization:** Enabled by default via the ASSET daemon. When you materialize an asset, all downstream assets with `AutoMaterializePolicy.eager()` will automatically materialize.

**Schedules:** Start schedules to trigger assets on a cron schedule:

```bash
# Port-forward Dagster if not already running
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &

# Start all schedules
for schedule in $(curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ workspaceOrError { ... on Workspace { locationEntries { locationOrLoadError { ... on RepositoryLocation { repositories { schedules { name } } } } } } } }"}' \
  | python3 -c "import sys,json; data=json.load(sys.stdin); schedules=[s['name'] for loc in data['data']['workspaceOrError']['locationEntries'] for repo in (loc.get('locationOrLoadError',{}).get('repositories') or []) for s in repo.get('schedules',[])]; print('\n'.join(schedules))"); do
  echo "Starting schedule: $schedule"
  curl -s -X POST http://localhost:3000/graphql \
    -H "Content-Type: application/json" \
    -d "{\"query\":\"mutation { startSchedule(scheduleSelector: {scheduleName: \\\"$schedule\\\"}) { ... on ScheduleStateResult { scheduleState { status } } } }\"}"
done
```

Or start a specific schedule:

```bash
# Start the test_trino_to_trino schedule (runs every minute)
curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { startSchedule(scheduleSelector: {repositoryLocationName: \"trino\", repositoryName: \"__repository__\", scheduleName: \"test_trino_to_trino_schedule\"}) { ... on ScheduleStateResult { scheduleState { status } } } }"}'
```

**Verify daemons are healthy:**

```bash
curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ instance { daemonHealth { allDaemonStatuses { daemonType healthy } } } }"}' | python3 -m json.tool
```

The `ASSET` daemon must be healthy for auto-materialization to work.

### Enable Sensors

Sensors for job retries start in STOPPED status. Enable them after deployment:

```bash
# Enable retry sensors
for sensor in $(curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ workspaceOrError { ... on Workspace { sensors { name } } } }"}' \
  | python3 -c "import sys,json; data=json.load(sys.stdin); print('\n'.join([s['name'] for s in data['data']['workspaceOrError']['sensors'] if 'retry_sensor' in s['name']]))"); do
  echo "Enabling sensor: $sensor"
  curl -s -X POST http://localhost:3000/graphql \
    -H "Content-Type: application/json" \
    -d "{\"query\":\"mutation { setSensorStatus(sensorSelector: {sensorName: \\\"$sensor\\\"}, status: RUNNING) { ... on SensorState { status } } }\"}"
done
```

## Data Setup

### Populate Test Data

Before running tests, populate the `test_a` table with sample data:

```bash
# Populate test_a table with 100 records
uv run python scripts/populate_trino.py

# With custom connection settings
uv run python scripts/populate_trino.py --host localhost --port 8080 --user dagster
```

This creates the `lakehouse.test.test_a` table with 100 records containing IDs and timestamps.

## Testing

### Run Integration Tests

The integration test (`tests/integration/test_full_pipeline.py`) performs end-to-end verification of the entire pipeline:

**What it tests:**
1. Deploys updated code to Dagster
2. Materializes the `test_trino_to_trino` asset
3. Verifies cross-workspace auto-materialization (Trino → S3 → Trino)
4. Confirms data integrity throughout the pipeline

**Prerequisites:**
- Running kind cluster with Dagster, Trino, and MinIO
- Port-forwards established
- Test data populated (run `populate_trino.py` first)

**Run the test:**

```bash
# Set up port-forwards (run in background)
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &
kubectl port-forward svc/trino-6f3317f2-trino -n trino 8080:8080 &
kubectl port-forward svc/minio-498506da -n trino 30900:9000 &

# Get MinIO credentials
export S3_SECRET_KEY=$(kubectl get secret minio-498506da -n trino -o jsonpath='{.data.rootPassword}' | base64 -d)

# Run the integration test
S3_SECRET_KEY=$S3_SECRET_KEY uv run pytest tests/integration/test_full_pipeline.py -v -s
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DAGSTER_URL` | `http://localhost:3000` | Dagster webserver URL |
| `TRINO_HOST` | `localhost` | Trino host |
| `TRINO_PORT` | `8080` | Trino port |
| `TRINO_CATALOG` | `lakehouse` | Trino catalog |
| `TRINO_SCHEMA` | `test` | Trino schema |
| `S3_ENDPOINT` | `http://localhost:30900` | MinIO/S3 endpoint |
| `S3_ACCESS_KEY` | `admin` | S3 access key |
| `S3_SECRET_KEY` | (none) | S3 secret key |
| `KIND_CLUSTER` | `trino` | Kind cluster name for deployment |

## Related Repositories

- [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster) - Deploys Dagster infrastructure to Kubernetes

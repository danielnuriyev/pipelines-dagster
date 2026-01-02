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

```bash
# Install dependencies
uv sync

# Install dev dependencies (linting, testing)
uv sync --extra dev

# Install pre-commit hooks
uv run pre-commit install
```


## Pipeline Configuration

Pipelines are defined in YAML files under the `pipelines/` directory. Each pipeline can have multiple steps that are executed in sequence.

### Basic Structure

```yaml
# Asset this pipeline produces
asset_key: ["lakehouse", "test", "table_name"]

# Dependencies (other assets this depends on)
depends_on:
  - ["lakehouse", "test", "upstream_table"]

# Pipeline steps (execute in order)
steps:
  - name: step_name
    executor: executor_name
    inputs: ["input_name"]   # Optional: data from previous step
    outputs: ["output_name"]  # Optional: data for next step
    config:
      # Executor-specific configuration
      ...

# Optional: Schedule (cron expression)
schedule: "* * * * *"  # Run every minute
```

### Available Executors

- **`trino_insert_select`**: Execute INSERT...SELECT in Trino
- **`trino_extract`**: Extract data from Trino into pandas DataFrame
- **`trino_load`**: Load pandas DataFrame into Trino table
- **`trino_to_s3`**: Query Trino and export results to S3 as CSV
- **`s3_to_trino`**: Load CSV from S3 into Trino table
- **`batch_splitter`**: Subdivide a DataFrame into smaller batches




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
# Lint YAML files
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

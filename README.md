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

## Project Structure

```
pipelines-dagster/
├── src/
│   └── pipelines_dagster/
│       ├── __init__.py
│       ├── definitions.py       # Shared pipeline generation logic
│       ├── trino_defs.py        # Dagster definitions for trino workspace
│       ├── s3_defs.py           # Dagster definitions for s3 workspace
│       └── ops/                 # Pipeline executors
│           ├── trino_insert_select.py
│           ├── trino_to_s3.py
│           ├── s3_to_trino.py
│           └── trino_pandas_etl.py
├── pipelines/
│   ├── trino/                   # Trino workspace pipelines
│   │   ├── test_trino_to_trino.yaml
│   │   ├── test_trino_to_s3.yaml
│   │   └── test_trino_pandas_etl.yaml
│   └── s3/                      # S3 workspace pipelines
│       └── test_s3_to_trino.yaml
├── tests/
│   └── integration/
│       └── test_full_pipeline.py  # Comprehensive end-to-end test
├── workspace.yaml               # Dagster workspace configuration
├── Dockerfile
├── pyproject.toml
├── .pre-commit-config.yaml
├── .yamllint.yaml
└── README.md
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
# 1. Build and load the new image
docker build -t pipelines-dagster:latest .
kind load docker-image pipelines-dagster:latest --name <cluster-name>

# 2. Restart user code deployments to pick up new image
kubectl rollout restart deployment -n dagster -l app.kubernetes.io/name=dagster-user-deployments

# 3. Wait for rollout to complete
kubectl rollout status deployment -n dagster -l app.kubernetes.io/name=dagster-user-deployments

# 4. Reload Dagster workspace (optional, for immediate refresh)
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &
curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { reloadWorkspace { __typename } }"}'
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

## Testing

### Integration Tests

The integration test (`test_full_pipeline.py`) performs a complete end-to-end verification:

1. **Deploys code to Dagster** - Builds Docker image, loads to kind, restarts deployments
2. **Disables test_trino_to_trino schedule** - Prevents scheduled runs during testing
3. **Materializes test_trino_to_trino asset** - Creates `test_b` table from `test_a`
4. **Verifies test_b data** - Confirms data in Trino
5. **Waits for test_trino_to_s3 auto-materialization** - Triggered by test_b completion
6. **Verifies S3 export** - Confirms CSV file in MinIO
7. **Waits for test_s3_to_trino auto-materialization** - Triggered by S3 export
8. **Verifies s3_data table** - Confirms data loaded back to Trino
9. **Re-enables test_trino_to_trino schedule** - Restores normal operation

**Prerequisites:**
- Kind cluster running with Dagster and Trino deployed
- Port-forwards set up for Dagster, Trino, and MinIO

```bash
# Set up port-forwards
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &
kubectl port-forward svc/trino-6f3317f2-trino -n trino 8080:8080 &
kubectl port-forward svc/minio-498506da -n trino 30900:9000 &

# Get MinIO secret key
export S3_SECRET_KEY=$(kubectl get secret minio-498506da -n trino -o jsonpath='{.data.rootPassword}' | base64 -d)

# Run integration test
S3_SECRET_KEY=$S3_SECRET_KEY uv run pytest tests/integration/ -v -s -m integration
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

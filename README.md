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
│                        │                                          │ │
│                        │  ┌─────────────────┐                     │ │
│                        │  │ snowflake       │                     │ │
│                        │  │ emulator        │                     │ │
│                        │  └─────────────────┘                     │ │
│                        └─────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

### Asset Graph

This project uses **Dagster Assets** with auto-materialization for cross-workspace dependencies:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            Global Asset Graph                                 │
│                                                                                │
│  trino workspace:                               s3 workspace:                  │
│                                                                                │
│  ┌────────────────────────────┐    ┌──────────────────────────────────┐        │
│  │ lakehouse/test/test_a      │    │ lakehouse/test/test_trino_*      │        │
│  │ (source data)              │    │ (various pipeline outputs)       │        │
│  └────────────┬───────────────┘    └─────────────────┬────────────────┘        │
│               │                                     │                         │
│               ▼                                     ▼                         │
│  ┌────────────────────────────┐    ┌──────────────────────────────────┐        │
│  │ s3/warehouse/exports/      │    │ lakehouse/test/test_trino_*      │        │
│  │ test_trino_*.csv           │    │ (parallel targets)               │        │
│  └────────────────────────────┘    └─────────────────┬────────────────┘        │
│                                                     │                         │
│                                                     ▼                         │
│                                    ┌──────────────────────────────────┐        │
│                                    │ lakehouse/test/s3_data           │        │
│                                    │ (cross-workspace dependency)     │        │
│                                    └──────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

- **Auto-materialization**: When upstream assets are materialized, dependent assets automatically materialize
- **Cross-workspace**: Assets in different workspaces can depend on each other
- **Parallel targets**: Pipelines can output to multiple destinations (Trino + S3) simultaneously
- **Batch processing**: Large datasets are processed in batches with fan-in operations

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
    - key: "s3_writes"
      limit: 3
    - key: "data_processing"
      limit: 4
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

### Central Configuration & Jinja2 Templating

Pipeline configurations use **centralized configuration management** via `pipelines/config.yaml` combined with **Jinja2 templating** for dynamic configuration. All pipeline YAML files support full Jinja2 templating with access to configuration values and environment variables.

**Configuration Structure:**
```yaml
# pipelines/config.yaml
trino:
  host: trino-0a966bea-trino.trino.svc.cluster.local
  port: 8080
  user: dagster
  catalog: lakehouse
  schema: test

minio:
  host: minio-ec2bcee8.trino.svc.cluster.local
  port: 9000
  bucket: warehouse
  access_key: admin
```

**Jinja2 Template Usage:**
Pipeline YAML files use standard Jinja2 syntax to access configuration values:

```yaml
# pipelines/my_pipeline/my_pipeline.yaml
steps:
  - name: extract
    executor: trino_extract
    config:
      host: {{ trino.host }}                    # From config.yaml
      port: {{ trino.port }}
      user: {{ trino.user }}
      select_query: SELECT * FROM {{ trino.catalog }}.{{ trino.schema }}.my_table
      target_catalog: {{ trino.catalog }}
      target_schema: {{ trino.schema }}

  - name: upload
    executor: dataframe_to_s3
    config:
      s3_endpoint: http://{{ minio.host }}:{{ minio.port }}
      s3_bucket: {{ minio.bucket }}
      s3_access_key: {{ minio.access_key }}
```

**Template Context Available:**
- `trino.*`: All Trino configuration values
- `minio.*`: All MinIO configuration values
- `config.*`: Access to entire config structure
- All environment variables (`os.environ`)

**Advanced Templating:**
```yaml
steps:
  - name: dynamic_extract
    executor: trino_extract
    config:
      # Environment variable override with fallback to config
      host: {{ TRINO_HOST | default(trino.host) }}
      # Custom logic and filters
      select_query: SELECT * FROM {{ trino.catalog }}.{{ trino.schema }}.{{ TABLE_NAME | default('default_table') | upper }}
      # Conditional configuration
      {% if ENVIRONMENT == 'production' %}
      retries: 5
      {% else %}
      retries: 1
      {% endif %}
```

**Environment Variables:**
- `TRINO_HOST`: Override Trino server hostname
- `TRINO_PORT`: Override Trino server port
- `TRINO_USER`: Override Trino username
- `TRINO_CATALOG`: Override default Trino catalog
- `TRINO_SCHEMA`: Override default Trino schema
- `S3_SECRET_KEY`: MinIO secret key (not in config.yaml for security)
- `ENVIRONMENT`: Environment name for conditional logic
- `TABLE_NAME`: Dynamic table name

**Example Usage:**
```bash
# Override configuration for different environments
export TRINO_HOST=my-production-trino.com
export TRINO_CATALOG=production_catalog
export ENVIRONMENT=production

# Run Dagster with custom configuration
uv run dagster dev
```

### Basic Structure

```yaml
# Asset this pipeline produces (single asset)
asset_key: ["lakehouse", "test", "table_name"]

# OR: Multiple assets this pipeline produces (parallel targets)
asset_keys:
  - ["lakehouse", "test", "table_name"]
  - ["s3", "warehouse", "exports", "file_name"]

# Dependencies (asset keys this depends on)
depends_on:
  - ["lakehouse", "test", "upstream_table"]

# Job-level concurrency (max concurrent ops in this job)
job_concurrency: 4  # Default: unlimited, set to limit parallel execution

# Job retry configuration (optional)
job_retry:
  max_attempts: 3          # Maximum retry attempts (default: 3)
  max_delay: 3600          # Maximum delay cap in seconds (default: 3600)

# Pipeline steps (execute in dependency order)
steps:
  - name: step_name
    executor: executor_name
    inputs: ["input_name"]        # Data from previous step(s)
    outputs: ["output_name"]      # Data for next step(s)
    depends_on: ["other_step"]    # Explicit step dependencies
    concurrency_key: "op_type"    # Concurrency limit for this step type
    config:
      # SQL can be specified inline or from file:
      sql_query: SELECT * FROM table  # Inline SQL
      sql_file: sql/my_query.sql     # SQL from file (relative to pipeline directory)
      # Other executor-specific configuration
      retry:                        # Optional: step-level retry configuration
        max_attempts: 3
        base_delay: 1.0
        max_delay: 60.0

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
# Global concurrency limits (in dagster.yaml)
concurrency:
  default_limit:
    - key: "trino_reads"
      limit: 2
    - key: "trino_writes"
      limit: 2
    - key: "s3_writes"
      limit: 3
    - key: "data_processing"
      limit: 4

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

  - name: process
    executor: duckdb_sql
    concurrency_key: "data_processing"  # Limits concurrent data processing
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
- **`trino_extract`**: Extract data from Trino into pandas DataFrame (supports batching)
- **`trino_load`**: Load pandas DataFrame into Trino table
- **`trino_to_s3`**: Query Trino and export results to S3 as CSV
- **`dataframe_to_s3`**: Upload pandas DataFrame to S3 as CSV

    ```yaml
    # Upload DataFrame to S3
    - name: upload_to_s3
      executor: dataframe_to_s3
      inputs: ["df"]
      outputs: ["s3_result"]
      config:
        s3_endpoint: http://minio-cluster.svc.cluster.local:9000
        s3_bucket: warehouse
        s3_key: exports/my_data.csv
        s3_access_key: admin
        # s3_secret_key provided via environment variable
    ```
- **`s3_to_trino`**: Load CSV from S3 into Trino table
- **`duckdb_sql`**: Execute SQL queries on DataFrames using DuckDB
- **`batch_splitter`**: Subdivide a DataFrame into smaller batches for nested batching

    ```yaml
    # Further subdivide batches (nested batching)
    - name: sub_batch
      executor: batch_splitter
      inputs: ["df"]
      outputs: ["sub_batch_df"]
      depends_on: ["extract"]
      config:
        batch_size: 100  # Subdivide into 100-row batches
        pk: id           # Primary key for consistent ordering
    ```

- **`batch_fan_in`**: Combine multiple DataFrames from parallel batches into one

    ```yaml
    # Combine results from parallel processing
    - name: combine_results
      executor: batch_fan_in
      inputs: ["processed_df"]
      outputs: ["combined_data"]
      depends_on: ["process_batch"]  # Depends on step that processes batches
    ```

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

- **`trino_extract`**: Extract data from Trino into pandas DataFrame (supports batching)

    ```yaml
    # Simple extraction
    - name: extract
      executor: trino_extract
      outputs: ["df"]
      config:
        select_query: SELECT * FROM my_table WHERE active = true

    # Batching extraction (creates dynamic outputs)
    - name: extract
      executor: trino_extract
      outputs: ["df"]  # This becomes a dynamic output when batching
      config:
        select_query: SELECT * FROM large_table ORDER BY id
        batch_size: 1000  # Process in batches of 1000 rows
        pk: id           # Primary key for ordering/batching
    ```

### Advanced Pipeline Patterns

#### Parallel Targets (Multi-Asset Pipelines)

Create pipelines that output to multiple destinations in parallel:

```yaml
# Pipeline that loads to both Trino and S3 simultaneously
asset_keys:
  - ["lakehouse", "test", "parallel_table"]
  - ["s3", "warehouse", "exports", "parallel_data"]

job_concurrency: 4  # Allow parallel execution

steps:
  - name: extract
    executor: trino_extract
    outputs: ["df"]
    concurrency_key: "trino_reads"

  - name: load_trino
    executor: trino_load
    inputs: ["df"]
    outputs: ["trino_result"]
    depends_on: ["extract"]  # Parallel to S3 load
    concurrency_key: "trino_writes"

  - name: load_s3
    executor: dataframe_to_s3
    inputs: ["df"]
    outputs: ["s3_result"]
    depends_on: ["extract"]  # Parallel to Trino load
    concurrency_key: "s3_writes"
```

#### Fan-In Operations (Batch Processing)

Process large datasets in batches, then combine results:

```yaml
# Extract → Process in batches → Combine → Load
asset_key: ["lakehouse", "test", "batch_processed"]

job_concurrency: 2

steps:
  - name: extract
    executor: trino_extract
    outputs: ["df"]
    concurrency_key: "trino_reads"
    config:
      select_query: SELECT * FROM large_table ORDER BY id
      batch_size: 1000  # Process in 1000-row batches
      pk: id

  - name: process_batch
    executor: duckdb_sql
    inputs: ["df"]
    outputs: ["processed_df"]
    depends_on: ["extract"]
    concurrency_key: "data_processing"
    config:
      sql_query: SELECT id, UPPER(name) as name, amount * 1.1 as adjusted_amount FROM input_df

  - name: combine_results
    executor: batch_fan_in
    inputs: ["processed_df"]
    outputs: ["combined_data"]
    depends_on: ["process_batch"]

  - name: load_final
    executor: trino_load
    inputs: ["combined_data"]
    depends_on: ["combine_results"]
    concurrency_key: "trino_writes"
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
uv run dagster dev
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

When you only change pipeline code or YAML configs, you can skip Pulumi and just reload the workspace.

If port-forward is not running, start it first:
```bash
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &
```

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
# Start the test_trino_insert_select schedule (runs every minute)
curl -s -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { startSchedule(scheduleSelector: {repositoryLocationName: \"trino\", repositoryName: \"__repository__\", scheduleName: \"test_trino_insert_select\"}) { ... on ScheduleStateResult { scheduleState { status } } }"}'
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

### Deploy Snowflake Emulator

For detailed instructions on deploying and using the [nnnkkk7/snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator), see [snowflake.md](snowflake.md).

#### Quick Start

```bash
# 1. Clone and build the emulator
cd /Users/danielnuriyev/projects
git clone https://github.com/nnnkkk7/snowflake-emulator.git
cd snowflake-emulator
docker build -t snowflake-emulator:latest .

# 2. Load image into Kind cluster
kind load docker-image snowflake-emulator:latest --name trino

# 3. Deploy to Kubernetes
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: snowflake-emulator
  namespace: trino
spec:
  replicas: 1
  selector:
    matchLabels:
      app: snowflake-emulator
  template:
    metadata:
      labels:
        app: snowflake-emulator
    spec:
      containers:
      - name: snowflake-emulator
        image: snowflake-emulator:latest
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: snowflake-emulator
  namespace: trino
spec:
  selector:
    app: snowflake-emulator
  ports:
  - port: 8081
    targetPort: 8080
  type: ClusterIP
EOF

# 4. Port-forward for local access
kubectl port-forward svc/snowflake-emulator -n trino 8088:8081 &

# 5. Populate test data
uv run python scripts/populate_snowflake.py
```

**Note:** We build and load the image locally to avoid registry authentication issues. Port `8081` is used for the service to avoid conflicts with Trino's port `8080`. Local port `8088` is used for port-forwarding.

### Populate Test Data

Before running tests, populate the `test_a` table with sample data:

#### Trino
```bash
# Populate test_a table with 100 records
uv run python scripts/populate_trino.py

# With custom connection settings
uv run python scripts/populate_trino.py --host localhost --port 8080 --user dagster
```

This creates the `lakehouse.test.test_a` table with 100 records containing IDs and timestamps.

#### Snowflake
```bash
# Populate test_a table with 100 records (requires Snowflake emulator/server to be running)
# Note: Official Snowflake emulator not currently available - see deployment section above
uv run python scripts/populate_snowflake.py

# With custom connection settings
uv run python scripts/populate_snowflake.py --host localhost --port 8088 --user snowflake --password snowflake
```

This creates the `SNOWFLAKE.PUBLIC.test_a` table with 100 records containing IDs and timestamps.

**Current Status:** The official Snowflake emulator image is not available. Use LocalStack or alternative testing approaches as documented above.

## Testing

### Run Integration Tests

### Integration Tests

The project includes comprehensive integration tests for all pipeline functionality:

#### Full Pipeline Chain Test
`tests/integration/test_full_pipeline.py` performs end-to-end verification of the cross-workspace pipeline:

**What it tests:**
1. Deploys updated code to Dagster
2. Materializes the `test_trino_insert_select` asset
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
kubectl port-forward svc/trino-0a966bea-trino -n trino 8080:8080 &
kubectl port-forward svc/minio-ec2bcee8 -n trino 30900:9000 &
kubectl port-forward svc/snowflake-emulator -n trino 8088:8088 &

# Get MinIO credentials
export S3_SECRET_KEY=$(kubectl get secret minio-ec2bcee8 -n trino -o jsonpath='{.data.rootPassword}' | base64 -d)

# Run the integration test
S3_SECRET_KEY=$S3_SECRET_KEY uv run pytest tests/integration/test_full_pipeline.py -v -s
```

#### All Pipelines Tests

**Local Dagster Test** (`tests/integration/test_all_pipelines_local.py`):
- Deploys all pipelines to local Dagster instance
- Discovers and tests all `test_*` pipeline assets
- Verifies each pipeline materializes successfully

**Kubernetes Dagster Test** (`tests/integration/test_all_pipelines_k8s.py`):
- Builds Docker image and deploys to Kubernetes Dagster
- Discovers and tests all `test_*` pipeline assets
- Verifies each pipeline materializes successfully

**Run all pipelines tests:**
```bash
# For local Dagster (assuming dev server is running)
uv run pytest tests/integration/test_all_pipelines_local.py -v -s -m integration

# For Kubernetes Dagster
uv run pytest tests/integration/test_all_pipelines_k8s.py -v -s -m integration
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DAGSTER_URL` | `http://localhost:3000` | Dagster webserver URL |
| `TRINO_HOST` | `localhost` | Trino host |
| `TRINO_PORT` | `8080` | Trino port |
| `TRINO_CATALOG` | `lakehouse` | Trino catalog |
| `TRINO_SCHEMA` | `test` | Trino schema |
| `SNOWFLAKE_HOST` | `localhost` | Snowflake emulator host |
| `SNOWFLAKE_PORT` | `8088` | Snowflake emulator port |
| `SNOWFLAKE_USER` | `snowflake` | Snowflake user |
| `SNOWFLAKE_PASSWORD` | `snowflake` | Snowflake password |
| `SNOWFLAKE_DATABASE` | `SNOWFLAKE` | Snowflake database |
| `SNOWFLAKE_SCHEMA` | `PUBLIC` | Snowflake schema |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` | Snowflake warehouse |
| `S3_ENDPOINT` | `http://localhost:30900` | MinIO/S3 endpoint |
| `S3_ACCESS_KEY` | `admin` | S3 access key |
| `S3_SECRET_KEY` | (none) | S3 secret key |
| `KIND_CLUSTER` | `trino` | Kind cluster name for deployment |

## Related Repositories

- [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster) - Deploys Dagster infrastructure to Kubernetes

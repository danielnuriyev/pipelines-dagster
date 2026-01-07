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
# Dagster instance configuration
# This file controls global Dagster settings like concurrency limits

# Concurrency limits for runs and tagged operations
concurrency:
  runs:
    max_concurrent_runs: 1  # Maximum concurrent job runs across the instance
    tag_concurrency_limits:
      # Database operations
      - key: "trino_extract"
        limit: 2  # Max concurrent Trino extract operations
      - key: "trino_load"
        limit: 2  # Max concurrent Trino load operations
      - key: "trino_insert_select"
        limit: 1  # Max concurrent Trino insert/select operations
      - key: "snowflake_extract"
        limit: 2  # Max concurrent Snowflake extract operations
      - key: "snowflake_load"
        limit: 2  # Max concurrent Snowflake load operations

      # S3 operations
      - key: "s3_extract"
        limit: 2  # Max concurrent S3 extract operations
      - key: "dataframe_to_s3"
        limit: 2  # Max concurrent S3 upload operations

      # Processing operations
      - key: "duckdb_sql"
        limit: 4  # Max concurrent DuckDB operations
      - key: "batch_fan_in"
        limit: 2  # Max concurrent batch fan-in operations

      # Legacy keys (keeping for compatibility)
      - key: "trino_reads"
        limit: 1  # Legacy Trino reads limit
      - key: "trino_writes"
        limit: 1  # Legacy Trino writes limit
      - key: "snowflake_reads"
        limit: 1  # Legacy Snowflake reads limit
      - key: "snowflake_writes"
        limit: 1  # Legacy Snowflake writes limit
      - key: "s3_operations"
        limit: 1  # Legacy S3 operations limit
      - key: "data_processing"
        limit: 2  # Legacy data processing limit
      - key: "s3_writes"
        limit: 1  # Legacy S3 writes limit
```

#### Configuration Breakdown

**Run Concurrency:**
- `max_concurrent_runs`: Maximum number of job runs that can execute simultaneously across the entire Dagster instance

**Tag Concurrency Limits:**
- `tag_concurrency_limits`: Array of concurrency rules based on operation tags
- Each rule specifies a `key` (tag name) and `limit` (max concurrent operations)
- Operations with matching `concurrency_key` values are subject to these limits

**Concurrency Key Matching:**
- Pipeline steps use `concurrency_key: "executor_name"` to match these limits
- Keys correspond to executor operation types for resource management
- Legacy keys maintained for backward compatibility during transition

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

Pipeline configurations use **centralized configuration management** via `pipelines/config.yaml` combined with **Jinja2 templating** for dynamic configuration. All pipeline YAML files support full Jinja2 templating with access to configuration values.

**Configuration Structure:**
```yaml
# pipelines/config.yaml
# Central configuration for all pipelines
# These values are available as Jinja2 variables in pipeline YAML files

# Trino configuration
trino:
  host: trino-0a966bea-trino.trino.svc.cluster.local
  port: 8080
  user: dagster
  catalog: lakehouse
  schema: test

# Snowflake emulator configuration
snowflake:
  host: snowflake-emulator.trino.svc.cluster.local
  port: 8088
  user: snowflake
  password: snowflake
  database: SNOWFLAKE
  schema: PUBLIC
  warehouse: COMPUTE_WH

# MinIO/S3 configuration
minio:
  host: minio-ec2bcee8.trino.svc.cluster.local
  port: 9000
  bucket: warehouse
  access_key: admin
  # secret_key should be provided via environment variable S3_SECRET_KEY
```

**Security Note:**
- Sensitive values like passwords and secret keys should **not** be stored in `config.yaml`
- Use environment variables or secure secret management systems instead
- The `secret_key` field is intentionally omitted from config files

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
      catalog: {{ trino.catalog }}
      schema: {{ trino.schema }}

  - name: upload
    executor: dataframe_to_s3
    config:
      endpoint: http://{{ minio.host }}:{{ minio.port }}
      bucket: {{ minio.bucket }}
      key: exports/my_data.csv
      access_key: {{ minio.access_key }}
```

**Template Context Available:**
- `trino.*`: All Trino configuration values
- `snowflake.*`: All Snowflake configuration values
- `minio.*`: All MinIO configuration values
- `config.*`: Access to entire config structure

**Advanced Templating:**
```yaml
# pipelines/advanced_pipeline/advanced_pipeline.yaml
steps:
  - name: extract
    executor: trino_extract
    config:
      # Use config values with defaults
      host: {{ trino.host }}
      port: {{ trino.port }}
      user: {{ trino.user }}
      select_query: SELECT * FROM {{ trino.catalog }}.{{ trino.schema }}.my_table
```

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
  max_delay: 1h            # Maximum delay cap (default: 1h, supports: s/m/h/d/w)

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
        base_delay: 1s             # Initial delay (supports: s/m/h/d/w)
        max_delay: 1m              # Maximum delay cap (supports: s/m/h/d/w)

# Optional: Schedule (cron expression)
schedule: "* * * * *"  # Run every minute
```

### Complete YAML Configuration Reference

#### Pipeline-Level Configuration

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `asset_key` | string[] | ✅ | Asset key this pipeline produces (e.g., `["workspace", "schema", "table"]`) |
| `asset_keys` | string[][] | ❌ | Multiple asset keys for parallel targets |
| `depends_on` | string[][] | ❌ | Asset keys this pipeline depends on |
| `job_concurrency` | number | ❌ | Max concurrent operations in this job (default: unlimited) |
| `job_retry` | object | ❌ | Job-level retry configuration |
| `schedule` | string | ❌ | Cron expression for scheduled runs |

#### Job Retry Configuration

```yaml
job_retry:
  max_attempts: 3          # Maximum retry attempts (default: 3)
  max_delay: 1h            # Maximum delay cap with time units (default: 1h)
```

#### Step-Level Configuration

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `name` | string | ✅ | Unique step name within pipeline |
| `executor` | string | ✅ | Executor function to run (see Available Executors) |
| `inputs` | string[] | ❌ | Data inputs from previous steps |
| `outputs` | string[] | ❌ | Data outputs for next steps |
| `depends_on` | string[] | ❌ | Explicit step dependencies by name |
| `concurrency_key` | string | ❌ | Concurrency limit key (matches executor name) |
| `config` | object | ✅ | Executor-specific configuration |

#### Step Config Options

**SQL Configuration (for extract/load operations):**
```yaml
config:
  # SQL can be specified inline or from file
  sql_query: SELECT * FROM table  # Inline SQL (supports multi-line YAML)
  sql_file: sql/my_query.sql      # SQL from file (relative to pipeline directory)

  # OR for SELECT queries:
  select_query: SELECT * FROM table  # Alternative field name
```

**Time Configuration (for retry):**
```yaml
config:
  retry:
    max_attempts: 3
    base_delay: 1s    # Initial delay (supports: s/m/h/d/w)
    max_delay: 1m     # Maximum delay (supports: s/m/h/d/w)
    backoff_factor: 2.0
    jitter: true
```

**Database Connection (Trino):**
```yaml
config:
  host: trino-host.com
  port: 8080
  user: username
  # ... additional connection params
```

**Database Connection (Snowflake):**
```yaml
config:
  account: account.region
  user: username
  password: password
  warehouse: warehouse_name
  database: database_name
  schema: schema_name
```

**S3 Configuration:**
```yaml
config:
  endpoint: http://minio-host:9000
  bucket: bucket_name
  key: path/to/file.csv
  access_key: access_key
  # s3_secret_key from environment variable
```

**Batching Configuration:**
```yaml
config:
  batch_size: 1000    # Process in batches of 1000
  pk: id             # Primary key column for batching
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
        base_delay: 3s       # Initial delay (default: 2s for Trino/Snowflake, 1s for S3)
        max_delay: 1m        # Maximum delay cap (default: 30s for Trino/Snowflake, 20s for S3)
        backoff_factor: 2.0  # Exponential backoff multiplier (default: 2.0)
        jitter: true         # Add random jitter to prevent thundering herd (default: true)
```

**Defaults:**
- **Trino operations**: 3 retries, 2s base delay, 30s max delay
- **S3 operations**: 3 retries, 1s base delay, 20s max delay

**Time Format:**
- Supports units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks)
- Examples: `30s`, `5m`, `1h`, `2d`, `1w`

### YAML Configuration Best Practices

#### 1. Use Central Configuration
Store connection details in `pipelines/config.yaml` and reference via Jinja2:

```yaml
# config.yaml
trino:
  host: trino-cluster.company.com
  catalog: analytics
  schema: prod

# pipeline.yaml
config:
  host: {{ trino.host }}
  catalog: {{ trino.catalog }}
  schema: {{ trino.schema }}
```

#### 2. Organize Related Files
Keep pipeline YAML and associated SQL files together:

```
pipelines/my_pipeline/
├── my_pipeline.yaml
├── extract_users.sql
├── transform_data.sql
└── load_results.sql
```

#### 3. Use Descriptive Names
Choose clear, descriptive names for assets and steps:

```yaml
asset_key: ["analytics", "prod", "customer_summary_daily"]
steps:
  - name: extract_customer_data
  - name: calculate_summary_metrics
  - name: load_summary_table
```

#### 4. Set Appropriate Concurrency
Configure concurrency limits based on resource constraints:

```yaml
# Limit concurrent database operations
concurrency_key: "database_reads"

# Job-level concurrency for complex pipelines
job_concurrency: 3
```

#### 5. Configure Retries Thoughtfully
Set retry parameters based on operation characteristics:

```yaml
# Fast operations
retry:
  base_delay: 1s
  max_delay: 30s

# Slow operations
retry:
  base_delay: 30s
  max_delay: 5m
```

#### 6. Use Relative SQL File Paths
SQL files are resolved relative to the pipeline directory:

```yaml
config:
  sql_file: sql/extract_data.sql  # Relative to pipeline directory
```

#### 7. Document Complex Configurations
Add comments for non-obvious configurations:

```yaml
job_concurrency: 2  # Limit due to memory constraints
concurrency_key: "gpu_operations"  # GPU-accelerated processing
```

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
  max_delay: 1h            # Maximum delay cap (default: 1h, supports time units)
```

**Service-Specific Defaults:**

| Service | base_delay | max_delay | Description |
|---------|------------|-----------|-------------|
| **Trino** | `2s` | `30s` | Database queries and connections |
| **S3** | `1s` | `20s` | File uploads/downloads |
| **Snowflake** | `2s` | `30s` | Database queries and connections |

**Common Settings:**
- `max_attempts`: 3 (configurable)
- `backoff_factor`: 2.0 (exponential backoff)
- `jitter`: true (prevents thundering herd)

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

Control how many operations run simultaneously across the Dagster instance:

#### Instance-Level Concurrency (dagster.yaml)

Configure global concurrency limits in `dagster.yaml`:

```yaml
# Concurrency limits for runs and tagged operations
concurrency:
  runs:
    max_concurrent_runs: 1  # Maximum concurrent job runs across the instance
    tag_concurrency_limits:
      # Database operations
      - key: "trino_extract"
        limit: 2  # Max concurrent Trino extract operations
      - key: "trino_load"
        limit: 2  # Max concurrent Trino load operations
      - key: "trino_insert_select"
        limit: 1  # Max concurrent Trino insert/select operations
      - key: "snowflake_extract"
        limit: 2  # Max concurrent Snowflake extract operations
      - key: "snowflake_load"
        limit: 2  # Max concurrent Snowflake load operations

      # S3 operations
      - key: "s3_extract"
        limit: 2  # Max concurrent S3 extract operations
      - key: "dataframe_to_s3"
        limit: 2  # Max concurrent S3 upload operations

      # Processing operations
      - key: "duckdb_sql"
        limit: 4  # Max concurrent DuckDB operations
      - key: "batch_fan_in"
        limit: 2  # Max concurrent batch fan-in operations
```

#### Pipeline-Level Concurrency

Control concurrency within individual pipelines:

```yaml
# Job-level concurrency (max concurrent ops in this job)
job_concurrency: 3

# Pipeline steps with concurrency controls
steps:
  - name: extract
    executor: trino_extract
    concurrency_key: "trino_extract"  # Matches dagster.yaml tag limit
    config: ...

  - name: load
    executor: trino_load
    concurrency_key: "trino_load"  # Matches dagster.yaml tag limit
    config: ...

  - name: process
    executor: duckdb_sql
    concurrency_key: "duckdb_sql"  # Matches dagster.yaml tag limit
    config: ...
```

#### How Concurrency Works

1. **Instance Level**: `tag_concurrency_limits` in `dagster.yaml` set global limits for operation types
2. **Pipeline Level**: `concurrency_key` in pipeline steps tags operations for concurrency control
3. **Job Level**: `job_concurrency` limits concurrent operations within a single job run
4. **Matching**: Operations with matching `concurrency_key` values respect the corresponding `tag_concurrency_limits`

#### Best Practices

- **Resource-Based Limits**: Set limits based on database connections, API rate limits, or compute resources
- **Conservative Defaults**: Start with lower limits and increase based on system capacity
- **Monitor Usage**: Use Dagster UI to monitor concurrency utilization and adjust limits as needed
- **Legacy Compatibility**: Old concurrency keys are maintained for backward compatibility during transition

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

- **`trino_insert_select`**: Execute SQL queries in Trino, optionally INSERT...SELECT into target tables
- **`snowflake_insert_select`**: Execute SQL queries in Snowflake, optionally INSERT...SELECT into target tables
- **`trino_extract`**: Extract data from Trino into pandas DataFrame (supports batching)
- **`trino_load`**: Load pandas DataFrame into Trino table
- **`snowflake_extract`**: Extract data from Snowflake into pandas DataFrame (supports batching)
- **`snowflake_load`**: Load pandas DataFrame into Snowflake table
- **`dataframe_to_s3`**: Upload pandas DataFrame to S3 as CSV

    ```yaml
    # Upload DataFrame to S3
    - name: upload_to_s3
      executor: dataframe_to_s3
      inputs: ["df"]
      outputs: ["s3_result"]
      config:
        endpoint: http://minio-cluster.svc.cluster.local:9000
        bucket: warehouse
        key: exports/my_data.csv
        access_key: admin
        # s3_secret_key provided via environment variable
    ```
- **`s3_extract`**: Download CSV from S3 and return as DataFrame
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

- **`trino_insert_select`**: Execute SQL queries in Trino, optionally INSERT...SELECT into target tables
- **`snowflake_insert_select`**: Execute SQL queries in Snowflake, optionally INSERT...SELECT into target tables

    ```yaml
    # With target table: INSERT INTO or CREATE TABLE AS SELECT
    - name: load
      executor: trino_insert_select
      config:
        select_query: SELECT * FROM source_table
        target_catalog: lakehouse
        target_schema: test
        target_table: target_table

    # Without target table: Execute query directly (like DDL operations)
    - name: cleanup
      executor: trino_insert_select
      config:
        select_query: DROP TABLE IF EXISTS lakehouse.test.temp_table

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
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 --context kind-local &
```

```bash
# 1. Build the new image
docker build -t pipelines-dagster:latest .

# 2. Load the image into kind cluster (replace <cluster-name> with your cluster, e.g., 'local')
kind load docker-image pipelines-dagster:latest --name <cluster-name>

# 3. Delete user code deployment pods to pick up new image
kubectl delete pods -n dagster -l app.kubernetes.io/name=dagster-user-deployments --context kind-local

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
kubectl delete pods -n dagster -l app.kubernetes.io/name=dagster-user-deployments --context kind-local && \
sleep 25 && \
curl -s -X POST http://localhost:3000/graphql -H "Content-Type: application/json" -d '{"query":"mutation { reloadWorkspace { __typename } }"}'
```

**Note:** 
- Replace `<cluster-name>` with your kind cluster name (typically `local` for this project)

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
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 --context kind-local &

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

### External Dependencies Setup

This project requires several external services to be running:

#### Kubernetes Cluster
- **Setup**: See [`k8s.md`](k8s.md) for instructions on creating the local Kind cluster
- **Repository**: [danielnuriyev/pulumi-trino](https://github.com/danielnuriyev/pulumi-trino)

#### Trino
- **Setup**: See [pulumi-trino repository](https://github.com/danielnuriyev/pulumi-trino) for Trino deployment
- **Repository**: [danielnuriyev/pulumi-trino](https://github.com/danielnuriyev/pulumi-trino)

#### Snowflake Emulator
- **Setup**: See [pulumi-snowflake-emulator repository](https://github.com/danielnuriyev/pulumi-snowflake-emulator) for Snowflake emulator deployment
- **Repository**: [danielnuriyev/pulumi-snowflake-emulator](https://github.com/danielnuriyev/pulumi-snowflake-emulator)

#### Dagster
- **Setup**: See [pulumi-dagster repository](https://github.com/danielnuriyev/pulumi-dagster) for Dagster deployment
- **Repository**: [danielnuriyev/pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster)

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
uv run python scripts/populate_snowflake.py

# With custom connection settings
uv run python scripts/populate_snowflake.py --host localhost --port 8088 --user snowflake --password snowflake
```

This creates the `SNOWFLAKE.PUBLIC.test_a` table with 100 records containing IDs and timestamps.

## Testing

### Run Integration Tests

The project includes multiple ways to run integration tests depending on your setup:

#### Quick Start (Local Development)

For local development with a running Dagster instance:

```bash
# Run all test pipelines (requires local Dagster server)
uv run pytest tests/integration/test_all_pipelines_local.py -v -s -m integration

# Run specific pipeline test
uv run pytest tests/integration/test_full_pipeline.py -v -s -m integration
```

#### Full Integration Testing

For complete end-to-end testing with Kubernetes deployment:

```bash
# Run all pipelines on Kubernetes Dagster
uv run pytest tests/integration/test_all_pipelines_k8s.py -v -s -m integration

# Run full pipeline chain test
uv run pytest tests/integration/test_full_pipeline.py -v -s -m integration
```

### Available Pipeline Examples

The `pipelines/` directory contains example pipelines demonstrating different patterns:

#### Trino Pipelines (`pipelines/trino/`)

| Pipeline | Description | Demonstrates |
|----------|-------------|--------------|
| `test_trino_trino` | Extract → Load (Trino → Trino) | Basic ETL, single-step pipeline |
| `test_trino_s3` | Extract → S3 Upload | Data export to S3 |
| `test_trino_multiple` | Multi-step Trino operations | Complex SQL workflows |
| `test_trino_insert_select` | SQL execution with targets | INSERT SELECT operations |
| `test_trino_batching_trino` | Large dataset batching | Memory-efficient processing |
| `test_trino_duck_trino` | DuckDB + Trino integration | Multi-engine processing |
| `test_trino_duck_targets` | Parallel targets + S3 export | Complex workflows |
| `test_trino_duck_fanin_trino` | Batch processing + fan-in | Parallel batch processing |
| `test_trino_targets` | Multiple parallel targets | Multi-asset pipelines |

#### S3 Pipelines (`pipelines/s3/`)

| Pipeline | Description | Demonstrates |
|----------|-------------|--------------|
| `test_s3_trino` | S3 CSV → Trino table | Data import from S3 |

#### Snowflake Pipelines (`pipelines/snowflake/`)

| Pipeline | Description | Demonstrates |
|----------|-------------|--------------|
| `test_snowflake_snowflake` | Extract → Load (Snowflake → Snowflake) | Basic ETL with Snowflake |
| `test_snowflake_s3` | Extract → S3 Upload | Snowflake data export |

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
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 --context kind-local &
kubectl port-forward svc/trino-0a966bea-trino -n trino 8080:8080 --context kind-local &
kubectl port-forward svc/minio-ec2bcee8 -n trino 30900:9000 --context kind-local &
kubectl port-forward svc/snowflake-emulator -n trino 8088:8088 --context kind-local &

# Get MinIO credentials
export S3_SECRET_KEY=$(kubectl get secret minio-ec2bcee8 -n trino --context kind-local -o jsonpath='{.data.rootPassword}' | base64 -d)

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


## Related Repositories

- [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster) - Deploys Dagster infrastructure to Kubernetes

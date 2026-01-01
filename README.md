# pipelines-dagster

Dagster pipelines deployed to Kubernetes.

This repository contains user code deployments for [Dagster](https://dagster.io/), which is deployed to Kubernetes using [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster).

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                    │
│                                                          │
│  ┌──────────────────┐  ┌──────────────────────────────┐ │
│  │  Dagster Core    │  │  User Code (this repo)       │ │
│  │  (pulumi-dagster)│  │  ┌────────────────────────┐  │ │
│  │  ├─ Webserver    │◄─┼──│  pipelines-dagster     │  │ │
│  │  ├─ Daemon       │  │  │  ├─ test_a_trino_...   │  │ │
│  │  └─ PostgreSQL   │  │  │  └─ test_b_trino_...   │  │ │
│  └──────────────────┘  │  └────────────────────────┘  │ │
│                        └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Setup

```bash
# Install dependencies
uv sync

# Install dev dependencies (linting, testing)
uv sync --extra dev

# Install pre-commit hooks
uv run pre-commit install
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

```bash
# Build and load image
docker build -t pipelines-dagster:latest .
kind load docker-image pipelines-dagster:latest --name <cluster-name>

# Deploy with Pulumi
cd ../pulumi-dagster
pulumi up
```

## Testing

### Integration Tests

Integration tests verify the full pipeline chain by deploying to Dagster and Trino.

**Prerequisites:**
- Dagster running and accessible at `http://localhost:3000`
- Trino running and accessible at `localhost:8080`

```bash
# Port-forward Dagster (if using Kubernetes)
kubectl port-forward svc/dagster-dagster-webserver -n dagster 3000:80 &

# Port-forward Trino (if using Kubernetes)
kubectl port-forward svc/trino-6f3317f2-trino -n trino 8080:8080 &

# Run integration tests
uv run pytest tests/integration/ -v -s -m integration
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DAGSTER_URL` | `http://localhost:3000` | Dagster webserver URL |
| `TRINO_HOST` | `localhost` | Trino host |
| `TRINO_PORT` | `8080` | Trino port |
| `TRINO_CATALOG` | `lakehouse` | Trino catalog |
| `TRINO_SCHEMA` | `test` | Trino schema |

## Project Structure

```
pipelines-dagster/
├── src/
│   └── pipelines_dagster/
│       ├── __init__.py
│       └── definitions.py      # Dagster jobs, ops, sensors
├── pipelines/
│   ├── test_a_trino_insert_select.yaml
│   └── test_b_trino_insert_select.yaml
├── tests/
│   └── integration/
│       └── test_pipeline_chain.py
├── Dockerfile
├── pyproject.toml
├── .pre-commit-config.yaml
├── .yamllint.yaml
└── README.md
```

## Related Repositories

- [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster) - Deploys Dagster infrastructure to Kubernetes

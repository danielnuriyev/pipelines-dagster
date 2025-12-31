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
│  │  ├─ Daemon       │  │  │  └─ noop_job           │  │ │
│  │  └─ PostgreSQL   │  │  └────────────────────────┘  │ │
│  └──────────────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Local Development

```bash
# Install dependencies
uv sync

# Run Dagster dev server locally
uv run dagster dev -m pipelines_dagster.definitions
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

## Related Repositories

- [pulumi-dagster](https://github.com/danielnuriyev/pulumi-dagster) - Deploys Dagster infrastructure to Kubernetes

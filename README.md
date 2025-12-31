# pipelines-dagster

A minimal Dagster pipeline for deployment to Kubernetes.

## Local Development

```bash
# Install dependencies
uv sync

# Run Dagster dev server locally
uv run dagster dev -m pipelines_dagster.definitions
```

## Build Docker Image

```bash
# Build the image (for local Kubernetes like kind/minikube)
docker build -t pipelines-dagster:latest .

# For kind: load the image into the cluster
kind load docker-image pipelines-dagster:latest
```

## Deploy to Kubernetes

The image is deployed via the Pulumi configuration in `../pulumi-dagster`.

```bash
cd ../pulumi-dagster
pulumi up
```


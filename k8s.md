# Creating a Local Kubernetes Cluster with Kind

This guide explains how to create a local Kubernetes cluster using Kind (Kubernetes in Docker) with the cluster name "local".

## Prerequisites

Before creating the cluster, ensure you have the following tools installed:

1. **Docker**: Kind runs Kubernetes clusters in Docker containers
   - Download from: https://docs.docker.com/get-docker/

2. **Kind**: The tool for running Kubernetes clusters locally
   ```bash
   # macOS with Homebrew
   brew install kind

   # Linux
   curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
   chmod +x ./kind
   sudo mv ./kind /usr/local/bin/kind

   # Windows
   curl.exe -Lo kind-windows-amd64.exe https://kind.sigs.k8s.io/dl/v0.20.0/kind-windows-amd64.exe
   ```

3. **kubectl**: Kubernetes command-line tool
   ```bash
   # macOS with Homebrew
   brew install kubectl

   # Other platforms: https://kubernetes.io/docs/tasks/tools/
   ```

## Create Kind Configuration File

Create a configuration file named `kind-config.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: local
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  # Minio
  - containerPort: 9000
    hostPort: 9000
    protocol: TCP
  - containerPort: 9001
    hostPort: 9001
    protocol: TCP
  # Nessie
  - containerPort: 19120
    hostPort: 19120
    protocol: TCP
  # Trino
  - containerPort: 8080
    hostPort: 8080
    protocol: TCP
  # Snowflake Emulator
  - containerPort: 8081
    hostPort: 8081
    protocol: TCP
  # Dagster
  - containerPort: 3000
    hostPort: 3000
    protocol: TCP
  - containerPort: 4000
    hostPort: 4000
    protocol: TCP
  - containerPort: 4266
    hostPort: 4266
    protocol: TCP
# Worker nodes
- role: worker
- role: worker
- role: worker
- role: worker
- role: worker
- role: worker
- role: worker
```

## Create the Cluster

Run the following command to create the cluster:

```bash
kind create cluster --name local --config kind-config.yaml
```

This will:
- Create a Kubernetes cluster named "local" with 8 nodes (1 control-plane + 7 workers)
- Configure port mappings for data platform services:
  - Minio: 9000 (API), 9001 (console)
  - Nessie: 19120
  - Trino: 8080
  - Snowflake-emulator: 8081
  - Dagster: 3000 (webserver), 4000 (GraphQL), 4266 (daemon)
- Add an ingress-ready label for easier ingress setup

## Verify the Cluster

After creation, verify the cluster is running:

```bash
# Check cluster status
kubectl cluster-info --context kind-local

# List all nodes
kubectl get nodes

# Check cluster components
kubectl get pods -n kube-system
```

## Deploy Data Platform Services

The cluster is configured with port mappings for the following services:

### Minio (Object Storage)
- **Port**: 9000 (API), 9001 (Console)
- Deploy using Helm or Kubernetes manifests for S3-compatible storage

### Nessie (Git-like Catalog for Data Lakes)
- **Port**: 19120
- Deploy the Nessie server for metadata management

### Trino (Distributed SQL Query Engine)
- **Port**: 8080
- Deploy Trino coordinator and workers for federated queries

### Snowflake Emulator
- **Port**: 8081
- Deploy from [snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator) for local Snowflake-compatible testing

### Dagster (Data Orchestration Platform)
- **Ports**: 3000 (Web UI), 4000 (GraphQL API), 4266 (Daemon)
- Deploy Dagster for data pipeline orchestration

## Using the Cluster

Once created, the cluster will be available as a kubectl context named `kind-local`. You can now:

- Deploy applications and services
- Create services and deployments
- Install ingress controllers
- Set up persistent volumes
- Deploy the data platform stack
- And more!

## Cleanup

To delete the cluster when you're done:

```bash
kind delete cluster --name local
```

## Additional Resources

- [Kind Documentation](https://kind.sigs.k8s.io/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [kubectl Cheat Sheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/)

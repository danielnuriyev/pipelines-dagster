# Snowflake Emulator Setup

This document describes how to deploy and use the Snowflake emulator ([nnnkkk7/snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator)) on a local Kubernetes cluster.

## Overview

The [nnnkkk7/snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator) is a lightweight, open-source Snowflake emulator built with Go and DuckDB. It supports:
- gosnowflake protocol (Python/Go clients)
- REST API v2
- Standard SQL operations with automatic Snowflake → DuckDB translation
- No authentication required (dev mode)

## Deployment

The Snowflake emulator is deployed to the `trino` namespace using Kubernetes Deployment and Service.

### 1. Build the Docker Image

Clone and build the repository:

```bash
cd /Users/danielnuriyev/projects
git clone https://github.com/nnnkkk7/snowflake-emulator.git
cd snowflake-emulator
docker build -t snowflake-emulator:latest .
```

### 2. Load Image into Kind Cluster

```bash
kind load docker-image snowflake-emulator:latest --name trino
```

### 3. Deploy to Kubernetes

```bash
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
```

### 4. Verify Deployment

Wait for the pod to be ready:

```bash
kubectl wait --for=condition=ready pod -l app=snowflake-emulator -n trino --timeout=60s
```

Check the service:

```bash
kubectl get svc snowflake-emulator -n trino
```

Health check:

```bash
kubectl exec -n trino deployment/snowflake-emulator -- curl http://localhost:8080/health
```

### 5. Port Forwarding

To access the emulator from your local machine:

```bash
kubectl port-forward svc/snowflake-emulator -n trino 8088:8081 &
```

This maps your local port `8088` to the service port `8081` (which maps to the emulator's internal port `8080`).

## Connection Details

Once deployed and port-forwarded, use these settings (configured in `pipelines/config.yaml`):

| Parameter | Value |
|-----------|-------|
| **Host** | `localhost` (local) / `snowflake-emulator.trino.svc.cluster.local` (K8s) |
| **Port** | `8088` (local) / `8081` (K8s service) |
| **User** | `snowflake` |
| **Password** | `snowflake` |
| **Account** | `snowflake` |
| **Database** | `SNOWFLAKE` |
| **Schema** | `PUBLIC` |
| **Warehouse** | `COMPUTE_WH` |

## Populating Test Data

Use the provided script to populate the emulator with test data:

```bash
uv run python scripts/populate_snowflake.py
```

This script creates the `SNOWFLAKE.PUBLIC.test_a` table and inserts 100 sample records.

## API Examples

### Using gosnowflake (Python/Go Driver)

```bash
# Create a session
curl -X POST http://localhost:8088/session/v1/login-request \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "CLIENT_APP_ID": "snowflake-emulator",
      "CLIENT_VERSION": "3.0.0"
    }
  }'
```

### Using REST API v2

```bash
# Submit SQL statement
curl -X POST http://localhost:8088/api/v2/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "SELECT 1",
    "database": "SNOWFLAKE",
    "schema": "PUBLIC"
  }'

# List databases
curl http://localhost:8088/api/v2/databases

# Health check
curl http://localhost:8088/health
```

## Features Supported

### SQL Operations
- Query: SELECT, SHOW, DESCRIBE, EXPLAIN
- DML: INSERT, UPDATE, DELETE
- DDL: CREATE/DROP TABLE, DATABASE, SCHEMA
- Transaction: BEGIN, COMMIT, ROLLBACK
- Data Loading: COPY INTO
- Upsert: MERGE INTO

### SQL Functions (with Snowflake → DuckDB translation)
- IFF → IF
- NVL/NVL2/IFNULL → COALESCE
- DATEADD/DATEDIFF → date arithmetic
- TO_VARIANT/PARSE_JSON → JSON casting
- OBJECT_CONSTRUCT → json_object
- LISTAGG → STRING_AGG
- FLATTEN → UNNEST

### Data Types
- NUMBER, INTEGER, BIGINT, FLOAT, DOUBLE
- VARCHAR, STRING, TEXT, CHAR
- BOOLEAN, DATE, TIME, TIMESTAMP
- VARIANT, OBJECT, ARRAY
- BINARY, VARBINARY
- GEOGRAPHY, GEOMETRY

## Troubleshooting

### Pod not starting

Check logs:
```bash
kubectl logs -n trino -l app=snowflake-emulator
```

### Health check failing

Test connectivity:
```bash
curl http://localhost:8088/health
```

### Can't connect from populate_snowflake.py

Ensure port-forward is running:
```bash
kubectl port-forward svc/snowflake-emulator -n trino 8088:8081
```

## References

- [nnnkkk7/snowflake-emulator GitHub Repository](https://github.com/nnnkkk7/snowflake-emulator)
- [Supported SQL Operations & Functions](https://github.com/nnnkkk7/snowflake-emulator#compatibility)

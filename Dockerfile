FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY pipelines_dagster/ pipelines_dagster/

# Install the package with uv
RUN uv pip install --system --no-cache .

# Set the module for dagster to load
ENV DAGSTER_HOME=/app

# Expose the grpc port for user code
EXPOSE 4000

# Run dagster code server
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "pipelines_dagster.definitions"]

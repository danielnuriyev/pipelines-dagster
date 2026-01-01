"""Dagster definitions for the S3 workspace."""

from pipelines_dagster.definitions import generate_definitions_for_workspace

defs = generate_definitions_for_workspace("s3")


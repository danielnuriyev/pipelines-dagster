"""Dagster definitions for the Snowflake workspace."""

from pipelines_dagster.definitions import generate_definitions_for_workspace

defs = generate_definitions_for_workspace("snowflake")
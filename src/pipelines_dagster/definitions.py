"""Shared utilities for dynamically generating Dagster definitions from YAML."""

import os
from pathlib import Path
from typing import Any, Callable

import yaml
from dagster import (
    AssetKey,
    AutoMaterializePolicy,
    DefaultScheduleStatus,
    Definitions,
    In,
    OpExecutionContext,
    Out,
    ScheduleDefinition,
    asset,
    define_asset_job,
    graph_asset,
    op,
)

from pipelines_dagster.ops.s3_to_trino import s3_to_trino_op
from pipelines_dagster.ops.trino_insert_select import trino_insert_select_op
from pipelines_dagster.ops.trino_pandas_etl import trino_extract_op, trino_load_op
from pipelines_dagster.ops.trino_to_s3 import trino_to_s3_op

# Base directory containing pipeline YAML configurations
PIPELINES_BASE_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", "/app/pipelines"))


# Map pipeline names to their executor functions
PIPELINE_EXECUTORS: dict[str, Callable[[OpExecutionContext, dict], Any]] = {
    "trino_insert_select": lambda ctx, cfg: trino_insert_select_op(ctx, cfg),
    "trino_to_s3": lambda ctx, cfg: trino_to_s3_op(ctx, cfg),
    "s3_to_trino": lambda ctx, cfg: s3_to_trino_op(ctx, cfg),
}


def load_pipeline_configs_from_dir(directory: Path) -> dict[str, dict]:
    """Load all pipeline configurations from YAML files in a directory."""
    configs = {}
    if directory.exists():
        for yaml_file in directory.glob("*.yaml"):
            name = yaml_file.stem
            with open(yaml_file) as f:
                configs[name] = yaml.safe_load(f)
    return configs


def make_pandas_etl_graph_asset(job_name: str, asset_key: AssetKey, config: dict):
    """Create a graph_asset for the pandas ETL pipeline with visible extract and load ops."""

    @op(
        name=f"{job_name}_extract",
        out=Out(description="DataFrame extracted from Trino"),
    )
    def extract_op(context: OpExecutionContext):
        return trino_extract_op(context, config)

    @op(
        name=f"{job_name}_load",
        ins={"df": In(description="DataFrame to load")},
        out=Out(is_required=False),
    )
    def load_op(context: OpExecutionContext, df):
        trino_load_op(context, config, df)
        return None

    @graph_asset(
        key=asset_key,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    )
    def pandas_etl_graph():
        df = extract_op()
        return load_op(df)

    return pandas_etl_graph


def make_asset_for_pipeline(job_name: str, config: dict):
    """Create an asset for a pipeline configuration."""
    pipeline_name = config.get("name")

    # Get asset key from config
    asset_key_list = config.get("asset_key")
    if not asset_key_list:
        raise ValueError(f"Missing 'asset_key' in config for job: {job_name}")

    asset_key = AssetKey(asset_key_list)

    # Get dependencies
    depends_on = config.get("depends_on", [])
    dep_keys = {AssetKey(dep) for dep in depends_on}

    # Special handling for multi-op trino_pandas_etl pipeline
    if pipeline_name == "trino_pandas_etl":
        return make_pandas_etl_graph_asset(job_name, asset_key, config)

    # For other pipelines, use single-op assets
    executor = PIPELINE_EXECUTORS.get(pipeline_name)
    if not executor:
        raise ValueError(f"Unknown pipeline name: {pipeline_name} for job: {job_name}")

    @asset(
        key=asset_key,
        non_argument_deps=dep_keys if dep_keys else None,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    )
    def pipeline_asset(context: OpExecutionContext):
        executor(context, config)

    return pipeline_asset


def generate_definitions_for_workspace(workspace_name: str) -> Definitions:
    """Generate Dagster definitions for a specific workspace (subdirectory)."""
    workspace_dir = PIPELINES_BASE_DIR / workspace_name
    configs = load_pipeline_configs_from_dir(workspace_dir)

    assets = []
    jobs = []
    schedules = []

    # Create all assets
    for job_name, config in configs.items():
        pipeline_name = config.get("name")
        if not pipeline_name:
            continue  # Skip configs without a name

        asset_key = config.get("asset_key")
        if not asset_key:
            continue  # Skip configs without an asset_key

        pipeline_asset = make_asset_for_pipeline(job_name, config)
        assets.append(pipeline_asset)

    # Create jobs for assets that have schedules
    for job_name, config in configs.items():
        schedule_cron = config.get("schedule")
        if not schedule_cron:
            continue

        asset_key = config.get("asset_key")
        if not asset_key:
            continue

        # Create a job that materializes this specific asset
        asset_job = define_asset_job(
            name=f"{job_name}_job",
            selection=[AssetKey(asset_key)],
        )
        jobs.append(asset_job)

        # Create a schedule for this job
        schedule = ScheduleDefinition(
            name=f"{job_name}_schedule",
            job=asset_job,
            cron_schedule=schedule_cron,
            default_status=DefaultScheduleStatus.RUNNING,
        )
        schedules.append(schedule)

    return Definitions(
        assets=assets,
        jobs=jobs,
        schedules=schedules,
    )

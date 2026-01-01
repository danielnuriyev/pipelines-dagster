"""Shared utilities for dynamically generating Dagster definitions from YAML."""

import os
from pathlib import Path

import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutoMaterializePolicy,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from pipelines_dagster.ops import (
    execute_s3_to_trino,
    execute_trino_insert_select,
    execute_trino_to_s3,
)

# Base directory containing pipeline YAML configurations
PIPELINES_BASE_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", "/app/pipelines"))

# Map pipeline names to their executor functions
PIPELINE_EXECUTORS = {
    "trino_insert_select": execute_trino_insert_select,
    "trino_to_s3": execute_trino_to_s3,
    "s3_to_trino": execute_s3_to_trino,
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


def make_asset_for_pipeline(job_name: str, config: dict):
    """Create an asset for a pipeline configuration."""
    pipeline_name = config.get("name")
    executor = PIPELINE_EXECUTORS.get(pipeline_name)

    if not executor:
        raise ValueError(f"Unknown pipeline name: {pipeline_name} for job: {job_name}")

    # Get asset key from config
    asset_key_list = config.get("asset_key")
    if not asset_key_list:
        raise ValueError(f"Missing 'asset_key' in config for job: {job_name}")

    asset_key = AssetKey(asset_key_list)

    # Get dependencies
    depends_on = config.get("depends_on", [])
    dep_keys = [AssetKey(dep) for dep in depends_on]

    # Create the asset with auto-materialization for downstream triggering
    # Note: when using key, we can't specify name (the last part of key becomes the name)
    @asset(
        key=asset_key,
        deps=dep_keys,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    )
    def pipeline_asset(context: AssetExecutionContext):
        """Dynamically generated asset."""
        context.log.info(f"Materializing asset: {asset_key} (pipeline: {pipeline_name})")
        executor(context, config)
        context.log.info(f"Asset {asset_key} materialized successfully")

    # Update docstring
    pipeline_asset.__doc__ = f"Asset '{job_name}' (pipeline: {pipeline_name})"

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

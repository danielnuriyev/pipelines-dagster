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
    GraphOut,
    In,
    OpExecutionContext,
    Out,
    ScheduleDefinition,
    asset,
    define_asset_job,
    graph,
    op,
)

from pipelines_dagster.ops.s3_to_trino import s3_to_trino_op
from pipelines_dagster.ops.trino_insert_select import trino_insert_select_op
from pipelines_dagster.ops.trino_pandas_etl import trino_extract_op, trino_load_op
from pipelines_dagster.ops.trino_to_s3 import trino_to_s3_op

# Base directory containing pipeline YAML configurations
PIPELINES_BASE_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", "/app/pipelines"))


# Map executor names to their implementation functions
EXECUTOR_FUNCTIONS: dict[str, Callable[[OpExecutionContext, dict], Any]] = {
    "trino_insert_select": trino_insert_select_op,
    "trino_to_s3": trino_to_s3_op,
    "s3_to_trino": s3_to_trino_op,
    "trino_extract": trino_extract_op,
    "trino_load": trino_load_op,
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


def create_op_for_step(step_name: str, executor_func: Callable, step_config: dict, job_name: str):
    """Create an op for a single step based on YAML configuration."""
    has_inputs = step_config.get("inputs") is not None and len(step_config.get("inputs", [])) > 0
    has_outputs = step_config.get("outputs") is not None and len(step_config.get("outputs", [])) > 0
    step_cfg = step_config.get("config", {})

    if has_inputs and has_outputs:
        @op(name=f"{job_name}_{step_name}", ins={"data": In()}, out=Out())
        def step_op(context: OpExecutionContext, data):
            return executor_func(context, step_cfg, data)

    elif has_inputs and not has_outputs:
        @op(name=f"{job_name}_{step_name}", ins={"data": In()})
        def step_op(context: OpExecutionContext, data):
            executor_func(context, step_cfg, data)

    elif not has_inputs and has_outputs:
        @op(name=f"{job_name}_{step_name}", out=Out())
        def step_op(context: OpExecutionContext):
            return executor_func(context, step_cfg)

    else:
        @op(name=f"{job_name}_{step_name}")
        def step_op(context: OpExecutionContext):
            executor_func(context, step_cfg)

    return step_op


def make_graph_asset_from_steps(
    job_name: str, asset_key: AssetKey, dep_keys: set, steps: list
):
    """Create an asset backed by a graph that shows individual ops."""

    # Create ops for each step
    step_ops_list = []
    for i, step in enumerate(steps):
        executor_name = step.get("executor")
        step_name = step.get("name", f"step_{i}")

        executor_func = EXECUTOR_FUNCTIONS.get(executor_name)
        if not executor_func:
            raise ValueError(f"Unknown executor: {executor_name}")

        step_op = create_op_for_step(step_name, executor_func, step, job_name)
        has_inputs = step.get("inputs") is not None and len(step.get("inputs", [])) > 0
        has_outputs = step.get("outputs") is not None and len(step.get("outputs", [])) > 0
        step_ops_list.append((step_name, step_op, has_inputs, has_outputs))

    # Create the appropriate graph based on the step pattern
    if len(step_ops_list) == 2:
        step1_name, step1_op, step1_in, step1_out = step_ops_list[0]
        step2_name, step2_op, step2_in, step2_out = step_ops_list[1]

        if not step1_in and step1_out and step2_in and not step2_out:
            # Pattern: first op has output, second op takes input (like extract+load)
            # Create a graph
            @graph(name=f"{job_name}_graph")
            def execution_graph():
                data = step1_op()
                step2_op(data=data)

            # Wrap the graph in an asset
            @asset(
                key=asset_key,
                non_argument_deps=dep_keys if dep_keys else None,
                auto_materialize_policy=AutoMaterializePolicy.eager(),
            )
            def graph_backed_asset(context: OpExecutionContext):
                return execution_graph()

            return graph_backed_asset

    # Single op or other multi-op patterns - not yet supported
    raise ValueError(f"Unsupported pipeline pattern for {job_name}. Currently only supports 2-op patterns with extract+load.")


def make_single_op_asset(
    job_name: str, asset_key: AssetKey, dep_keys: set, step: dict
):
    """Create an asset for a single-step pipeline."""
    executor_name = step.get("executor")
    step_name = step.get("name", "execute")
    step_config = step.get("config", {})

    executor_func = EXECUTOR_FUNCTIONS.get(executor_name)
    if not executor_func:
        raise ValueError(f"Unknown executor: {executor_name}")

    @asset(
        key=asset_key,
        non_argument_deps=dep_keys if dep_keys else None,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    )
    def pipeline_asset(context: OpExecutionContext):
        executor_func(context, step_config)

    return pipeline_asset


def make_asset_for_pipeline(job_name: str, config: dict):
    """Create an asset for a pipeline configuration based on its steps."""
    # Get asset key from config
    asset_key_list = config.get("asset_key")
    if not asset_key_list:
        raise ValueError(f"Missing 'asset_key' in config for job: {job_name}")

    asset_key = AssetKey(asset_key_list)

    # Get dependencies
    depends_on = config.get("depends_on", [])
    dep_keys = {AssetKey(dep) for dep in depends_on}

    # Get steps
    steps = config.get("steps", [])
    if not steps:
        raise ValueError(f"No steps defined in config for job: {job_name}")

    # If single step, create a simple asset
    if len(steps) == 1:
        return make_single_op_asset(job_name, asset_key, dep_keys, steps[0])

    # If multiple steps, create a graph-backed asset
    return make_graph_asset_from_steps(job_name, asset_key, dep_keys, steps)


def generate_definitions_for_workspace(workspace_name: str) -> Definitions:
    """Generate Dagster definitions for a specific workspace (subdirectory)."""
    workspace_dir = PIPELINES_BASE_DIR / workspace_name
    configs = load_pipeline_configs_from_dir(workspace_dir)

    assets = []
    jobs = []
    schedules = []

    # Create all assets
    for job_name, config in configs.items():
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

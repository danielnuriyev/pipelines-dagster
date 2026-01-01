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
    DynamicIn,
    DynamicOut,
    DynamicOutput,
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
from pipelines_dagster.ops.trino_pandas_etl import (
    trino_extract_batch_generator,
    trino_extract_op,
    trino_load_op,
)
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


def create_op_for_step(
    step_name: str,
    executor_func: Callable,
    step_config: dict,
    job_name: str,
    dynamic_output: bool = False,
    dynamic_input: bool = False,
):
    """Create an op for a single step based on YAML configuration."""
    has_inputs = step_config.get("inputs") is not None and len(step_config.get("inputs", [])) > 0
    step_cfg = step_config.get("config", {})

    if dynamic_output:
        @op(name=f"{job_name}_{step_name}", out=DynamicOut())
        def step_op(context: OpExecutionContext):
            for mapping_key, payload in executor_func(context, step_cfg):
                yield DynamicOutput(payload, mapping_key=str(mapping_key))

        return step_op

    if has_inputs:
        ins = {"data": DynamicIn() if dynamic_input else In()}

        @op(name=f"{job_name}_{step_name}", ins=ins)
        def step_op(context: OpExecutionContext, data):
            executor_func(context, step_cfg, data)

        return step_op

    @op(name=f"{job_name}_{step_name}")
    def step_op(context: OpExecutionContext):
        executor_func(context, step_cfg)

    return step_op


def make_graph_asset_from_steps(
    job_name: str, asset_key: AssetKey, dep_keys: set, steps: list
):
    """Create an asset backed by a graph that shows individual ops."""

    if len(steps) != 2:
        raise ValueError(
            f"Unsupported pipeline pattern for {job_name}. Currently only supports 2-op patterns with extract+load."
        )

    extract_step = steps[0]
    load_step = steps[1]

    extract_executor = EXECUTOR_FUNCTIONS.get(extract_step.get("executor"))
    load_executor = EXECUTOR_FUNCTIONS.get(load_step.get("executor"))

    if extract_executor is None or load_executor is None:
        raise ValueError(f"Unknown executor in steps for job: {job_name}")

    is_batched = extract_step.get("config", {}).get("batch_size") is not None

    extract_op = create_op_for_step(
        step_name=extract_step.get("name", "extract"),
        executor_func=trino_extract_batch_generator if is_batched else extract_executor,
        step_config=extract_step,
        job_name=job_name,
        dynamic_output=is_batched,
    )

    load_op = create_op_for_step(
        step_name=load_step.get("name", "load"),
        executor_func=load_executor,
        step_config=load_step,
        job_name=job_name,
        dynamic_input=is_batched,
    )

    @graph(name=f"{job_name}_graph")
    def execution_graph():
        data = extract_op()
        step_result = load_op(data=data)
        return step_result

    @asset(
        key=asset_key,
        non_argument_deps=dep_keys if dep_keys else None,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    )
    def graph_backed_asset(context: OpExecutionContext):
        return execution_graph()

    return graph_backed_asset


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

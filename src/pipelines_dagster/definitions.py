"""Shared utilities for dynamically generating Dagster definitions from YAML."""

import os
from pathlib import Path
from typing import Any, Callable

import yaml
from dagster import (
    AssetKey,
    AutomationCondition,
    DefaultScheduleStatus,
    Definitions,
    DynamicOut,
    DynamicOutput,
    In,
    OpExecutionContext,
    Out,
    ScheduleDefinition,
    asset,
    define_asset_job,
    graph_asset,
    op,
)

from pipelines_dagster.ops.batch_splitter import batch_splitter_op
from pipelines_dagster.ops.s3_to_trino import s3_to_trino_op
from pipelines_dagster.ops.trino_insert_select import trino_insert_select_op
from pipelines_dagster.ops.trino_pandas_etl import (
    trino_extract_op,
    trino_load_op,
)
from pipelines_dagster.ops.trino_to_s3 import trino_to_s3_op

# Base directory containing pipeline YAML configurations
# Use relative path when running locally, absolute path in Docker
default_path = "/app/pipelines" if os.path.exists("/app/pipelines") else "pipelines"
PIPELINES_BASE_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", default_path))


# Map executor names to their implementation functions
EXECUTOR_FUNCTIONS: dict[str, Callable[[OpExecutionContext, dict], Any]] = {
    "trino_insert_select": trino_insert_select_op,
    "trino_to_s3": trino_to_s3_op,
    "s3_to_trino": s3_to_trino_op,
    "trino_extract": trino_extract_op,
    "trino_load": trino_load_op,
    "batch_splitter": batch_splitter_op,
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


def create_op_for_step(step_name: str, executor_func: Callable, step_config: dict, job_name: str, is_batching: bool = False):
    """Create an op for a single step based on YAML configuration.

    Args:
        step_name: Name of the step
        executor_func: The executor function to call
        step_config: Step configuration from YAML
        job_name: Name of the parent job
        is_batching: If True, this op produces DynamicOut (yields multiple batches)
    """
    has_inputs = step_config.get("inputs") is not None and len(step_config.get("inputs", [])) > 0
    has_outputs = step_config.get("outputs") is not None and len(step_config.get("outputs", [])) > 0
    step_cfg = step_config.get("config", {})

    if is_batching:
        # Batching op: produces DynamicOut
        if has_inputs:
            @op(name=f"{job_name}_{step_name}", ins={"data": In()}, out=DynamicOut())
            def step_op(context: OpExecutionContext, data):
                result = executor_func(context, step_cfg, data)
                for batch_key, batch_data in result:
                    yield DynamicOutput(batch_data, mapping_key=str(batch_key))
        else:
            @op(name=f"{job_name}_{step_name}", out=DynamicOut())
            def step_op(context: OpExecutionContext):
                result = executor_func(context, step_cfg)
                for batch_key, batch_data in result:
                    yield DynamicOutput(batch_data, mapping_key=str(batch_key))
    elif has_inputs and has_outputs:
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
    """Create an asset that executes pipeline steps.

    Uses @graph_asset with .map() chaining for pipelines with at most 1 batching step,
    showing individual ops in the UI. Uses recursive execution for nested batching.
    """

    if not steps:
        raise ValueError(f"No steps defined for job: {job_name}")

    # Determine which steps have batching
    def step_has_batching(step):
        return step.get("config", {}).get("batch_size") is not None

    batching_step_indices = [i for i, step in enumerate(steps) if step_has_batching(step)]
    batching_step_count = len(batching_step_indices)

    # Create ops for each step
    step_ops_list = []
    for i, step in enumerate(steps):
        executor_name = step.get("executor")
        step_name = step.get("name", f"step_{i}")

        executor_func = EXECUTOR_FUNCTIONS.get(executor_name)
        if not executor_func:
            raise ValueError(f"Unknown executor: {executor_name}")

        is_batching = step_has_batching(step)
        step_op = create_op_for_step(step_name, executor_func, step, job_name, is_batching)
        has_inputs = step.get("inputs") is not None and len(step.get("inputs", [])) > 0
        has_outputs = step.get("outputs") is not None and len(step.get("outputs", [])) > 0

        step_ops_list.append({
            "name": step_name,
            "op": step_op,
            "has_inputs": has_inputs,
            "has_outputs": has_outputs,
            "config": step,
            "executor": executor_func,
            "is_batching": is_batching,
        })

    # Validate pipeline structure
    if step_ops_list:
        first_step = step_ops_list[0]
        if first_step["has_inputs"]:
            raise ValueError(f"First step '{first_step['name']}' should not have inputs")

    # Use @graph_asset with .map() chaining for 0 or 1 batching steps
    if batching_step_count <= 1:
        # Create a final collect op for dynamic results
        @op(name=f"{job_name}_collect_final", ins={"data": In()})
        def collect_final_op(data):
            return data  # data is the collected list

        @graph_asset(
            name=asset_key.path[-1],
            key_prefix=asset_key.path[:-1] if len(asset_key.path) > 1 else None
        )
        def graph_backed_asset():
            # First step
            data = step_ops_list[0]["op"]()
            is_dynamic = step_ops_list[0]["is_batching"]

            # Chain remaining steps using .map() when dynamic
            for step_info in step_ops_list[1:]:
                step_op = step_info["op"]

                if is_dynamic:
                    # Use .map() to apply op to each dynamic output
                    data = data.map(step_op)
                else:
                    # Regular invocation
                    data = step_op(data) if step_info["has_inputs"] else step_op()
                    is_dynamic = step_info["is_batching"]

            # If final result is dynamic, collect it and pass to final op
            if is_dynamic:
                return collect_final_op(data.collect())
            # Otherwise, return the final data directly
            return data

        return graph_backed_asset

    # Nested batching (2+ batching steps): use recursive execution
    @asset(
        key=asset_key,
        deps=list(dep_keys) if dep_keys else None,
        automation_condition=AutomationCondition.eager(),
    )
    def graph_backed_asset(context: OpExecutionContext):
        """Process through all steps with support for nested batching fan-out."""

        def execute_steps_from(
            start_idx: int,
            input_data: Any,
            batch_context: dict,
        ) -> None:
            """Recursively execute steps with nested batching support."""
            if start_idx >= len(step_ops_list):
                return

            step_info = step_ops_list[start_idx]
            step_name = step_info["name"]
            step_executor = step_info["executor"]
            has_inputs = step_info["has_inputs"]
            has_outputs = step_info["has_outputs"]
            step_cfg = step_info["config"].get("config", {}).copy()

            batch_size = step_cfg.get("batch_size")

            if batch_size:
                context.log.info(f"Step {start_idx} ('{step_name}') batching with batch_size={batch_size}")

                # Get batch generator
                if has_inputs and input_data is not None:
                    batch_generator = step_executor(context, step_cfg, input_data)
                else:
                    batch_generator = step_executor(context, step_cfg)

                # Process each batch
                batch_num = 0
                for batch_key, batch_data in batch_generator:
                    batch_num += 1
                    batch_path = f"{batch_context.get('path', '')}/{step_name}:{batch_num}"
                    context.log.info(f"Processing batch path: {batch_path}")

                    new_batch_context = batch_context.copy()
                    new_batch_context["path"] = batch_path
                    new_batch_context[f"step_{start_idx}_batch"] = batch_num

                    if not has_outputs:
                        batch_cfg = step_cfg.copy()
                        if "recreate_table" in batch_cfg and batch_num > 1:
                            batch_cfg["recreate_table"] = False
                        step_executor(context, batch_cfg, batch_data)
                        batch_data = None

                    execute_steps_from(start_idx + 1, batch_data, new_batch_context)

                context.log.info(f"Step {start_idx} completed {batch_num} batches")

            else:
                exec_cfg = step_cfg.copy()
                
                if "recreate_table" in exec_cfg:
                    is_first_batch = all(
                        batch_context.get(f"step_{i}_batch", 1) == 1
                        for i in range(start_idx)
                    )
                    if not is_first_batch:
                        exec_cfg["recreate_table"] = False
                
                if has_inputs:
                    result = step_executor(context, exec_cfg, input_data)
                else:
                    result = step_executor(context, exec_cfg)

                output_data = result if has_outputs else None
                execute_steps_from(start_idx + 1, output_data, batch_context)

        execute_steps_from(0, None, {"path": "root"})

    return graph_backed_asset


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

    # All pipelines (single or multi-step, batched or non-batched) handled by unified engine
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

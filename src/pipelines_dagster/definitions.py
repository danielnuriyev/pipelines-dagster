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

from pipelines_dagster.ops.batch_utils import generic_batch_generator
from pipelines_dagster.ops.batch_splitter import batch_splitter_op
from pipelines_dagster.ops.batch_utils import generic_batch_generator
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

    if not steps:
        raise ValueError(f"No steps defined for job: {job_name}")

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
        step_ops_list.append((step_name, step_op, has_inputs, has_outputs, step))

    # Validate pipeline structure: linear chain (no inputs on first, no outputs on last)
    if step_ops_list:
        first_step = step_ops_list[0]

        if first_step[2]:  # first step should not have inputs
            raise ValueError(f"First step '{first_step[0]}' should not have inputs")

    # Execute all steps (single or multi-step, with or without batching)
    # using unified recursive execution engine
    from pipelines_dagster.ops.trino_pandas_etl import trino_extract_batch_generator

    @asset(
        key=asset_key,
        non_argument_deps=dep_keys if dep_keys else None,
        auto_materialize_policy=AutoMaterializePolicy.eager(),
    )
    def graph_backed_asset(context: OpExecutionContext):
        """Process through all steps with support for nested batching fan-out."""

        def execute_steps_from(
            start_idx: int,
            input_data: Any,
            batch_context: dict,
        ) -> None:
            """
            Recursively execute steps starting from start_idx.
            Supports nested batching at multiple steps.

            Args:
                start_idx: Index of first step to execute
                input_data: Data from previous step (None if no input)
                batch_context: Context tracking batch state (for recreate_table logic)
            """
            if start_idx >= len(step_ops_list):
                return

            step_name, step_op, has_inputs, has_outputs, step_config = step_ops_list[start_idx]
            step_executor = EXECUTOR_FUNCTIONS.get(step_config.get("executor"))
            step_cfg = step_config.get("config", {}).copy()

            if not step_executor:
                raise ValueError(f"Missing executor for step {start_idx}")

            # Check if this step has batching
            batch_size = step_cfg.get("batch_size")

            if batch_size:
                # This step has batching: generate batches and recursively execute remaining steps
                context.log.info(
                    f"Step {start_idx} ('{step_name}') batching with batch_size={batch_size}"
                )

                # Determine the batch generator
                if start_idx == 0 and step_executor == trino_extract_op:
                    # First step using trino_extract: use specialized batch generator
                    batch_generator = trino_extract_batch_generator(context, step_cfg)
                elif step_executor == batch_splitter_op:
                    # Batch splitter: directly use it as generator
                    batch_generator = batch_splitter_op(context, step_cfg, input_data)
                elif has_inputs and input_data is not None:
                    # Step with inputs: use generic batch generator
                    batch_generator = generic_batch_generator(
                        context, step_cfg, input_data, step_executor
                    )
                else:
                    raise NotImplementedError(
                        f"Batching on step {start_idx} ('{step_name}') not supported for this executor"
                    )

                # Process each batch
                batch_num = 0
                for batch_key, batch_data in batch_generator:
                    batch_num += 1
                    batch_path = f"{batch_context.get('path', '')}/{step_name}:{batch_num}"
                    context.log.info(f"Processing batch path: {batch_path}")

                    # Create new batch context for this batch
                    new_batch_context = batch_context.copy()
                    new_batch_context["path"] = batch_path
                    new_batch_context[f"step_{start_idx}_batch"] = batch_num

                    # If batching step doesn't have outputs, execute it here
                    if not has_outputs:
                        # Prepare config for this batch
                        batch_cfg = step_cfg.copy()
                        # Only recreate table on first batch of this step
                        if "recreate_table" in batch_cfg and batch_num > 1:
                            batch_cfg["recreate_table"] = False

                        step_executor(context, batch_cfg, batch_data)
                        batch_data = None

                    # Recursively execute remaining steps with this batch
                    execute_steps_from(start_idx + 1, batch_data, new_batch_context)

                context.log.info(f"Step {start_idx} completed {batch_num} batches")

            else:
                # Non-batching step: execute once and continue
                # Prepare config for this batch
                exec_cfg = step_cfg.copy()
                
                # For recreate_table, check if we're in the first batch at all levels
                if "recreate_table" in exec_cfg:
                    # Check if any parent step has processed more than 1 batch
                    is_first_batch = all(
                        batch_context.get(f"step_{i}_batch", 1) == 1
                        for i in range(start_idx)
                    )
                    if not is_first_batch:
                        exec_cfg["recreate_table"] = False
                
                # Execute the step
                if has_inputs:
                    result = step_executor(context, exec_cfg, input_data)
                else:
                    result = step_executor(context, exec_cfg)

                # Continue to next step
                output_data = result if has_outputs else None
                execute_steps_from(start_idx + 1, output_data, batch_context)

        # Start execution from step 0
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

"""Shared utilities for dynamically generating Dagster definitions from YAML."""

import os
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Callable, Dict, List

import os
import yaml
import jinja2
from dagster import (
    AssetKey,
    AutomationCondition,
    DefaultScheduleStatus,
    Definitions,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ScheduleDefinition,
    asset,
    define_asset_job,
    graph_asset,
    job,
    multiprocess_executor,
    op,
    graph,
    AssetOut,
    GraphOut,
    AssetsDefinition,
)

from pipelines_dagster.ops.batch_fan_in import batch_fan_in_op
from pipelines_dagster.ops.batch_splitter import batch_splitter_op
from pipelines_dagster.ops.duckdb_sql import duckdb_sql_op
from pipelines_dagster.ops.s3_to_trino import s3_to_trino_op
from pipelines_dagster.ops.trino_insert_select import trino_insert_select_op
from pipelines_dagster.ops.trino_extract import trino_extract_op
from pipelines_dagster.ops.trino_load import trino_load_op
from pipelines_dagster.ops.trino_execute import trino_execute_op
from pipelines_dagster.sensors import create_job_retry_sensor


def resolve_step_dependencies(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Resolve step dependencies and return steps in execution order.

    Uses topological sort to handle dependencies between steps.
    Steps without dependencies or with resolved dependencies execute first.

    Args:
        steps: List of step configurations from YAML

    Returns:
        Steps in dependency-resolved execution order

    Raises:
        ValueError: If circular dependencies are detected
    """
    # Create step name to index mapping
    step_indices = {step["name"]: i for i, step in enumerate(steps)}

    # Build dependency graph
    graph_deps = defaultdict(list)  # step -> list of steps that depend on it
    in_degree = {step["name"]: 0 for step in steps}

    for step in steps:
        step_name = step["name"]
        depends_on = step.get("depends_on", [])

        for dep in depends_on:
            if dep not in step_indices:
                raise ValueError(f"Step '{step_name}' depends on unknown step '{dep}'")
            graph_deps[dep].append(step_name)
            in_degree[step_name] += 1

    # Topological sort using Kahn's algorithm
    queue = deque([name for name, degree in in_degree.items() if degree == 0])
    result = []
    processed = set()

    while queue:
        current_step_name = queue.popleft()
        if current_step_name in processed:
            continue
        processed.add(current_step_name)

        # Find the step config
        step_config = next(step for step in steps if step["name"] == current_step_name)
        result.append(step_config)

        # Update dependencies
        for dependent in graph_deps[current_step_name]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Check for circular dependencies
    if len(result) != len(steps):
        remaining = set(step["name"] for step in steps) - processed
        raise ValueError(f"Circular dependency detected involving steps: {remaining}")

    return result
from pipelines_dagster.ops.trino_to_s3 import trino_to_s3_op
from pipelines_dagster.ops.dataframe_to_s3 import dataframe_to_s3_op

# Base directory containing pipeline YAML configurations
# Use relative path when running locally, absolute path in Docker
default_path = "/app/pipelines" if os.path.exists("/app/pipelines") else "pipelines"
PIPELINES_BASE_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", default_path))


def passthrough_op(context: OpExecutionContext, config: dict, data: Any = None, **kwargs) -> Any:
    """Simple pass-through operation that returns the input data."""
    return data

# Map executor names to their implementation functions
EXECUTOR_FUNCTIONS: dict[str, Callable[[OpExecutionContext, dict], Any]] = {
    "trino_insert_select": trino_insert_select_op,
    "trino_to_s3": trino_to_s3_op,
    "dataframe_to_s3": dataframe_to_s3_op,
    "s3_to_trino": s3_to_trino_op,
    "trino_extract": trino_extract_op,
    "trino_load": trino_load_op,
    "trino_execute": trino_execute_op,
    "batch_splitter": batch_splitter_op,
    "batch_fan_in": batch_fan_in_op,
    "duckdb_sql": duckdb_sql_op,
    "passthrough": passthrough_op,
}


def _load_yaml_with_template(file_path: Path) -> dict:
    """Load a YAML file, processing Jinja2 templates if present."""
    with open(file_path) as f:
        content = f.read()

    # Load central configuration for template context
    central_config = _load_pipeline_config()

    # Create Jinja2 template context
    template_context = dict(os.environ)
    template_context.update({
        "config": central_config,  # Make entire config available
        "trino": central_config.get("trino", {}),
        "minio": central_config.get("minio", {}),
    })

    # Check if content contains Jinja2 syntax (simple heuristic)
    if "{{" in content and "}}" in content:
        # Render template
        template = jinja2.Template(content)
        rendered = template.render(**template_context)

        # Parse as YAML
        return yaml.safe_load(rendered)
    else:
        # Regular YAML file
        return yaml.safe_load(content)


def _load_pipeline_config() -> dict:
    """Load the central pipeline configuration."""
    config_path = PIPELINES_BASE_DIR / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


def _preprocess_temp_tables(config: dict) -> dict:
    """Preprocess pipeline config to handle temp table configurations and dependencies."""
    if "steps" not in config:
        return config

    # Find all temp steps and generate their table names upfront
    temp_mappings = {}
    processed_config = config.copy()
    processed_config["steps"] = []

    for step in config["steps"]:
        processed_step = step.copy()
        step_config = processed_step.get("config", {})

        # Check if this step creates a temp table
        if step_config.get("temp", False) and step_config.get("target_table"):
            original_table = step_config["target_table"]
            temp_table = _generate_temp_table_name_for_config(original_table)
            temp_mappings[original_table] = temp_table
            step_config["actual_target_table"] = temp_table
            processed_step["config"] = step_config

        processed_config["steps"].append(processed_step)

    # Now substitute table names in all SQL queries
    for step in processed_config["steps"]:
        step_config = step.get("config", {})
        if "select_query" in step_config:
            step_config["select_query"] = _substitute_table_names_in_sql(
                step_config["select_query"], temp_mappings
            )

    # Store temp mappings in config for cleanup
    if temp_mappings:
        processed_config["_temp_table_mappings"] = temp_mappings

    return processed_config


def _create_config_substitutions(config: dict) -> dict:
    """Create a mapping of placeholder strings to config values for substitution."""
    substitutions = {}

    if "trino" in config:
        trino = config["trino"]
        substitutions.update({
            "__TRINO_HOST__": trino.get("host", ""),
            "__TRINO_PORT__": str(trino.get("port", "")),
            "__TRINO_USER__": trino.get("user", ""),
            "__TRINO_CATALOG__": trino.get("catalog", ""),
            "__TRINO_SCHEMA__": trino.get("schema", ""),
        })

    if "minio" in config:
        minio = config["minio"]
        substitutions.update({
            "__MINIO_HOST__": minio.get("host", ""),
            "__MINIO_PORT__": str(minio.get("port", "")),
            "__MINIO_BUCKET__": minio.get("bucket", ""),
            "__MINIO_ACCESS_KEY__": minio.get("access_key", ""),
        })

    return substitutions


def _apply_config_substitutions(yaml_content: str, substitutions: dict) -> str:
    """Apply configuration substitutions to YAML content."""
    result = yaml_content
    for placeholder, value in substitutions.items():
        result = result.replace(placeholder, value)
    return result


def _preprocess_with_config(config: dict) -> dict:
    """Apply configuration substitutions and temp table processing to pipeline config."""
    # Load central config
    central_config = _load_pipeline_config()
    config_substitutions = _create_config_substitutions(central_config)

    # Convert config dict to YAML string for substitution
    yaml_content = yaml.dump(config, default_flow_style=False)

    # Apply substitutions
    substituted_yaml = _apply_config_substitutions(yaml_content, config_substitutions)

    # Parse back to dict
    substituted_config = yaml.safe_load(substituted_yaml)

    # Apply temp table processing
    return _preprocess_temp_tables(substituted_config)


def _generate_temp_table_name_for_config(original_table: str) -> str:
    """Generate a temporary table name in the format: z_temp_{timestamp}_{random32}_{original_table}"""
    import random
    import string
    from datetime import datetime

    # Generate timestamp in format yyyyMMddhhmmssmmm
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]  # Remove microseconds to milliseconds

    # Generate 32-bit random integer (8 hex characters)
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    return f"z_temp_{timestamp}_{random_suffix}_{original_table}"


def _substitute_table_names_in_sql(sql_query: str, temp_mappings: dict) -> str:
    """Substitute original table names with temp table names in SQL query."""
    import re
    result = sql_query
    for original_name, temp_name in temp_mappings.items():
        # Use word boundaries to avoid partial matches
        # Match table name as a whole word, possibly with schema prefix
        pattern = r'\b(\w+\.)?' + re.escape(original_name) + r'\b'
        result = re.sub(pattern, r'\1' + temp_name, result)
    return result


def load_pipeline_configs_from_dir(directory: Path) -> dict[str, dict]:
    """Load all pipeline configurations from YAML files (with optional Jinja2 templating)."""
    configs = {}
    if directory.exists():
        # Look for <dirname>.yaml files in subdirectories
        for subdir in directory.iterdir():
            if subdir.is_dir():
                dirname = subdir.name
                pipeline_yaml = subdir / f"{dirname}.yaml"

                if pipeline_yaml.exists():
                    config = _load_yaml_with_template(pipeline_yaml)
                    # Apply configuration substitutions and temp table processing
                    config = _preprocess_with_config(config)
                    name = dirname

                    # Add pipeline directory info for SQL file resolution
                    config["_pipeline_dir"] = subdir
                    configs[name] = config

        # Also check for *.yaml files directly in directory (legacy support)
        for yaml_file in directory.glob("*.yaml"):
            name = yaml_file.stem
            if name not in configs:  # Don't overwrite if already loaded from subdirectory
                config = _load_yaml_with_template(yaml_file)
                # Apply configuration substitutions and temp table processing
                config = _preprocess_with_config(config)
                # For legacy files, pipeline_dir is the parent directory
                config["_pipeline_dir"] = directory
                configs[name] = config

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
    depends_on = step_config.get("depends_on", [])
    
    # Special case for cleanup step which might have multiple dependencies but no data input
    is_cleanup = step_name == "cleanup_temp_tables"
    
    # Define inputs for the op
    ins = {}
    if is_cleanup:
        ins["wait_for"] = In(Nothing)
    elif has_inputs:
        ins["data"] = In()
    
    # Add Nothing inputs for any dependencies that aren't the primary data input
    # (Excluding the first dependency which is handled by 'data' or 'wait_for')
    for i in range(1, len(depends_on)):
        ins[f"wait_{i}"] = In(Nothing)

    step_cfg = step_config.get("config", {}).copy()
    # Include pipeline directory for SQL file resolution
    if "_pipeline_dir" in step_config:
        step_cfg["_pipeline_dir"] = step_config["_pipeline_dir"]

    # Extract concurrency settings
    concurrency_key = step_config.get("concurrency_key")
    op_tags = {}
    if concurrency_key:
        op_tags["dagster/concurrency_key"] = concurrency_key

    if is_batching:
        # Batching op: produces DynamicOut
        @op(name=f"{job_name}_{step_name}", ins=ins, out=DynamicOut(), tags=op_tags)
        def step_op(context: OpExecutionContext, **kwargs):
            data = kwargs.get("data")
            result = executor_func(context, step_cfg, data) if has_inputs else executor_func(context, step_cfg)
            for batch_key, batch_data in result:
                yield DynamicOutput(batch_data, mapping_key=str(batch_key))
        return step_op

    # Regular op
    @op(name=f"{job_name}_{step_name}", ins=ins, out=Out() if has_outputs else None, tags=op_tags)
    def step_op(context: OpExecutionContext, **kwargs):
        data = kwargs.get("data")
        return executor_func(context, step_cfg, data) if has_inputs else executor_func(context, step_cfg)
    
    return step_op


def make_graph_asset_from_steps(
    job_name: str, asset_keys: list, dep_keys: set, resolved_steps: list, temp_mappings: dict = None
):
    """Create a graph-backed asset (or multi-asset) from pipeline steps."""

    if not resolved_steps:
        raise ValueError(f"No steps defined for job: {job_name}")

    # Find all original leaf nodes (steps that no other step depends on)
    all_deps = set()
    for s in resolved_steps:
        all_deps.update(s.get("depends_on", []))
    original_leaf_names = [s["name"] for s in resolved_steps if s["name"] not in all_deps]

    # Add cleanup step for temp tables if any exist
    if temp_mappings:
        # Extract connection info from the first Trino step if available
        trino_config = {}
        for step in resolved_steps:
            if "trino" in step.get("executor", ""):
                step_config = step.get("config", {})
                trino_config = {
                    "host": step_config.get("host"),
                    "port": step_config.get("port"),
                    "user": step_config.get("user"),
                }
                break

        cleanup_step = {
            "name": "cleanup_temp_tables",
            "executor": "trino_execute",
            "depends_on": original_leaf_names, # Run after all original leaf nodes
            "config": {
                "host": trino_config.get("host", "trino-0a966bea-trino.trino.svc.cluster.local"),
                "port": trino_config.get("port", 8080),
                "user": trino_config.get("user", "dagster"),
                "select_query": ";".join([f"DROP TABLE IF EXISTS lakehouse.test.{temp_table}"
                                        for temp_table in temp_mappings.values()])
            }
        }
        resolved_steps = resolved_steps + [cleanup_step]
        
        # To ensure the original leaf nodes are the ones that "finish" the asset materialization
        # but ONLY after cleanup is done, we add a pass-through step for each original leaf node
        # that depends on both the original leaf node AND the cleanup step.
        for leaf_name in original_leaf_names:
            passthrough_name = f"final_{leaf_name}"
            # Find the original step to see if it has batching
            original_step = next(s for s in resolved_steps if s["name"] == leaf_name)
            is_batching = original_step.get("config", {}).get("batch_size") is not None
            
            resolved_steps.append({
                "name": passthrough_name,
                "executor": "passthrough", 
                "depends_on": [leaf_name, "cleanup_temp_tables"],
                "inputs": [leaf_name],
                "config": {}
            })
        
        # The NEW leaf nodes we map to assets are these pass-through steps
        leaf_names = [f"final_{leaf_name}" for leaf_name in original_leaf_names]
    else:
        leaf_names = original_leaf_names

    # Heuristic to map leaf nodes to asset keys if we have multiple of both
    asset_key_objects = [AssetKey(ak) for ak in asset_keys]
    
    # Internal function to define the graph logic
    def define_graph():
        outputs = {}
        step_is_dynamic = {}

        # First step (must have no inputs)
        first_step = step_ops_list[0]
        outputs[first_step["name"]] = first_step["op"]()
        step_is_dynamic[first_step["name"]] = first_step["is_batching"]

        # Process remaining steps in order (topologically sorted)
        for step_info in step_ops_list[1:]:
            name = step_info["name"]
            step_op = step_info["op"]
            depends_on = step_info["config"].get("depends_on", [])
            executor_name = step_info["config"].get("executor")
            
            if not depends_on:
                outputs[name] = step_op()
                step_is_dynamic[name] = step_info["is_batching"]
                continue

            # Use the first dependency for the 'data' or 'wait_for' input
            dep_name = depends_on[0]
            dep_output = outputs[dep_name]
            
            # Prepare all inputs for the step
            kwargs = {}
            if name == "cleanup_temp_tables":
                kwargs["wait_for"] = outputs[dep_name]
            elif step_info["has_inputs"]:
                kwargs["data"] = outputs[dep_name]
            
            # Add extra dependencies as 'wait_i' inputs
            for i in range(1, len(depends_on)):
                kwargs[f"wait_{i}"] = outputs[depends_on[i]]

            if step_is_dynamic[dep_name]:
                if executor_name == "batch_fan_in":
                    # Fan-in: collect all parallel outputs into a list
                    # This collapses the dynamic stream
                    outputs[name] = step_op(data=dep_output.collect(), **{k:v for k,v in kwargs.items() if k != "data"})
                    step_is_dynamic[name] = step_info["is_batching"]
                else:
                    # Continue parallel processing using .map()
                    # Map only works on the primary data input
                    outputs[name] = dep_output.map(step_op, **{k:v for k,v in kwargs.items() if k != "data"})
                    step_is_dynamic[name] = True # Map propagates dynamic status
            else:
                # Regular invocation
                outputs[name] = step_op(**kwargs)
                step_is_dynamic[name] = step_info["is_batching"]

        return outputs

    if len(asset_key_objects) > 1 and len(leaf_names) > 1:
        # Multi-asset support for parallel outputs using AssetsDefinition.from_graph
        keys_by_output_name = {}
        remaining_keys = asset_key_objects.copy()
        remaining_leaves = leaf_names.copy()

        # Match Trino leaves to lakehouse keys
        trino_leaves = [l for l in remaining_leaves if "trino" in l.lower()]
        lakehouse_keys = [k for k in remaining_keys if k.path[0].lower() == "lakehouse"]
        for i in range(min(len(trino_leaves), len(lakehouse_keys))):
            leaf = trino_leaves[i]
            key = lakehouse_keys[i]
            keys_by_output_name[leaf] = key
            remaining_leaves.remove(leaf)
            remaining_keys.remove(key)

        # Match S3 leaves to s3 keys
        s3_leaves = [l for l in remaining_leaves if "s3" in l.lower()]
        s3_keys = [k for k in remaining_keys if k.path[0].lower() == "s3"]
        for i in range(min(len(s3_leaves), len(s3_keys))):
            leaf = s3_leaves[i]
            key = s3_keys[i]
            keys_by_output_name[leaf] = key
            remaining_leaves.remove(leaf)
            remaining_keys.remove(key)

        # Map remaining leaves to remaining keys by order
        for i in range(min(len(remaining_leaves), len(remaining_keys))):
            leaf = remaining_leaves[i]
            key = remaining_keys[i]
            keys_by_output_name[leaf] = key

        # Define the graph with explicit GraphOuts matching leaf names
        @graph(name=f"{job_name}_graph", out={leaf: GraphOut() for leaf in keys_by_output_name.keys()})
        def pipeline_graph():
            outputs = define_graph()
            return {leaf: outputs[leaf] for leaf in keys_by_output_name.keys()}

        return AssetsDefinition.from_graph(
            pipeline_graph,
            keys_by_output_name=keys_by_output_name,
            can_subset=True,
        )

    # Single asset or joining multiple leaves into one asset
    @graph_asset(
        name=asset_key_objects[0].path[-1],
        key_prefix=asset_key_objects[0].path[:-1] if len(asset_key_objects[0].path) > 1 else None,
    )
    def graph_backed_asset():
        outputs = define_graph()
        
        if len(leaf_names) > 1:
            # Join non-dynamic leaves if possible
            if not any(step_is_dynamic[name] for name in leaf_names):
                ins = {f"in_{i}": In() for i in range(len(leaf_names))}
                @op(name=f"{job_name}_join_outputs", ins=ins)
                def join_outputs(**kwargs):
                    return list(kwargs.values())[-1]
                return join_outputs(**{f"in_{i}": outputs[name] for i, name in enumerate(leaf_names)})
        
        return outputs[step_ops_list[-1]["name"]]

    return graph_backed_asset


def make_asset_for_pipeline(job_name: str, config: dict):
    """Create asset(s) for a pipeline configuration based on its steps."""
    # Support both 'asset_key' and 'asset_keys'
    asset_keys = config.get("asset_keys")
    if not asset_keys:
        single_key = config.get("asset_key")
        if not single_key:
            raise ValueError(f"Missing 'asset_key' or 'asset_keys' in config for job: {job_name}")
        asset_keys = [single_key]

    # Get dependencies
    depends_on = config.get("depends_on", [])
    dep_keys = {AssetKey(dep) for dep in depends_on}

    # Get steps
    steps = config.get("steps", [])
    if not steps:
        raise ValueError(f"No steps defined in config for job: {job_name}")

    # Add pipeline directory info to steps for SQL file resolution
    pipeline_dir = config.get("_pipeline_dir")
    for step in steps:
        if pipeline_dir:
            step["_pipeline_dir"] = pipeline_dir
            # Also add to the config section so it gets passed to executors
            if "config" not in step:
                step["config"] = {}
            step["config"]["_pipeline_dir"] = pipeline_dir

    # Resolve step dependencies for execution order
    resolved_steps = resolve_step_dependencies(steps)

    # Get temp table mappings for cleanup
    temp_mappings = config.get("_temp_table_mappings", {})

    # Multi-asset handles multiple parallel outputs as separate Dagster assets
    return make_graph_asset_from_steps(job_name, asset_keys, dep_keys, resolved_steps, temp_mappings)


def generate_definitions_for_workspace(workspace_name: str) -> Definitions:
    """Generate Dagster definitions for a specific workspace (subdirectory)."""
    workspace_dir = PIPELINES_BASE_DIR / workspace_name
    configs = load_pipeline_configs_from_dir(workspace_dir)

    assets = []
    jobs = []
    schedules = []
    sensors = []

    # Create all assets
    for job_name, config in configs.items():
        asset_keys = config.get("asset_keys")
        if not asset_keys:
            asset_key = config.get("asset_key")
            if not asset_key:
                continue
            asset_keys = [asset_key]

        pipeline_asset = make_asset_for_pipeline(job_name, config)
        assets.append(pipeline_asset)

    # Create jobs for assets that have schedules
    for job_name, config in configs.items():
        schedule_cron = config.get("schedule")
        if schedule_cron:
            asset_keys = config.get("asset_keys")
            if not asset_keys:
                asset_key = config.get("asset_key")
                if not asset_key:
                    continue
                asset_keys = [asset_key]

            # Create asset keys list for job selection
            asset_key_objects = [AssetKey(key) for key in asset_keys if key]

            # Create a job that materializes these specific assets
            job_concurrency = config.get("job_concurrency")
            if job_concurrency:
                # Custom job with concurrency limits
                @job(
                    name=f"{job_name}_job",
                    executor_def=multiprocess_executor.configured({
                        "max_concurrent": job_concurrency
                    })
                )
                def asset_job():
                    # Import here to avoid circular imports
                    from dagster import materialize
                    materialize(asset_key_objects)
                jobs.append(asset_job)
            else:
                # Default job without concurrency limits
                asset_job = define_asset_job(
                    name=f"{job_name}_job",
                    selection=asset_key_objects,
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

    # Create sensors for pipelines with retry configuration (only if they have a schedule/job)
    for job_name, config in configs.items():
        schedule_cron = config.get("schedule")
        retry_config = config.get("job_retry")
        if retry_config and schedule_cron:  # Only create sensor if job exists (created by schedule)
            retry_sensor = create_job_retry_sensor(job_name, config)
            sensors.append(retry_sensor)

    return Definitions(
        assets=assets,
        jobs=jobs,
        schedules=schedules,
        sensors=sensors,
    )

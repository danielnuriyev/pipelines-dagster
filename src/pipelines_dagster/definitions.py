"""Dynamically generated Dagster definitions from YAML configuration files."""

import os
from pathlib import Path

import yaml
from dagster import (
    Config,
    DagsterRunStatus,
    Definitions,
    OpExecutionContext,
    RunRequest,
    job,
    op,
    run_status_sensor,
)

from pipelines_dagster.ops import (
    execute_s3_to_trino,
    execute_trino_insert_select,
    execute_trino_to_s3,
)

# Directory containing pipeline YAML configurations
PIPELINES_DIR = Path(os.environ.get("PIPELINES_CONFIG_DIR", "/app/pipelines"))


def load_all_pipeline_configs() -> dict[str, dict]:
    """Load all pipeline configurations from YAML files."""
    configs = {}
    if PIPELINES_DIR.exists():
        for yaml_file in PIPELINES_DIR.glob("*.yaml"):
            name = yaml_file.stem
            with open(yaml_file) as f:
                configs[name] = yaml.safe_load(f)
    return configs


# Map pipeline types to their executor functions
PIPELINE_EXECUTORS = {
    "trino_insert_select": execute_trino_insert_select,
    "trino_to_s3": execute_trino_to_s3,
    "s3_to_trino": execute_s3_to_trino,
}


# =============================================================================
# Dynamic Op and Job Generation
# =============================================================================


def make_op_for_pipeline(job_name: str, config: dict):
    """Create an op for a pipeline configuration."""
    pipeline_name = config.get("name")
    executor = PIPELINE_EXECUTORS.get(pipeline_name)

    if not executor:
        raise ValueError(f"Unknown pipeline name: {pipeline_name} for job: {job_name}")

    # Create a Config class with the pipeline's default values
    class PipelineConfig(Config):
        pass

    @op(name=f"{job_name}_op")
    def pipeline_op(context: OpExecutionContext):
        """Dynamically generated op for {job_name}."""
        context.log.info(f"Executing job: {job_name} (pipeline: {pipeline_name})")
        executor(context, config)

    # Update docstring
    pipeline_op.__doc__ = f"Op for job '{job_name}' (pipeline: {pipeline_name})"

    return pipeline_op


def make_job_for_pipeline(name: str, pipeline_op):
    """Create a job for a pipeline op."""

    @job(name=name)
    def pipeline_job():
        pipeline_op()

    pipeline_job.__doc__ = f"Job for pipeline '{name}'"

    return pipeline_job


def generate_pipelines() -> tuple[list, list, dict]:
    """Generate all ops, jobs, and sensors from YAML configurations."""
    configs = load_all_pipeline_configs()

    ops = {}
    jobs = {}
    sensors = []

    # First pass: create all ops and jobs
    for job_name, config in configs.items():
        pipeline_name = config.get("name")
        if not pipeline_name:
            continue  # Skip configs without a name

        pipeline_op = make_op_for_pipeline(job_name, config)
        pipeline_job = make_job_for_pipeline(job_name, pipeline_op)

        ops[job_name] = pipeline_op
        jobs[job_name] = pipeline_job

    # Second pass: create sensors for "after" triggers
    # If job B has "after: [job_a, job_c]", create a sensor that triggers B when ANY of them succeeds
    for job_name, config in configs.items():
        after = config.get("after")
        if not after:
            continue

        # Normalize to list
        if isinstance(after, str):
            after_list = [after]
        else:
            after_list = after

        # Filter to only jobs that exist
        monitored_jobs_list = [jobs[dep] for dep in after_list if dep in jobs]

        if monitored_jobs_list:
            target_job = jobs[job_name]  # This job runs after

            # Create sensor name from dependencies
            deps_suffix = "_".join(after_list[:2])  # Limit name length
            if len(after_list) > 2:
                deps_suffix += f"_and_{len(after_list) - 2}_more"

            sensor = run_status_sensor(
                name=f"{job_name}_after_{deps_suffix}_sensor",
                run_status=DagsterRunStatus.SUCCESS,
                monitored_jobs=monitored_jobs_list,
                request_job=target_job,
            )(lambda context: RunRequest())

            sensors.append(sensor)

    return list(jobs.values()), sensors, ops


# =============================================================================
# Static definitions (noop job for health checks)
# =============================================================================


@op
def noop_op():
    """An op that does nothing."""
    pass


@job
def noop_job():
    """A job that does nothing - used for health checks."""
    noop_op()


# =============================================================================
# Generate all definitions
# =============================================================================

generated_jobs, generated_sensors, _ops = generate_pipelines()

defs = Definitions(
    jobs=[noop_job, *generated_jobs],
    sensors=generated_sensors,
)

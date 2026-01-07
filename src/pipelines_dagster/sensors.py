"""Sensors for intelligent job retry logic."""

import re
from typing import Dict, Any, Optional

from dagster import (
    sensor,
    RunFailureSensorContext,
    RunRequest,
    DagsterRunStatus,
    OpExecutionContext,
    DefaultSensorStatus,
    SensorDefinition,
)

from pipelines_dagster.retry_utils import parse_time_with_units


def detect_failure_reason(context: RunFailureSensorContext) -> str:
    """
    Analyze run failure to determine the cause.

    Returns:
        - "oom": Out of memory
        - "pod_deleted": Pod was deleted/terminated
        - "other": Other failure
    """
    try:
        # Get failure event logs
        events = context.instance.get_run_failure_events(context.dagster_run.run_id)

        for event in events:
            error_message = str(event).lower()

            # Check for out-of-memory errors
            oom_indicators = [
                "out of memory",
                "oom killed",
                "memory limit exceeded",
                "cannot allocate memory",
                "killed by oom",
            ]
            if any(indicator in error_message for indicator in oom_indicators):
                return "oom"

            # Check for pod deletion/termination
            pod_deletion_indicators = [
                "pod was deleted",
                "terminated",
                "evicted",
                "node failure",
                "preempted",
            ]
            if any(indicator in error_message for indicator in pod_deletion_indicators):
                return "pod_deleted"

    except Exception as e:
        context.log.warning(f"Could not analyze failure reason: {e}")

    return "other"


def get_retry_config(pipeline_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract retry configuration from pipeline YAML."""
    config = pipeline_config.get("job_retry", {})

    # Apply hardcoded defaults for removed YAML fields
    config.setdefault("base_delay", 60)  # 60 seconds
    config.setdefault("backoff_factor", 2.0)  # Exponential backoff
    config.setdefault("retry_on_memory_failure", True)  # Enable memory scaling
    config.setdefault("memory_multiplier", 2.0)  # Double memory

    return config


def calculate_retry_delay(
    attempt: int, base_delay: float, backoff_factor: float, max_delay: float
) -> float:
    """Calculate retry delay with exponential backoff."""
    delay = base_delay * (backoff_factor ** attempt)
    return min(delay, max_delay)


def create_job_retry_sensor(
    job_name: str, pipeline_config: Dict[str, Any]
) -> SensorDefinition:
    """
    Create a sensor for intelligent job retries.

    Args:
        job_name: Name of the job to monitor
        pipeline_config: Pipeline configuration from YAML
    """

    @sensor(
        name=f"{job_name}_retry_sensor",
        job_name=f"{job_name}_job",  # Monitor the scheduled job
        default_status=DefaultSensorStatus.STOPPED,  # Enable manually
        minimum_interval_seconds=30,
    )
    def job_retry_sensor(context: RunFailureSensorContext):
        """Sensor that intelligently retries failed jobs."""
        retry_config = get_retry_config(pipeline_config)

        if not retry_config:
            context.log.info("No retry config found, skipping retry")
            return None

        # Check if this run has already been retried too many times
        current_attempt = int(context.dagster_run.tags.get("retry_attempt", "0"))
        max_attempts = retry_config.get("max_attempts", 3)

        if current_attempt >= max_attempts:
            context.log.info(
                f"Run {context.dagster_run.run_id} has exceeded max retries ({max_attempts})"
            )
            return None

        # Analyze failure reason
        failure_reason = detect_failure_reason(context)
        context.log.info(f"Detected failure reason: {failure_reason}")

        # Determine retry strategy based on failure type
        next_attempt = current_attempt + 1
        base_delay = parse_time_with_units(retry_config.get("base_delay", 60))
        backoff_factor = retry_config.get("backoff_factor", 2.0)
        max_delay = parse_time_with_units(retry_config.get("max_delay", 3600))

        delay = calculate_retry_delay(next_attempt - 1, base_delay, backoff_factor, max_delay)

        # Prepare retry tags and config
        retry_tags = {
            "retry_attempt": str(next_attempt),
            "original_run_id": context.dagster_run.run_id,
            "failure_reason": failure_reason,
        }

        run_config = {}

        if failure_reason == "oom" and retry_config.get("retry_on_memory_failure", False):
            # Scale memory for OOM failures
            memory_multiplier = retry_config.get("memory_multiplier", 2.0)

            retry_tags.update({
                "memory_scaled": "true",
                "memory_multiplier": str(memory_multiplier),
            })

            context.log.info(
                f"Retrying with {memory_multiplier}x memory due to OOM failure"
            )

        elif failure_reason == "pod_deleted":
            # Normal retry for pod deletion - infrastructure issue
            context.log.info("Retrying after pod deletion - infrastructure issue")

        else:
            # Normal retry for other failures
            context.log.info(f"Retrying after {failure_reason} failure")

        # Schedule the retry
        context.log.info(
            f"Scheduling retry {next_attempt}/{max_attempts} in {delay} seconds"
        )

        return RunRequest(
            run_key=f"{context.dagster_run.run_id}_retry_{next_attempt}",
            run_config=run_config,
            tags=retry_tags,
            # Note: In a real implementation, you'd want to pass delay to a queue
            # For now, this creates an immediate retry (delay handled externally)
        )

    return job_retry_sensor

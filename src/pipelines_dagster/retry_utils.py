"""Retry utilities for external service connections."""

import re
import time
from typing import Any, Callable, Type, Union

import trino
import snowflake.connector
from botocore.exceptions import ClientError
from dagster import OpExecutionContext


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        jitter: bool = True
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter


def parse_time_with_units(time_str: Union[str, float, int]) -> float:
    """
    Parse time string with units (e.g., "1s", "2m", "3h", "4d", "5w") into seconds.

    Args:
        time_str: Time value as string with units or numeric value

    Returns:
        Time in seconds as float

    Examples:
        "1s" -> 1.0
        "2m" -> 120.0
        "3h" -> 10800.0
        "4d" -> 345600.0
        "5w" -> 3024000.0
        60 -> 60.0
        60.5 -> 60.5
    """
    if isinstance(time_str, (int, float)):
        return float(time_str)

    if not isinstance(time_str, str):
        raise ValueError(f"Invalid time format: {time_str}")

    # Match pattern like "1s", "2.5m", "3h", etc.
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([smhdw]?)$', time_str.strip().lower())
    if not match:
        raise ValueError(f"Invalid time format: {time_str}. Expected format: <number><unit> where unit is s/m/h/d/w")

    value, unit = match.groups()
    value = float(value)

    # Unit multipliers (in seconds)
    multipliers = {
        's': 1,           # seconds
        'm': 60,          # minutes
        'h': 3600,        # hours
        'd': 86400,       # days
        'w': 604800,      # weeks
        '': 1             # no unit = seconds
    }

    if unit not in multipliers:
        raise ValueError(f"Unknown time unit: {unit}. Supported units: s, m, h, d, w")

    return value * multipliers[unit]


def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    context: OpExecutionContext,
    *args,
    **kwargs
) -> Any:
    """
    Execute a function with exponential backoff retry logic.

    Args:
        func: Function to execute
        config: Retry configuration
        context: Dagster execution context for logging
        *args, **kwargs: Arguments to pass to func
    """
    last_exception = None

    for attempt in range(config.max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e

            if attempt < config.max_attempts - 1:  # Not the last attempt
                delay = min(
                    config.base_delay * (config.backoff_factor ** attempt),
                    config.max_delay
                )

                if config.jitter:
                    # Add random jitter to prevent thundering herd
                    import random
                    delay = delay * (0.5 + random.random() * 0.5)

                context.log.warning(
                    f"Attempt {attempt + 1}/{config.max_attempts} failed: {e}. "
                    f"Retrying in {delay:.1f} seconds..."
                )
                time.sleep(delay)
            else:
                context.log.error(
                    f"All {config.max_attempts} attempts failed. Last error: {e}"
                )
                raise e

    # This should never be reached, but just in case
    raise last_exception


def is_retryable_trino_error(error: Exception) -> bool:
    """Determine if a Trino error should be retried."""
    if isinstance(error, trino.exceptions.TrinoQueryError):
        # Retry on connection errors, timeouts, and temporary server errors
        error_message = str(error).lower()
        return any(keyword in error_message for keyword in [
            "connection",
            "timeout",
            "temporary",
            "server error",
            "service unavailable",
            "too many requests"
        ])
    elif isinstance(error, trino.exceptions.TrinoConnectionError):
        return True
    return False


def is_retryable_s3_error(error: Exception) -> bool:
    """Determine if an S3 error should be retried."""
    if isinstance(error, ClientError):
        error_code = error.response.get('Error', {}).get('Code', '')
        # Retry on throttling, temporary errors, and connection issues
        retryable_codes = [
            'ThrottlingException',
            'RequestTimeout',
            'InternalError',
            'ServiceUnavailable',
            'SlowDown',
            'TooManyRequests'
        ]
        return error_code in retryable_codes
    return False


def is_retryable_snowflake_error(error: Exception) -> bool:
    """Determine if a Snowflake error should be retried."""
    if isinstance(error, snowflake.connector.errors.Error):
        # Check error message for retryable conditions
        error_message = str(error).lower()
        return any(keyword in error_message for keyword in [
            "connection",
            "timeout",
            "temporary",
            "server error",
            "service unavailable",
            "too many requests",
            "rate limit",
            "throttling"
        ])
    return False


# Default retry configurations (fallback values)
DEFAULT_TRINO_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=30.0
)

DEFAULT_S3_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=20.0
)

DEFAULT_SNOWFLAKE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=30.0
)


def get_retry_config_from_yaml(yaml_config: dict, service_type: str = "trino") -> RetryConfig:
    """
    Extract retry configuration from YAML config.

    Args:
        yaml_config: Step configuration from YAML
        service_type: "trino", "s3", or "snowflake" for default fallbacks

    Returns:
        RetryConfig with values from YAML or defaults
    """
    retry_config = yaml_config.get("retry", {})

    # Use defaults based on service type
    if service_type == "trino":
        defaults = DEFAULT_TRINO_RETRY_CONFIG
    elif service_type == "snowflake":
        defaults = DEFAULT_SNOWFLAKE_RETRY_CONFIG
    else:  # s3 or default
        defaults = DEFAULT_S3_RETRY_CONFIG

    return RetryConfig(
        max_attempts=retry_config.get("max_attempts", defaults.max_attempts),
        base_delay=parse_time_with_units(retry_config.get("base_delay", defaults.base_delay)),
        max_delay=parse_time_with_units(retry_config.get("max_delay", defaults.max_delay)),
        backoff_factor=retry_config.get("backoff_factor", defaults.backoff_factor),
        jitter=retry_config.get("jitter", defaults.jitter)
    )

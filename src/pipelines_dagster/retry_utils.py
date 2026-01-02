"""Retry utilities for external service connections."""

import time
from typing import Any, Callable, Type

import trino
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


def get_retry_config_from_yaml(yaml_config: dict, service_type: str = "trino") -> RetryConfig:
    """
    Extract retry configuration from YAML config.

    Args:
        yaml_config: Step configuration from YAML
        service_type: "trino" or "s3" for default fallbacks

    Returns:
        RetryConfig with values from YAML or defaults
    """
    retry_config = yaml_config.get("retry", {})

    # Use defaults based on service type
    defaults = DEFAULT_TRINO_RETRY_CONFIG if service_type == "trino" else DEFAULT_S3_RETRY_CONFIG

    return RetryConfig(
        max_attempts=retry_config.get("max_attempts", defaults.max_attempts),
        base_delay=retry_config.get("base_delay", defaults.base_delay),
        max_delay=retry_config.get("max_delay", defaults.max_delay),
        backoff_factor=retry_config.get("backoff_factor", defaults.backoff_factor),
        jitter=retry_config.get("jitter", defaults.jitter)
    )

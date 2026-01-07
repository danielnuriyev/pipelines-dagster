"""Factory functions for creating source instances."""

from .trino import TrinoSource
from .snowflake import SnowflakeSource
from .s3 import S3Source


def create_source_from_config(source_name: str, config: dict):
    """Create a source instance based on the source name containing specific strings.

    Args:
        source_name: The name or executor that may contain source type indicators
        config: Configuration dictionary for the source

    Returns:
        An instance of the appropriate source class

    Raises:
        ValueError: If the source type cannot be determined from the name
    """
    source_name_lower = source_name.lower()

    if "trino" in source_name_lower:
        return TrinoSource(config)
    elif "snowflake" in source_name_lower:
        return SnowflakeSource(config)
    elif "s3" in source_name_lower:
        return S3Source(config)
    else:
        raise ValueError(
            f"Cannot determine source type from name '{source_name}'. "
            "Expected name to contain 'trino', 'snowflake', or 's3'"
        )

"""Base classes and utilities for data sources."""

import random
import string
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, Union

import pandas as pd
from dagster import OpExecutionContext


def _load_sql_query(
    sql_query: Optional[str],
    sql_file: Optional[str],
    context: OpExecutionContext,
    pipeline_dir: Optional[Path] = None
) -> str:
    """
    Load SQL query from either inline config or file.

    Args:
        sql_query: Inline SQL query string
        sql_file: Path to SQL file
        context: Dagster execution context
        pipeline_dir: Directory containing the pipeline YAML (for relative SQL file resolution)

    Returns:
        SQL query string

    Raises:
        ValueError: If neither sql_query nor sql_file is specified, or both are specified
    """
    if sql_query and sql_file:
        raise ValueError("Cannot specify both 'sql_query' and 'sql_file'")
    elif sql_query:
        # Inline SQL query (can be string or multi-line YAML)
        if isinstance(sql_query, list):
            return "\n".join(sql_query)
        return sql_query
    elif sql_file:
        sql_file_path = Path(sql_file)

        # If relative path and we have pipeline directory, try same directory first
        if not sql_file_path.is_absolute() and pipeline_dir:
            candidate_path = pipeline_dir / sql_file
            if candidate_path.exists():
                sql_file_path = candidate_path
            else:
                pipelines_dir = Path(__file__).parent.parent / "pipelines"
                sql_file_path = pipelines_dir / sql_file

        if not sql_file_path.is_absolute():
            pipelines_dir = Path(__file__).parent.parent / "pipelines"
            sql_file_path = pipelines_dir / sql_file

        if not sql_file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        context.log.info(f"Loading SQL from file: {sql_file_path}")
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    else:
        raise ValueError("Must specify either 'sql_query' or 'sql_file'")


class Source(ABC):
    """Abstract base class for data sources.

    All source classes must implement:
    - extract(): Extract data and return a DataFrame
    - cleanup(): Clean up any temporary resources (e.g., temp tables)
    - get_schema_prefix(): Return the qualified schema prefix for table names
    - get_cleanup_executor(): Return the executor name for cleanup operations
    """

    def __init__(self, config: dict):
        """Initialize the source from a configuration dictionary."""
        self.select_query = config.get("select_query") or config.get("sql_query")
        self.sql_file = config.get("sql_file")
        self.batch_size = config.get("batch_size")
        self.pk = config.get("pk")
        self.temp = config.get("temp", False)
        self.table = config.get("table") or config.get("target_table")
        self.retry = config.get("retry", {})
        
        # Internal state
        self._temp_table_name = None
        self._pipeline_dir = config.get("_pipeline_dir")
        
        # Public type field (to be set by subclasses)
        self.type: str = "base"

    @abstractmethod
    def extract(self, context: OpExecutionContext) -> Union[pd.DataFrame, Generator]:
        """Extract data from the source.

        Returns either a DataFrame or a generator of (batch_key, DataFrame) tuples
        if batching is enabled.
        """
        pass

    @abstractmethod
    def cleanup(self, context: OpExecutionContext) -> None:
        """Clean up any temporary resources created by this source."""
        pass

    @abstractmethod
    def get_schema_prefix(self) -> str:
        """Return the qualified schema prefix (e.g., 'catalog.schema' or 'database.schema')."""
        pass

    @abstractmethod
    def get_cleanup_executor(self) -> str:
        """Return the executor name to use for cleanup operations."""
        pass

    @abstractmethod
    def get_connection_config(self) -> dict:
        """Return the connection configuration for cleanup operations."""
        pass

    @staticmethod
    def generate_temp_table_name(original_table: str) -> str:
        """Generate a temporary table name in the format: z_temp_{timestamp}_{random32}_{original_table}"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=32))
        return f"z_temp_{timestamp}_{random_suffix}_{original_table}"

    def get_temp_table_name(self) -> Optional[str]:
        """Get the temporary table name, generating one if needed."""
        if self.temp and self.table:
            if self._temp_table_name is None:
                self._temp_table_name = self.generate_temp_table_name(self.table)
            return self._temp_table_name
        return None

    def get_actual_table_name(self) -> Optional[str]:
        """Get the actual table name to use (temp name if temp=True, otherwise original)."""
        if self.temp:
            return self.get_temp_table_name()
        return self.table

    def _get_sql_query(self, context: OpExecutionContext) -> str:
        """Load and return the SQL query."""
        return _load_sql_query(
            self.select_query,
            self.sql_file,
            context,
            self._pipeline_dir
        )

    @classmethod
    def from_config(cls, config: dict) -> "Source":
        """Create a Source instance from a configuration dictionary.
        
        This method is kept for backward compatibility but now calls the constructor.
        """
        return cls(config)

"""Trino data source for extracting data from Trino."""

from typing import Generator, Optional, Union

import pandas as pd
import trino
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    get_retry_config_from_yaml,
    is_retryable_trino_error,
    retry_with_backoff,
)

from .source import Source


class TrinoSource(Source):
    """Source for extracting data from Trino.

    YAML Configuration:
        executor: trino_extract
        config:
          host: trino-host
          port: 8080
          user: dagster
          select_query: SELECT * FROM catalog.schema.table
          # OR
          sql_file: path/to/query.sql
          batch_size: 1000  # Optional
          pk: id  # Required if batch_size is set
          temp: false  # Optional
          table: target_table  # Required if temp is true
          retry:
            max_attempts: 3
            base_delay: 1s
            max_delay: 1m
    """
    def __init__(self, config: dict):
        """Initialize the Trino source from a configuration dictionary."""
        super().__init__(config)
        self.type = "trino"
        self.host = config.get("host", "")
        self.port = config.get("port", 8080)
        self.user = config.get("user", "dagster")
        self.catalog = config.get("catalog")
        self.schema = config.get("schema")

    def _connect(self, context: OpExecutionContext):
        """Create a connection to Trino with retry logic."""
        def connect_trino():
            return trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
            )

        retry_config = get_retry_config_from_yaml({"retry": self.retry}, "trino")
        try:
            return retry_with_backoff(connect_trino, retry_config, context)
        except Exception as e:
            if not is_retryable_trino_error(e):
                raise
            raise

    def extract(self, context: OpExecutionContext) -> Union[pd.DataFrame, Generator]:
        """Extract data from Trino source table."""
        select_query = self._get_sql_query(context)
        context.log.info(f"Connecting to Trino at {self.host}:{self.port}")

        # If batching is requested, use batch generator
        if self.batch_size is not None and self.pk is not None:
            return self._extract_batches(context, select_query)

        # Non-batched: fetch all data
        conn = self._connect(context)
        cursor = conn.cursor()

        context.log.info(f"Executing query: {select_query}")
        cursor.execute(select_query)

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        df = pd.DataFrame(rows, columns=columns)
        df = df.convert_dtypes(dtype_backend='pyarrow')
        context.log.info(f"Extracted {len(df)} rows with columns: {list(df.columns)}")

        return df

    def _extract_batches(self, context: OpExecutionContext, select_query: str) -> Generator:
        """Generate DataFrames for each batch."""
        conn = self._connect(context)
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT MIN({self.pk}), MAX({self.pk})
            FROM ({select_query}) AS base
        """)
        bounds = cursor.fetchone()
        if bounds is None or bounds[0] is None or bounds[1] is None:
            cursor.close()
            conn.close()
            return

        min_pk, max_pk = bounds
        cursor.close()

        current = min_pk
        while current <= max_pk:
            upper = current + self.batch_size
            batch_query = f"""
                SELECT *
                FROM ({select_query}) AS base
                WHERE {self.pk} >= {current}
                  AND {self.pk} < {upper}
            """
            cursor = conn.cursor()
            context.log.info(f"Executing batch query for {current}-{upper - 1}")
            cursor.execute(batch_query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=columns)
            df = df.convert_dtypes(dtype_backend='pyarrow')
            yield current, df

            cursor.close()
            current = upper

        conn.close()

    def cleanup(self, context: OpExecutionContext) -> None:
        """Drop temporary table if one was created."""
        temp_table = self.get_temp_table_name()
        if temp_table:
            conn = self._connect(context)
            cursor = conn.cursor()
            drop_sql = f"DROP TABLE IF EXISTS {self.get_schema_prefix()}.{temp_table}"
            context.log.info(f"Cleaning up temp table: {drop_sql}")
            cursor.execute(drop_sql)
            cursor.fetchall()
            cursor.close()
            conn.close()

    def get_schema_prefix(self) -> str:
        """Return the qualified schema prefix (catalog.schema)."""
        return f"{self.catalog or 'lakehouse'}.{self.schema or 'test'}"

    def get_cleanup_executor(self) -> str:
        """Return the executor name for cleanup operations."""
        return "trino_insert_select"

    def get_connection_config(self) -> dict:
        """Return the connection configuration for cleanup operations."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "catalog": self.catalog,
            "schema": self.schema,
        }

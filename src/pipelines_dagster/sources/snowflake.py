"""Snowflake data source for extracting data from Snowflake."""

from typing import Generator, Optional, Union

import pandas as pd
import snowflake.connector
from dagster import OpExecutionContext

from pipelines_dagster.retry_utils import (
    get_retry_config_from_yaml,
    is_retryable_snowflake_error,
    retry_with_backoff,
)

from .source import Source


class SnowflakeSource(Source):
    """Source for extracting data from Snowflake.

    YAML Configuration:
        executor: snowflake_extract
        config:
          account: my-account.snowflakecomputing.com
          user: myuser
          password: mypassword
          warehouse: COMPUTE_WH
          database: MY_DB
          schema: PUBLIC
          select_query: SELECT * FROM database.schema.table
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
        """Initialize the Snowflake source from a configuration dictionary."""
        super().__init__(config)
        self.type = "snowflake"
        self.account = config.get("account", "")
        self.user = config.get("user", "")
        self.password = config.get("password", "")
        self.warehouse = config.get("warehouse")
        self.database = config.get("database")
        self.schema = config.get("schema")

    def _connect(self, context: OpExecutionContext):
        """Create a connection to Snowflake with retry logic."""
        def connect_snowflake():
            return snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )

        retry_config = get_retry_config_from_yaml({"retry": self.retry}, "snowflake")
        try:
            return retry_with_backoff(connect_snowflake, retry_config, context)
        except Exception as e:
            if not is_retryable_snowflake_error(e):
                raise
            raise

    def extract(self, context: OpExecutionContext) -> Union[pd.DataFrame, Generator]:
        """Extract data from Snowflake source table."""
        select_query = self._get_sql_query(context)
        context.log.info(f"Connecting to Snowflake at {self.account}")

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
            cursor.close()
            conn.close()

    def get_schema_prefix(self) -> str:
        """Return the qualified schema prefix (database.schema)."""
        return f"{self.database or 'DATABASE'}.{self.schema or 'SCHEMA'}"

    def get_cleanup_executor(self) -> str:
        """Return the executor name for cleanup operations."""
        return "snowflake_insert_select"

    def get_connection_config(self) -> dict:
        """Return the connection configuration for cleanup operations."""
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }

"""Pipeline operation executors."""

from pipelines_dagster.ops.trino_insert_select import execute_trino_insert_select
from pipelines_dagster.ops.trino_to_s3 import execute_trino_to_s3

__all__ = ["execute_trino_insert_select", "execute_trino_to_s3"]


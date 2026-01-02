"""Pipeline operation functions for Dagster assets."""

from pipelines_dagster.ops.s3_to_trino import s3_to_trino_op
from pipelines_dagster.ops.trino_insert_select import trino_insert_select_op
from pipelines_dagster.ops.trino_extract import trino_extract_op, trino_load_op
from pipelines_dagster.ops.trino_to_s3 import trino_to_s3_op

__all__ = [
    "trino_insert_select_op",
    "trino_to_s3_op",
    "s3_to_trino_op",
    "trino_extract_op",
    "trino_load_op",
]

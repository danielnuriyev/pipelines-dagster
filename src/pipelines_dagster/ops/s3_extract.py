"""S3 extract operation - download CSV from S3 and return pandas DataFrame.

This module provides backward-compatible functions that delegate to the S3Source class.
For new code, prefer using S3Source directly from pipelines_dagster.sources.
"""

import pandas as pd
from dagster import OpExecutionContext

from pipelines_dagster.sources import S3Source


def s3_extract_op(context: OpExecutionContext, config: dict) -> pd.DataFrame:
    """Extract CSV data from S3 and return as pandas DataFrame.
    
    This function delegates to S3Source.extract() for the actual implementation.
    """
    source = S3Source.from_config(config)
    return source.extract(context)

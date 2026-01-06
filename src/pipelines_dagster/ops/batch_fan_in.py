"""Batch fan-in operations for combining parallel batch results."""

import pandas as pd
from typing import List, Dict, Any
from dagster import OpExecutionContext


def batch_fan_in_op(context: OpExecutionContext, config: dict, data: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Fan-in operation that combines multiple DataFrames into a single DataFrame.

    Args:
        context: Dagster execution context
        config: Configuration (currently unused)
        data: List of input DataFrames from parallel batches

    Returns:
        A single concatenated DataFrame
    """
    if not data:
        context.log.info("Fan-in: received empty list of DataFrames")
        return pd.DataFrame()

    context.log.info(f"Fan-in: combining {len(data)} batch results")
    
    # Concatenate all DataFrames in the list
    combined_df = pd.concat(data, ignore_index=True)
    
    context.log.info(f"Fan-in: produced combined DataFrame with {len(combined_df)} rows, {len(combined_df.columns)} columns")
    
    return combined_df

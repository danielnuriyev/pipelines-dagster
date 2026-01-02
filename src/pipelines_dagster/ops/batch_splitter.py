"""Batch splitter executor for further subdividing batches."""

from typing import Iterator

import pandas as pd
from dagster import OpExecutionContext


def batch_splitter_op(
    context: OpExecutionContext, config: dict, df: pd.DataFrame
) -> Iterator[tuple[int, pd.DataFrame]]:
    """
    Split a DataFrame into smaller batches.
    
    This is useful for creating nested batching where one step produces batches,
    and a following step subdivides those batches further.
    
    Args:
        context: Dagster execution context
        config: Configuration with batch_size and pk
        df: Input DataFrame to split
        
    Yields:
        Tuples of (batch_key, batch_dataframe)
    """
    batch_size = config.get("batch_size")
    pk_column = config.get("pk")
    
    if not batch_size:
        raise ValueError("batch_size must be provided for batch_splitter")
    
    if not pk_column:
        raise ValueError("pk must be provided for batch_splitter")
    
    # Sort by PK to ensure consistent batching
    df_sorted = df.sort_values(by=pk_column)
    total_rows = len(df_sorted)
    
    context.log.info(
        f"Batch splitter: subdividing {total_rows} rows into batches of {batch_size}"
    )
    
    for batch_idx, start_idx in enumerate(range(0, total_rows, batch_size)):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df_sorted.iloc[start_idx:end_idx]
        
        # Use the first PK value as the batch key
        batch_key = batch_df[pk_column].iloc[0] if len(batch_df) > 0 else batch_idx
        
        context.log.info(
            f"Batch splitter yielding sub-batch {batch_idx + 1}: "
            f"rows {start_idx}-{end_idx - 1} (key={batch_key})"
        )
        yield batch_key, batch_df


"""Generic batch generator wrapper for any executor."""

from typing import Any, Callable, Iterator

from dagster import OpExecutionContext


def generic_batch_generator(
    context: OpExecutionContext,
    config: dict,
    input_data: Any,
    executor_func: Callable,
) -> Iterator[tuple[int, Any]]:
    """
    Generic batch generator that wraps any executor.
    
    For data that's already in memory (like a DataFrame), splits it into batches.
    For executors that need to generate batches from a data source, delegates to them.
    """
    import pandas as pd
    
    batch_size = config.get("batch_size")
    pk_column = config.get("pk")
    
    if not batch_size:
        raise ValueError("batch_size must be provided for batching")
    
    # If input_data is a pandas DataFrame, batch it
    if isinstance(input_data, pd.DataFrame):
        if not pk_column:
            raise ValueError("pk must be provided for batching DataFrames")
        
        # Sort by PK to ensure consistent batching
        df_sorted = input_data.sort_values(by=pk_column)
        total_rows = len(df_sorted)
        
        context.log.info(f"Batching {total_rows} rows with batch_size={batch_size}, pk={pk_column}")
        
        for batch_idx, start_idx in enumerate(range(0, total_rows, batch_size)):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df_sorted.iloc[start_idx:end_idx]
            
            # Use the first PK value as the batch key
            batch_key = batch_df[pk_column].iloc[0] if len(batch_df) > 0 else batch_idx
            
            context.log.info(f"Yielding batch {batch_idx + 1}: rows {start_idx}-{end_idx - 1} (key={batch_key})")
            yield batch_key, batch_df
    
    else:
        # For other data types, the executor must handle batching itself
        # Just pass through to the executor and expect it to yield batches
        raise NotImplementedError(
            f"Batching for data type {type(input_data)} not implemented. "
            "Executor must handle batching internally."
        )


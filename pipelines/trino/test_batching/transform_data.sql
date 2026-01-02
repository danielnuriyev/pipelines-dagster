-- Transform extracted data using DuckDB SQL
-- This demonstrates complex SQL transformations on DataFrames

SELECT
  id,
  ts,
  -- Add computed columns
  CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END as parity,
  -- Extract date/time components
  strftime(ts, '%Y-%m-%d') as date_only,
  strftime(ts, '%H:%M:%S') as time_only,
  -- Add some aggregations per group
  ROW_NUMBER() OVER (ORDER BY ts) as row_num
FROM input_df
WHERE id <= 50  -- Filter to first 50 records
ORDER BY ts DESC

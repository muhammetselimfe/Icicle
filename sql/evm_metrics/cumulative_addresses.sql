-- Cumulative addresses metric - total unique addresses seen up to each period
-- Parameters: chain_id, first_period, last_period, granularity
-- Strategy: Find first appearance of each address, then running sum

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
WITH
-- Find the first period each address appeared in (within our range)
first_appearances AS (
    SELECT 
        toStartOf{granularityCamelCase}(min(block_time)) as first_period,
        address
    FROM (
        SELECT from as address, block_time
        FROM raw_traces
        WHERE chain_id = @chain_id
          AND block_time >= @first_period
          AND block_time < @last_period
          AND from != unhex('0000000000000000000000000000000000000000')
        
        UNION ALL
        
        SELECT to as address, block_time
        FROM raw_traces
        WHERE chain_id = @chain_id
          AND block_time >= @first_period
          AND block_time < @last_period
          AND to IS NOT NULL
          AND to != unhex('0000000000000000000000000000000000000000')
    ) AS all_occurrences
    GROUP BY address
),
-- Count new addresses per period
new_per_period AS (
    SELECT 
        first_period as period,
        uniq(address) as new_count
    FROM first_appearances
    GROUP BY period
),
-- Get the baseline: cumulative count before our range
baseline AS (
    SELECT countDistinct(address) as prev_cumulative
    FROM (
        SELECT from as address
        FROM raw_traces
        WHERE chain_id = @chain_id
          AND block_time < @first_period
          AND from != unhex('0000000000000000000000000000000000000000')
        
        UNION ALL
        
        SELECT to as address
        FROM raw_traces
        WHERE chain_id = @chain_id
          AND block_time < @first_period
          AND to IS NOT NULL
          AND to != unhex('0000000000000000000000000000000000000000')
    ) AS historical_addresses
)
-- Running sum of new addresses + baseline
SELECT
    {chain_id} as chain_id,
    'cumulative_addresses' as metric_name,
    '{granularity}' as granularity,
    period,
    (SELECT prev_cumulative FROM baseline) + sum(new_count) OVER (ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
FROM new_per_period
ORDER BY period;
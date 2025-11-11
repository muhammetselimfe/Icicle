-- Cumulative deployers metric - total unique deployers up to each period
-- Parameters: chain_id, first_period, last_period, granularity
-- Strategy: Find first deployment of each address, then running sum

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
WITH
-- Find the first period each deployer deployed in (within our range)
first_deployments AS (
    SELECT 
        toStartOf{granularityCamelCase}(min(block_time)) as first_period,
        from as deployer
    FROM raw_traces
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
      AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
      AND tx_success = true
      AND from != unhex('0000000000000000000000000000000000000000')
    GROUP BY deployer
),
-- Count new deployers per period
new_per_period AS (
    SELECT 
        first_period as period,
        uniq(deployer) as new_count
    FROM first_deployments
    GROUP BY period
),
-- Get the baseline: cumulative count before our range
baseline AS (
    SELECT countDistinct(from) as prev_cumulative
    FROM raw_traces
    WHERE chain_id = @chain_id
      AND block_time < @first_period
      AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
      AND tx_success = true
      AND from != unhex('0000000000000000000000000000000000000000')
)
-- Running sum of new deployers + baseline
SELECT
    {chain_id} as chain_id,
    'cumulative_deployers' as metric_name,
    '{granularity}' as granularity,
    period,
    (SELECT prev_cumulative FROM baseline) + sum(new_count) OVER (ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
FROM new_per_period
ORDER BY period;


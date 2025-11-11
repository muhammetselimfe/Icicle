-- Cumulative contracts metric - total contracts created up to each period
-- Parameters: chain_id, first_period, last_period, granularity
-- Strategy: Count contracts per period, then running sum

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
WITH
-- Count contracts created per period
contracts_per_period AS (
    SELECT
        toStartOf{granularityCamelCase}(block_time) as period,
        count(*) as period_count
    FROM raw_traces
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
      AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
      AND tx_success = true
    GROUP BY period
),
-- Get the baseline: total contracts before our range
baseline AS (
    SELECT count(*) as prev_cumulative
    FROM raw_traces
    WHERE chain_id = @chain_id
      AND block_time < @first_period
      AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
      AND tx_success = true
)
-- Running sum of contracts + baseline
SELECT
    {chain_id} as chain_id,
    'cumulative_contracts' as metric_name,
    '{granularity}' as granularity,
    period,
    (SELECT prev_cumulative FROM baseline) + sum(period_count) OVER (ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
FROM contracts_per_period
ORDER BY period;
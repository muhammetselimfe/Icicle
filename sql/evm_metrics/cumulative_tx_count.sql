-- Cumulative transaction count metric - total transactions up to each period
-- Parameters: chain_id, first_period, last_period, granularity
-- Strategy: Count transactions per period, then running sum

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
WITH
-- Count transactions per period
txs_per_period AS (
    SELECT
        toStartOf{granularityCamelCase}(block_time) as period,
        count(*) as period_count
    FROM raw_txs
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
    GROUP BY period
),
-- Get the baseline: total transactions before our range
baseline AS (
    SELECT count(*) as prev_cumulative
    FROM raw_txs
    WHERE chain_id = @chain_id
      AND block_time < @first_period
)
-- Running sum of transactions + baseline
SELECT
    {chain_id} as chain_id,
    'cumulative_tx_count' as metric_name,
    '{granularity}' as granularity,
    period,
    (SELECT prev_cumulative FROM baseline) + sum(period_count) OVER (ORDER BY period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
FROM txs_per_period
ORDER BY period;


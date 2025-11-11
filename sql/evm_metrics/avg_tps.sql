-- Average Transactions Per Second metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
WITH period_data AS (
    SELECT
        toStartOf{granularityCamelCase}(block_time) as period,
        count(*) as tx_count
    FROM raw_txs
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
    GROUP BY period
)
SELECT
    {chain_id} as chain_id,
    'avg_tps' as metric_name,
    '{granularity}' as granularity,
    period,
    CAST(tx_count / (toUnixTimestamp(period + INTERVAL 1 {granularity}) - toUnixTimestamp(period)) AS UInt64) as value
FROM period_data
ORDER BY period;

-- Average gas price metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'avg_gas_price' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    CAST(avg(gas_price) AS UInt64) as value
FROM raw_txs
WHERE chain_id = @chain_id
  AND block_time >= @first_period
  AND block_time < @last_period
GROUP BY period
ORDER BY period;

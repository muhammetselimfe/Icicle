-- Contracts created metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'contracts' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    count(*) as value
FROM raw_traces
WHERE chain_id = @chain_id
  AND block_time >= @first_period
  AND block_time < @last_period
  AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
  AND tx_success = true
GROUP BY period
ORDER BY period;
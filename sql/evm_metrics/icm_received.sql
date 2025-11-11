-- ICM (Interchain Messaging) received metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'icm_received' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    count(*) as value
FROM raw_logs
WHERE chain_id = @chain_id
  AND block_time >= @first_period
  AND block_time < @last_period
  AND topic0 = unhex('292ee90bbaf70b5d4936025e09d56ba08f3e421156b6a568cf3c2840d9343e34')
GROUP BY period
ORDER BY period;

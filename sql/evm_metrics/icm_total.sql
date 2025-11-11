-- ICM (Interchain Messaging) total metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'icm_total' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    count(*) as value
FROM raw_logs
WHERE chain_id = @chain_id
  AND block_time >= @first_period
  AND block_time < @last_period
  AND topic0 IN (
    unhex('2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8'), -- SEND
    unhex('292ee90bbaf70b5d4936025e09d56ba08f3e421156b6a568cf3c2840d9343e34')  -- RECEIVE
  )
GROUP BY period
ORDER BY period;

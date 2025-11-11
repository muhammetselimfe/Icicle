-- ICM (Interchain Messaging) sent metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'icm_sent' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    count(*) as value
FROM raw_logs
WHERE chain_id = @chain_id
  AND block_time >= @first_period
  AND block_time < @last_period
  AND topic0 = unhex('2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8')
GROUP BY period
ORDER BY period;
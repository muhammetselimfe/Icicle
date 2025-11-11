-- Maximum Gas Per Second metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
WITH gas_per_second AS (
    SELECT 
        toStartOf{granularityCamelCase}(block_time) as period,
        toStartOfSecond(block_time) as second,
        sum(gas_used) as gas_used
    FROM raw_blocks
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
    GROUP BY period, second
)
SELECT
    {chain_id} as chain_id,
    'max_gps' as metric_name,
    '{granularity}' as granularity,
    period,
    max(gas_used) as value
FROM gas_per_second
GROUP BY period
ORDER BY period;

-- Active addresses metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'active_addresses' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    uniq(address) as value
FROM (
    SELECT from as address, block_time
    FROM raw_traces
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
      AND from != unhex('0000000000000000000000000000000000000000')
    
    UNION ALL
    
    SELECT to as address, block_time
    FROM raw_traces
    WHERE chain_id = @chain_id
      AND block_time >= @first_period
      AND block_time < @last_period
      AND to IS NOT NULL
      AND to != unhex('0000000000000000000000000000000000000000')
)
GROUP BY period
ORDER BY period;
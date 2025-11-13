-- USDC transfer volume metric
-- Parameters: chain_id, first_period, last_period, granularity
-- Tracks total USDC transferred via Transfer events
-- USDC contract: 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e (Avalanche C-Chain)

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,
    'usdc_volume' as metric_name,
    '{granularity}' as granularity,
    toStartOf{granularityCamelCase}(block_time) as period,
    -- Sum USDC amounts (decode from data field, divide by 1e6 for USDC decimals)
    CAST(sum(reinterpretAsUInt256(reverse(data))) / 1000000 AS UInt64) as value
FROM raw_logs
WHERE chain_id = @chain_id
  AND address = unhex('b97ef9ef8734c71904d8002f8b6bc66dd9c48a6e') -- USDC contract address
  AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') -- Transfer event signature
  AND block_time >= @first_period
  AND block_time < @last_period
GROUP BY period
ORDER BY period;


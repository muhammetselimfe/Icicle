-- ERC-20 Balance Tracking - Current Balance Only
-- Stores: chain_id, wallet, token, balance, last_updated_block
-- Uses ReplacingMergeTree for idempotency

CREATE TABLE IF NOT EXISTS erc20_balances (
    chain_id UInt32,
    wallet FixedString(20),
    token FixedString(20),
    balance UInt256,
    last_updated_block UInt32,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, wallet, token);

-- Process block range: update current balances  
-- Start from max(last_updated_block) to ensure idempotency
INSERT INTO erc20_balances (chain_id, wallet, token, balance, last_updated_block)
WITH 
-- Get the max last_updated_block across all wallets/tokens for this chain
max_block AS (
    SELECT coalesce(max(last_updated_block), 0) as start_block
    FROM erc20_balances FINAL
    WHERE chain_id = @chain_id
)
SELECT 
    @chain_id as chain_id,
    agg.wallet,
    agg.token,
    -- Calculate new balance: add incoming, subtract outgoing, clamp to 0
    CAST(
        if(coalesce(old.balance, toUInt256(0)) + agg.total_in >= agg.total_out,
           coalesce(old.balance, toUInt256(0)) + agg.total_in - agg.total_out,
           0),
        'UInt256'
    ) as balance,
    @to_block as last_updated_block
FROM (
    SELECT 
        wallet,
        token,
        sum(if(is_incoming, amount, toUInt256(0))) as total_in,
        sum(if(NOT is_incoming, amount, toUInt256(0))) as total_out
    FROM (
        -- Incoming transfers
        SELECT 
            substring(topic2, 13, 20) as wallet,
            address as token,
            reinterpretAsUInt256(reverse(data)) as amount,
            true as is_incoming
        FROM raw_logs
        CROSS JOIN max_block
        WHERE chain_id = @chain_id
          AND block_number > max_block.start_block
          AND block_number <= @to_block
          AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
          AND length(data) = 32
          AND topic2 IS NOT NULL
          AND substring(topic2, 13, 20) != unhex('0000000000000000000000000000000000000000')
          
        UNION ALL
        
        -- Outgoing transfers  
        SELECT 
            substring(topic1, 13, 20) as wallet,
            address as token,
            reinterpretAsUInt256(reverse(data)) as amount,
            false as is_incoming
        FROM raw_logs
        CROSS JOIN max_block
        WHERE chain_id = @chain_id
          AND block_number > max_block.start_block
          AND block_number <= @to_block
          AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
          AND length(data) = 32
          AND topic1 IS NOT NULL
          AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
    ) transfers
    GROUP BY wallet, token
) agg
LEFT JOIN (
    SELECT wallet, token, balance, last_updated_block
    FROM erc20_balances FINAL
    WHERE chain_id = @chain_id
) old ON agg.wallet = old.wallet AND agg.token = old.token;

-- ===========================================
-- QUERY EXAMPLES
-- ===========================================
-- Get current balance:
-- SELECT balance FROM erc20_balances FINAL
-- WHERE chain_id = ? AND wallet = ? AND token = ?

-- Get all token balances for a wallet:
-- SELECT token, balance
-- FROM erc20_balances FINAL  
-- WHERE chain_id = ? AND wallet = ? 
-- AND balance > 0

-- Get top holders of a token:
-- SELECT wallet, balance
-- FROM erc20_balances FINAL
-- WHERE chain_id = ? AND token = ?
-- ORDER BY balance DESC
-- LIMIT 100

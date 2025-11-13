-- ERC-20 Balance Tracking - Single Block Processing
-- Stores: chain_id, wallet, token, block_number, balance
-- Balance is the wallet's balance AFTER this block executes
-- Uses ReplacingMergeTree for idempotency

CREATE TABLE IF NOT EXISTS erc20_balances (
    chain_id UInt32,
    wallet FixedString(20),
    token FixedString(20),
    block_number UInt32,
    balance UInt256,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (chain_id, wallet, token, block_number);

-- Process a single block: compute deltas and update balances
INSERT INTO erc20_balances (chain_id, wallet, token, block_number, balance)
WITH
-- Compute all deltas for this block
deltas_this_block AS (
    -- Incoming transfers (to address)
    SELECT 
        substring(topic2, 13, 20) as wallet,
        address as token,
        CAST(reinterpretAsUInt256(reverse(data)) AS Int256) as delta
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number = @block_number
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      AND length(data) = 32
      AND topic2 IS NOT NULL
      AND substring(topic2, 13, 20) != unhex('0000000000000000000000000000000000000000')
      
    UNION ALL
    
    -- Outgoing transfers (from address)
    SELECT 
        substring(topic1, 13, 20) as wallet,
        address as token,
        -CAST(reinterpretAsUInt256(reverse(data)) AS Int256) as delta
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number = @block_number
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      AND length(data) = 32
      AND topic1 IS NOT NULL
      AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
),
-- Sum deltas by wallet/token for this block
net_deltas AS (
    SELECT 
        wallet,
        token,
        sum(delta) as net_delta
    FROM deltas_this_block
    GROUP BY wallet, token
),
-- Get previous balance for each affected wallet/token pair
previous_balances AS (
    SELECT 
        nd.wallet,
        nd.token,
        nd.net_delta,
        coalesce(eb.prev_balance, toUInt256(0)) as prev_balance
    FROM net_deltas nd
    LEFT JOIN (
        SELECT 
            wallet,
            token,
            argMax(balance, block_number) as prev_balance
        FROM erc20_balances FINAL
        WHERE chain_id = @chain_id
          AND block_number < @block_number
        GROUP BY wallet, token
    ) eb ON eb.wallet = nd.wallet AND eb.token = nd.token
)
-- Calculate new balances
SELECT 
    {chain_id} as chain_id,
    wallet,
    token,
    @block_number as block_number,
    CAST(prev_balance + net_delta AS UInt256) as balance
FROM previous_balances;

-- ===========================================
-- QUERY EXAMPLES
-- ===========================================
-- Get balance at specific block:
-- SELECT balance FROM erc20_balances FINAL
-- WHERE chain_id = ? AND wallet = ? AND token = ? AND block_number <= ?
-- ORDER BY block_number DESC LIMIT 1

-- Get all token balances for a wallet:
-- SELECT token, argMax(balance, block_number) as balance
-- FROM erc20_balances FINAL  
-- WHERE chain_id = ? AND wallet = ? AND block_number <= ?
-- GROUP BY token
-- HAVING balance > 0

-- ERC-20 Balance Tracking - Two Table Approach
-- Table 1: Deltas (all transfers) - for history and recomputation
-- Table 2: Balance snapshots - for fast queries
-- Both queries are idempotent using ReplacingMergeTree

-- Step 1: Delta table stores every transfer event
CREATE TABLE IF NOT EXISTS erc20_deltas (
    wallet FixedString(20),
    token FixedString(20),
    block_number UInt32,
    transaction_index UInt16,
    log_index UInt32,
    delta Int256,  -- Positive for incoming, negative for outgoing
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (wallet, token, block_number, transaction_index, log_index);

-- Step 2: Balance snapshots - stores balance after each block with activity
CREATE TABLE IF NOT EXISTS erc20_balances (
    wallet FixedString(20),
    token FixedString(20),
    block_number UInt32,
    balance UInt256,  -- Balance AFTER this block
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (wallet, token, block_number);

-- ===========================================
-- STEP 1: Write all deltas (idempotent)
-- ===========================================
INSERT INTO erc20_deltas (wallet, token, block_number, transaction_index, log_index, delta)
SELECT 
    wallet,
    token,
    block_number,
    transaction_index,
    log_index,
    delta
FROM (
    -- Incoming transfers (to address)
    SELECT 
        substring(topic2, 13, 20) as wallet,
        address as token,
        block_number,
        transaction_index,
        log_index,
        CAST(reinterpretAsUInt256(reverse(data)) AS Int256) as delta
    FROM raw_logs
    WHERE chain_id = {chain_id:UInt32}
      AND block_number >= {first_block:UInt64}
      AND block_number <= {last_block:UInt64}
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      AND length(data) = 32
      AND topic2 IS NOT NULL
      AND substring(topic2, 13, 20) != unhex('0000000000000000000000000000000000000000')
      
    UNION ALL
    
    -- Outgoing transfers (from address)
    SELECT 
        substring(topic1, 13, 20) as wallet,
        address as token,
        block_number,
        transaction_index,
        log_index,
        -CAST(reinterpretAsUInt256(reverse(data)) AS Int256) as delta
    FROM raw_logs
    WHERE chain_id = {chain_id:UInt32}
      AND block_number >= {first_block:UInt64}
      AND block_number <= {last_block:UInt64}
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      AND length(data) = 32
      AND topic1 IS NOT NULL
      AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
);

-- ===========================================
-- STEP 2: Compute and write balance snapshots
-- ===========================================
-- This recomputes balances for affected wallets in this batch
-- Idempotent: ReplacingMergeTree will handle duplicate (wallet,token,block) entries
INSERT INTO erc20_balances (wallet, token, block_number, balance)
WITH 
-- Get all unique wallet/token pairs affected in this batch
affected_pairs AS (
    SELECT DISTINCT wallet, token 
    FROM erc20_deltas FINAL
    WHERE block_number >= {first_block:UInt64}
      AND block_number <= {last_block:UInt64}
),
-- Get the starting balance for each affected pair
-- Use argMax to avoid correlated subquery
starting_balances AS (
    SELECT 
        ap.wallet,
        ap.token,
        coalesce(
            argMax(eb.balance, eb.block_number),
            toUInt256(0)
        ) as balance
    FROM affected_pairs ap
    LEFT JOIN (
        SELECT wallet, token, block_number, balance
        FROM erc20_balances FINAL
        WHERE block_number < {first_block:UInt64}
    ) eb ON eb.wallet = ap.wallet AND eb.token = ap.token
    GROUP BY ap.wallet, ap.token
),
-- Compute balance at each block in this batch where there was activity
block_balances AS (
    SELECT 
        d.wallet,
        d.token,
        d.block_number,
        -- Starting balance + all deltas up to this block
        sb.balance + sum(d.delta) OVER (
            PARTITION BY d.wallet, d.token 
            ORDER BY d.block_number, d.transaction_index, d.log_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as running_balance
    FROM (
        -- Get all deltas for affected pairs, summed by block
        SELECT 
            wallet,
            token, 
            block_number,
            0 as transaction_index,  -- Aggregate at block level
            0 as log_index,
            sum(delta) as delta
        FROM erc20_deltas FINAL
        WHERE (wallet, token) IN (SELECT wallet, token FROM affected_pairs)
          AND block_number >= {first_block:UInt64}
          AND block_number <= {last_block:UInt64}
        GROUP BY wallet, token, block_number
    ) d
    JOIN starting_balances sb ON d.wallet = sb.wallet AND d.token = sb.token
)
-- Get the final balance for each block (last tx/log in that block)
SELECT 
    wallet,
    token,
    block_number,
    max(running_balance) as balance  -- Last balance in the block
FROM block_balances
GROUP BY wallet, token, block_number;

-- ===========================================
-- QUERY EXAMPLES
-- ===========================================
-- Get balance at specific block:
-- SELECT balance FROM erc20_balances FINAL
-- WHERE wallet = ? AND token = ? AND block_number <= ?
-- ORDER BY block_number DESC LIMIT 1

-- Get all token balances for a wallet:
-- SELECT token, argMax(balance, block_number) as balance
-- FROM erc20_balances FINAL  
-- WHERE wallet = ? AND block_number <= ?
-- GROUP BY token
-- HAVING balance > 0

-- Get transfer history:
-- SELECT block_number, transaction_index, log_index, delta
-- FROM erc20_deltas FINAL
-- WHERE wallet = ? AND token = ?
-- ORDER BY block_number DESC, transaction_index DESC, log_index DESC
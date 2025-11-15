-- ERC-20 Balance Tracking - 2 Stage Process
-- Stage 1: erc20_balance_changes table (stores diffs per block range) 
-- Stage 2: erc20_balances view (aggregates all diffs)

-- ========================================================================
-- STAGE 1: CREATE BALANCE CHANGES TABLE
-- ========================================================================
-- ORDER BY is optimized for token-centric queries (e.g. "all holders of token X")
-- If you mainly query by wallet instead, swap token and wallet in ORDER BY

CREATE TABLE IF NOT EXISTS erc20_balance_changes (
    chain_id UInt32,
    wallet FixedString(20),
    token FixedString(20),
    from_block UInt32,  -- start of processed range
    to_block UInt32,    -- end of processed range
    deposits UInt256,   -- total incoming in this range
    withdrawals UInt256, -- total outgoing in this range
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, token, wallet, from_block, to_block);

-- ========================================================================
-- STAGE 2: CREATE BALANCE VIEW
-- ========================================================================

CREATE OR REPLACE VIEW erc20_balances AS
SELECT 
    chain_id,
    wallet,
    token,
    sum(deposits) as total_in,
    sum(withdrawals) as total_out,
    sum(deposits) - sum(withdrawals) as balance,
    max(to_block) as last_updated_block
FROM erc20_balance_changes FINAL
GROUP BY chain_id, wallet, token;

-- ========================================================================
-- INSERT: Process balance changes for block range
-- ========================================================================
-- Run this with parameters: @chain_id, @from_block, @to_block
-- If same range is retried, ReplacingMergeTree keeps the latest version

INSERT INTO erc20_balance_changes (chain_id, wallet, token, from_block, to_block, deposits, withdrawals)
SELECT 
    @chain_id as chain_id,
    wallet,
    token,
    @from_block as from_block,
    @to_block as to_block,
    sum(if(is_incoming, amount, toUInt256(0))) as deposits,
    sum(if(NOT is_incoming, amount, toUInt256(0))) as withdrawals
FROM (
    -- ========================================================================
    -- INCOMING BALANCE CHANGES (DEPOSITS)
    -- ========================================================================
    
    -- Standard ERC20 incoming transfers (including mints from 0x0)
    SELECT 
        substring(topic2, 13, 20) as wallet,
        address as token,
        reinterpretAsUInt256(reverse(data)) as amount,
        true as is_incoming
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number >= @from_block
      AND block_number <= @to_block
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')  -- Transfer
      AND length(data) = 32
      AND topic2 IS NOT NULL
      
    UNION ALL
    
    -- Wrapped token deposits (WAVAX, WETH, etc)
    SELECT 
        substring(topic1, 13, 20) as wallet,
        address as token,
        reinterpretAsUInt256(reverse(data)) as amount,
        true as is_incoming
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number >= @from_block
      AND block_number <= @to_block
      AND topic0 = unhex('e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c')  -- Deposit
      AND length(data) = 32
      AND topic1 IS NOT NULL
      
    UNION ALL
    
    -- ========================================================================
    -- OUTGOING BALANCE CHANGES (WITHDRAWALS)
    -- ========================================================================
    
    -- Standard ERC20 outgoing transfers (including burns to 0x0)
    SELECT 
        substring(topic1, 13, 20) as wallet,
        address as token,
        reinterpretAsUInt256(reverse(data)) as amount,
        false as is_incoming
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number >= @from_block
      AND block_number <= @to_block
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')  -- Transfer
      AND length(data) = 32
      AND topic1 IS NOT NULL
      
    UNION ALL
    
    -- Wrapped token withdrawals (WAVAX, WETH, etc)
    SELECT 
        substring(topic1, 13, 20) as wallet,
        address as token,
        reinterpretAsUInt256(reverse(data)) as amount,
        false as is_incoming
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number >= @from_block
      AND block_number <= @to_block
      AND topic0 = unhex('7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65')  -- Withdrawal
      AND length(data) = 32
      AND topic1 IS NOT NULL
) transfers
WHERE wallet != unhex('0000000000000000000000000000000000000000')  -- Skip zero address as wallet
GROUP BY wallet, token
HAVING deposits > 0 OR withdrawals > 0;  -- Only insert if there were changes

-- ========================================================================
-- DEBUG QUERIES (commented out - uncomment to use)
-- ========================================================================

-- 1. Check processed block ranges for a chain:
-- SELECT from_block, to_block, to_block - from_block + 1 as blocks_in_range,
--        count(DISTINCT (wallet, token)) as wallet_token_pairs,
--        sum(deposits) as total_deposits, sum(withdrawals) as total_withdrawals
-- FROM erc20_balance_changes FINAL
-- WHERE chain_id = 43114
-- GROUP BY from_block, to_block ORDER BY from_block LIMIT 100;

-- 2. Find gaps in block coverage:
-- WITH ranges AS (
--     SELECT DISTINCT from_block, to_block, 
--            lead(from_block) OVER (ORDER BY from_block) as next_start
--     FROM erc20_balance_changes FINAL WHERE chain_id = 43114
-- )
-- SELECT current_end, next_start, next_start - current_end - 1 as gap_size
-- FROM (SELECT to_block as current_end, next_start FROM ranges)
-- WHERE next_start > current_end + 1;

-- 3. Check specific wallet/token history:
-- SELECT from_block, to_block, deposits, withdrawals, deposits - withdrawals as net_change
-- FROM erc20_balance_changes FINAL
-- WHERE chain_id = 43114
--   AND wallet = unhex('...')
--   AND token = unhex('...')
-- ORDER BY from_block;

-- 4. Find tokens with most negative balances:
-- SELECT lower(hex(token)) as token_addr, count(DISTINCT wallet) as negative_holders
-- FROM erc20_balances WHERE chain_id = 43114 AND balance < 0
-- GROUP BY token ORDER BY negative_holders DESC LIMIT 20;

-- 5. Compare manual vs view calculation:
-- WITH manual AS (
--     SELECT sum(deposits) as d, sum(withdrawals) as w, sum(deposits) - sum(withdrawals) as bal
--     FROM erc20_balance_changes FINAL
--     WHERE chain_id = 43114 AND wallet = unhex('...') AND token = unhex('...')
-- ), view_data AS (
--     SELECT total_in, total_out, balance FROM erc20_balances
--     WHERE chain_id = 43114 AND wallet = unhex('...') AND token = unhex('...')
-- )
-- SELECT m.d, v.total_in, m.d = v.total_in as match FROM manual m, view_data v;

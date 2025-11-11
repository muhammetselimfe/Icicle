-- Address to Chain mapping (incremental index)
-- Parameters: chain_id, first_block, last_block
--
-- Stores unique (address, chain_id) pairs for cross-chain address tracking.
-- Primary query: "SELECT DISTINCT chain_id FROM address_on_chain FINAL WHERE address = ?"
-- 
-- This is an incremental index, not a metric - no granularity.
-- Each run blindly inserts address-chain pairs; ReplacingMergeTree deduplicates on merge.
-- Optimized for fast incremental writes, acceptable read overhead with FINAL.

CREATE TABLE IF NOT EXISTS address_on_chain (
    address FixedString(20),
    chain_id UInt32,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (address, chain_id)
SETTINGS index_granularity = 1024;

-- Insert address-chain pairs from traces
-- No dedup check - ReplacingMergeTree handles it via background merges
INSERT INTO address_on_chain (address, chain_id)
SELECT DISTINCT
    address,
    {chain_id:UInt32} as chain_id
FROM (
    SELECT from as address
    FROM raw_traces
    WHERE chain_id = {chain_id:UInt32}
      AND block_number >= {first_block:UInt64}
      AND block_number <= {last_block:UInt64}
      AND from != unhex('0000000000000000000000000000000000000000')
    
    UNION ALL
    
    SELECT to as address
    FROM raw_traces
    WHERE chain_id = {chain_id:UInt32}
      AND block_number >= {first_block:UInt64}
      AND block_number <= {last_block:UInt64}
      AND to IS NOT NULL
      AND to != unhex('0000000000000000000000000000000000000000')
)
WHERE address IS NOT NULL;


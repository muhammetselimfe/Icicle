-- Unified watermark table for tracking indexer progress
CREATE TABLE IF NOT EXISTS indexer_watermarks (
    chain_id UInt32,
    indexer_name String,  -- Full path: e.g., "metrics/active_addresses_hour", "incremental/batched/address_on_chain"
    
    -- For granular metrics (time-based)
    last_period DateTime64(3, 'UTC'),
    
    -- For incremental indexers (block-based)
    last_block_num UInt64,
    
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, indexer_name);

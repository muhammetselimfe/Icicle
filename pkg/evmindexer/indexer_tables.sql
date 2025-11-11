-- Single unified metrics table for all metric types and granularities
CREATE TABLE IF NOT EXISTS metrics (
    chain_id UInt32,
    metric_name LowCardinality(String),  -- e.g., "active_addresses", "cumulative_addresses", "contracts", "cumulative_contracts", "icm_sent"
    granularity LowCardinality(String),  -- e.g., "hour", "day", "week", "month"
    period DateTime64(3, 'UTC'),         -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, metric_name, granularity, period)
PARTITION BY (chain_id, toYYYYMM(period));

-- Unified watermark table for tracking indexer progress
CREATE TABLE IF NOT EXISTS indexer_watermarks (
    chain_id UInt32,
    indexer_name String,  -- e.g., "evm_metrics/active_addresses", "evm_incremental/batched/address_on_chain"
    granularity LowCardinality(String),  -- For metrics: "hour", "day", etc. Empty for incrementals
    
    -- For granular metrics (time-based)
    last_period DateTime64(3, 'UTC'),
    
    -- For incremental indexers (block-based)
    last_block_num UInt64,
    
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, indexer_name, granularity);

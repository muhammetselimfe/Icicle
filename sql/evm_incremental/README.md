# Incremental Indexers

This directory contains block-based incremental indexers that process blockchain data continuously as new blocks arrive. Unlike granular metrics (which are time-based), incremental indexers use block numbers for watermarking.

## Two Types of Incremental Indexers

### 1. Batched (`batched/`)
- **Throttle**: Runs maximum once per 5 minutes (wall time)
- **Use case**: Heavy indexing operations that don't need real-time updates
- **Examples**: Cross-chain address tracking, contract metadata extraction

### 2. Immediate (`immediate/`)
- **Throttle**: Runs every batch with minimum 0.9 seconds spacing
- **Use case**: Near real-time indexing that needs to be current
- **Examples**: Live leaderboards, active user tracking

## Template Placeholders

The indexer runner (`pkg/indexer/incremental.go`) replaces these placeholders:

| Placeholder | Description | Example Replacement |
|------------|-------------|---------------------|
| `{chain_id:UInt32}` | Blockchain chain ID | `43114` |
| `{first_block:UInt64}` | First block number to process (inclusive) | `7563601` |
| `{last_block:UInt64}` | Last block number to process (inclusive) | `7564000` |

**Note**: Unlike granular metrics which use `block_time >= X AND block_time < Y`, incremental indexers use **inclusive ranges**: `block_number >= X AND block_number <= Y`

## Standard Structure

```sql
-- Create table (idempotent)
CREATE TABLE IF NOT EXISTS indexer_name (
    -- your columns here
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (your_primary_key);

-- Insert data (idempotent - ReplacingMergeTree handles duplicates)
INSERT INTO indexer_name (columns...)
SELECT DISTINCT
    -- your data
FROM raw_traces
WHERE chain_id = {chain_id:UInt32}
  AND block_number >= {first_block:UInt64}
  AND block_number <= {last_block:UInt64}
  -- additional filters
```

## Key Design Principles

### 1. Block Number Based
- Use `block_number` for filtering, not `block_time`
- Block numbers are sequential and never conflict (unlike block_time where multiple blocks can have same timestamp)
- Watermarks track `last_block_num` instead of `last_period`

### 2. Idempotent by Design
- ReplacingMergeTree handles duplicate inserts
- Safe to re-run on same block range
- On failure, watermark doesn't update, so blocks get reprocessed

### 3. Inclusive Range
- Both `first_block` and `last_block` are **inclusive**: `[first_block, last_block]`
- Different from granular metrics which use half-open intervals

### 4. No Granularity
- Single table per indexer (no hour/day/week/month variants)
- Tables grow continuously with new data

## Watermarks

Incremental indexers use block-based watermarks:

```sql
SELECT indexer_name, last_block_num 
FROM indexer_watermarks FINAL
WHERE chain_id = 43114
  AND indexer_name LIKE 'incremental/%'
```

Watermark pattern:
- Batched: `incremental/batched/indexer_name`
- Immediate: `incremental/immediate/indexer_name`

## Example: Cross-Chain Address Tracking

See `batched/address_on_chain.sql`:

```sql
CREATE TABLE IF NOT EXISTS address_on_chain (
    address FixedString(20),
    chain_id UInt32,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (address, chain_id);

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
```

## Adding New Incremental Indexers

1. Choose type: batched (5min) or immediate (0.9s)
2. Create `indexer_name.sql` in appropriate directory
3. Use `{chain_id:UInt32}`, `{first_block:UInt64}`, `{last_block:UInt64}` placeholders
4. Filter by `block_number >= {first_block:UInt64} AND block_number <= {last_block:UInt64}`
5. Use ReplacingMergeTree for idempotency
6. Restart indexer runner - auto-discovers new files

## Batched vs Immediate: When to Use

**Use Batched (5min throttle) when:**
- Indexing is computationally expensive
- Real-time updates aren't critical
- Building large lookup tables
- Cross-referencing multiple chains

**Use Immediate (0.9s throttle) when:**
- Near real-time data is needed
- Indexing is lightweight
- Powering live dashboards
- Tracking active users/contracts

## Querying Incremental Indexers

Always use `FINAL` to get deduplicated results:

```sql
-- Query address chains
SELECT DISTINCT chain_id 
FROM address_on_chain FINAL
WHERE address = unhex('742d35Cc6634C0532925a3b844Bc9e7595f0bEb');

-- Check watermark progress
SELECT last_block_num 
FROM indexer_watermarks FINAL
WHERE chain_id = 43114 
  AND indexer_name = 'incremental/batched/address_on_chain';
```

## Important Notes

- Incremental indexers start from block 1 on first run
- Use `block_number` for filtering, not `block_time`
- Ranges are **inclusive** on both ends: `[first_block, last_block]`
- ReplacingMergeTree handles duplicate inserts automatically
- Watermarks are block numbers, not timestamps
- New indexers are auto-discovered on startup
- Each chain processes independently with its own IndexRunner instance


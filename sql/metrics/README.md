# Granular Metrics SQL Query Templates

This directory contains SQL templates for computing time-based blockchain metrics at various granularities (hour/day/week/month). Most metrics are regular (per-period) only, while a few metrics also track cumulative (running total) values.

These are **granular metrics** - one of three types of indexers in the system. See also: `sql/incremental/batched/` and `sql/incremental/immediate/`.

## Supported Granularities

- **hour** - 3600-second periods  
- **day** - 86400-second periods (UTC day boundaries)
- **week** - 604800-second periods (Sunday start)
- **month** - Variable duration (actual calendar months: 28-31 days)

## Template Placeholders

The indexer runner (`pkg/indexer/granular.go`) replaces these placeholders when executing queries:

| Placeholder | Description | Example Replacement |
|------------|-------------|---------------------|
| `{chain_id:UInt32}` | Blockchain chain ID | `43114` |
| `{first_period:DateTime}` | Start of period range (inclusive) | `toDateTime64('2024-01-01 00:00:00.000', 3)` |
| `{last_period:DateTime}` | End of period range (exclusive) | `toDateTime64('2024-01-02 00:00:00.000', 3)` |
| `{granularity}` | Time granularity | `hour` |
| `toStartOf{granularity}` | ClickHouse function | `toStartOfHour` |
| `_{granularity}` | Table name suffix | `_hour` |

Note: `period_seconds` parameter is NOT used. For average metrics (TPS/GPS), period duration is calculated dynamically in SQL using `toUnixTimestamp(period + INTERVAL 1 {granularity}) - toUnixTimestamp(period)` to handle variable-length months correctly.

# File Structure

Each metric file contains:
1. Regular metric table definition and INSERT (all metrics)
2. Cumulative metric table definition and INSERT (only for: tx_count, addresses, contracts, deployers)

### Regular Metrics
Track values per period (e.g., transactions in an hour, gas used in a day). All metrics have regular versions.

### Cumulative Metrics
Track running totals over time (e.g., total transactions ever, total unique addresses ever). Only available for metrics where cumulative totals are meaningful: tx_count, addresses, contracts, and deployers. Built by:
1. Getting the last cumulative value before the current range
2. Reading from the regular metric table (with FINAL)
3. Calculating running sum with window functions

## Standard Table Structure

```sql
-- Regular metric table
CREATE TABLE IF NOT EXISTS metric_name_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,                 -- Or UInt256 for large values
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Regular metric insert
INSERT INTO metric_name_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    count(*) as value  -- or appropriate aggregation
FROM raw_transactions  -- or raw_traces, raw_logs, raw_blocks
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
GROUP BY period
ORDER BY period;

-- Cumulative metric table
CREATE TABLE IF NOT EXISTS cumulative_metric_name_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Cumulative metric insert (uses regular table)
INSERT INTO cumulative_metric_name_{granularity} (chain_id, period, value)
WITH 
-- Get the last cumulative value before our range
previous_cumulative AS (
    SELECT max(value) as prev_value
    FROM cumulative_metric_name_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period < {first_period:DateTime}
),
-- Get counts from regular table for our period range (use FINAL to get deduplicated values)
period_counts AS (
    SELECT 
        period,
        value as period_count
    FROM metric_name_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period >= {first_period:DateTime}
      AND period < {last_period:DateTime}
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    -- Add previous cumulative value to our running sum
    ifNull((SELECT prev_value FROM previous_cumulative), 0) + 
    sum(period_count) OVER (ORDER BY period) as value
FROM period_counts
ORDER BY period;
```

## Key Design Principles

### 1. DateTime64(3, 'UTC') for All Timestamps
- Millisecond precision with explicit UTC timezone
- Use `now64(3)` for computed_at defaults
- Consistent across all metric tables

### 2. Idempotent Processing
- COUNT ALL transactions in each period, not just new arrivals
- Same query always produces same result regardless of when run
- Enables safe reprocessing and backfilling

### 3. ReplacingMergeTree with computed_at
- Latest computation always wins
- Natural deduplication on (chain_id, period)
- Use FINAL keyword when reading for queries

### 4. Cumulative Metrics Build on Regular Metrics
- Regular metrics are source of truth
- Cumulative queries read from regular tables with FINAL
- Allows recomputation of cumulative without touching raw data
- Window functions (OVER ORDER BY) calculate running totals

### 5. Time Range Filtering
```sql
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}  -- Inclusive
  AND block_time < {last_period:DateTime}    -- Exclusive [first, last)
```

## Metric Categories

### Transaction Count Metrics
- **tx_count** - Transaction count per period (regular + cumulative)
- **avg_tps** - Average transactions per second (regular only)
- **max_tps** - Maximum TPS within period (regular only)

### Gas Metrics  
- **gas_used** - Total gas consumed (regular only)
- **avg_gps** - Average gas per second (regular only)
- **max_gps** - Maximum GPS within period (regular only)
- **avg_gas_price** - Average gas price (regular only)
- **max_gas_price** - Maximum gas price (regular only)
- **fees_paid** - Total fees paid, UInt256 (regular only)

### Address Metrics
- **active_addresses** - Unique addresses active in period (from + to) (regular + cumulative)
- **active_senders** - Unique transaction senders (regular only)
- **deployers** - Unique contract deployers (regular + cumulative)
- **contracts** - Contracts deployed in period (regular + cumulative)

### ICM (Interchain Messaging) Metrics
- **icm_total** - Total ICM messages (sent + received) (regular only)
- **icm_sent** - ICM messages sent (regular only)
- **icm_received** - ICM messages received (regular only)

Note: ICM events are detected by specific topic0 hashes:
- Send: `unhex('2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8')`
- Receive: `unhex('292ee90bbaf70b5d4936025e09d56ba08f3e421156b6a568cf3c2840d9343e34')`

## Special Cases

### Unique Entity Cumulative Metrics
For metrics tracking unique entities that can appear multiple times (addresses, deployers), cumulative calculation differs:

1. Find entities that existed BEFORE the period range
2. Find NEW entities that first appear IN the period range
3. Count new entities per period
4. Add previous cumulative + running sum of new entities

This ensures cumulative counts grow correctly even with reprocessing.

See: `active_addresses.sql`, `deployers.sql`

Note: Contracts use simple cumulative (sum of period counts) since each contract can only be created once.

### Average Rate Metrics (TPS, GPS)
For time-based averages, calculate period duration dynamically:

```sql
WITH period_data AS (
    SELECT
        toStartOf{granularity}(block_time) as period,
        count(*) as tx_count
    FROM raw_transactions
    WHERE chain_id = {chain_id:UInt32}
      AND block_time >= {first_period:DateTime}
      AND block_time < {last_period:DateTime}
    GROUP BY period
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    CAST(tx_count / (toUnixTimestamp(period + INTERVAL 1 {granularity}) - toUnixTimestamp(period)) AS UInt64) as value
FROM period_data
ORDER BY period;
```

This handles variable month lengths correctly (28-31 days) as well as all other granularities.

## Indexer Runner Behavior

The indexer runner (`pkg/indexer/runner.go`) runs one instance per chain:

1. Tracks watermarks per (chain_id, indexer_name) in memory, backed by DB
2. Receives block updates via `OnBlock(blockNum, blockTime)` calls
3. Runs a 200ms loop checking all indexers
4. For granular metrics: calculates complete periods using period boundary functions
5. Executes metric SQL for all complete periods in batch
6. Updates watermark after successful execution
7. Processes all 4 granularities (hour/day/week/month) for each metric file

Watermarks are stored in:
```sql
CREATE TABLE IF NOT EXISTS indexer_watermarks (
    chain_id UInt32,
    indexer_name String,        -- e.g., "metrics/tx_count_hour", "incremental/batched/address_on_chain"
    last_period DateTime64(3, 'UTC'),      -- For granular metrics
    last_block_num UInt64,                 -- For incremental indexers
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, indexer_name)
```

## Adding New Granular Metrics

1. Create `metric_name.sql` in this directory (`sql/metrics/`)
2. Follow the standard structure (regular + cumulative if applicable)
3. Use all required placeholders correctly
4. Test with multiple granularities
5. Verify idempotency (running twice produces same result)
6. Restart the indexer - it auto-discovers new SQL files on startup

For incremental (non-time-based) indexers, see `sql/incremental/batched/` and `sql/incremental/immediate/`

### Example: Simple Count Metric

```sql
-- Widget count metrics (regular and cumulative)
-- Parameters: chain_id, first_period, last_period, granularity

-- Regular widget count table
CREATE TABLE IF NOT EXISTS widgets_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

INSERT INTO widgets_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    count(*) as value
FROM raw_widgets
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
GROUP BY period
ORDER BY period;

-- Cumulative widget count table
CREATE TABLE IF NOT EXISTS cumulative_widgets_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

INSERT INTO cumulative_widgets_{granularity} (chain_id, period, value)
WITH 
previous_cumulative AS (
    SELECT max(value) as prev_value
    FROM cumulative_widgets_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period < {first_period:DateTime}
),
period_counts AS (
    SELECT 
        period,
        value as period_count
    FROM widgets_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period >= {first_period:DateTime}
      AND period < {last_period:DateTime}
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    ifNull((SELECT prev_value FROM previous_cumulative), 0) + 
    sum(period_count) OVER (ORDER BY period) as value
FROM period_counts
ORDER BY period;
```

## Querying Metrics

Always use FINAL when querying metric tables to get deduplicated results:

```sql
-- Get latest tx_count values
SELECT period, value
FROM tx_count_hour FINAL
WHERE chain_id = 43114
ORDER BY period DESC
LIMIT 10;

-- Get cumulative growth
SELECT period, value
FROM cumulative_tx_count_hour FINAL
WHERE chain_id = 43114
  AND period >= '2024-01-01'
ORDER BY period;
```

## Important Notes

- All timestamps use DateTime64(3, 'UTC') with millisecond precision
- All metric files must work with all 4 granularities (hour/day/week/month)
- Only 4 metrics have cumulative versions: tx_count, addresses, contracts, deployers
- Cumulative metrics always read from regular metrics, never from raw tables
- Use FINAL keyword when reading from metric tables in queries or CTEs
- Period ranges are half-open intervals: [first_period, last_period)
- ReplacingMergeTree allows safe reprocessing and backfilling
- The system auto-creates tables: `metric_name_hour`, `metric_name_day`, etc.

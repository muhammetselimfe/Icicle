-- Blocks table - main block headers
CREATE TABLE IF NOT EXISTS raw_blocks (
    chain_id UInt32,  -- Multiple chains in same tables
    block_number UInt32,
    hash FixedString(32),  -- 32 bytes
    parent_hash FixedString(32),
    block_time DateTime64(3, 'UTC'),  -- Millisecond precision, UTC timezone
    miner FixedString(20),  -- 20 bytes address
    difficulty UInt8,  -- Always 1 on PoS chains
    total_difficulty UInt64,  -- On PoS chains, equals block number, but store for compatibility
    size UInt32,
    gas_limit UInt32,
    gas_used UInt32,
    base_fee_per_gas UInt64,
    block_gas_cost UInt64,
    state_root FixedString(32),
    transactions_root FixedString(32),
    receipts_root FixedString(32),
    extra_data String,
    block_extra_data String,
    ext_data_hash FixedString(32),
    ext_data_gas_used UInt32,
    mix_hash FixedString(32),
    nonce LowCardinality(FixedString(8)),  -- 8 bytes, always 0x00...00 on PoS
    sha3_uncles FixedString(32),
    uncles Array(FixedString(32)),
    blob_gas_used UInt32,  -- Always 0 if no blob txs
    excess_blob_gas UInt64,  -- Always 0 if no blob txs
    parent_beacon_block_root LowCardinality(FixedString(32)),  -- Often all zeros
    min_delay_excess UInt64
) ENGINE = MergeTree()
ORDER BY (chain_id, block_number);

-- Transactions table - merged with receipts for analytics performance
CREATE TABLE IF NOT EXISTS raw_txs (
    chain_id UInt32,  -- Multiple chains in same tables
    hash FixedString(32),
    block_number UInt32,
    block_hash FixedString(32),
    block_time DateTime64(3, 'UTC'),  -- Millisecond precision, UTC timezone
    transaction_index UInt16,
    nonce UInt64,
    from FixedString(20),
    to Nullable(FixedString(20)),  -- NULL for contract creation
    value UInt256,
    gas_limit UInt32,  -- Renamed from 'gas' for clarity
    gas_price UInt64,
    gas_used UInt32,  -- From receipt
    success Bool,  -- From receipt status
    input String,  -- Calldata
    type UInt8,  -- 0,1,2,3 (legacy, EIP-2930, EIP-1559, EIP-4844)
    max_fee_per_gas Nullable(UInt64),  -- Only for EIP-1559
    max_priority_fee_per_gas Nullable(UInt64),  -- Only for EIP-1559
    priority_fee_per_gas Nullable(UInt64),  -- Computed: min(gas_price - base_fee, max_priority_fee)
    base_fee_per_gas UInt64,  -- Denormalized from blocks for easier queries
    contract_address Nullable(FixedString(20)),  -- From receipt if contract creation
    access_list Array(Tuple(
        address FixedString(20),
        storage_keys Array(FixedString(32))
    ))  -- Properly structured, not JSON
) ENGINE = MergeTree()
ORDER BY (chain_id, block_number);

-- Traces table - flattened trace calls
CREATE TABLE IF NOT EXISTS raw_traces (
    chain_id UInt32,  -- Multiple chains in same tables
    tx_hash FixedString(32),
    block_number UInt32,
    block_time DateTime64(3, 'UTC'),  -- Millisecond precision, UTC timezone
    transaction_index UInt16,
    trace_address Array(UInt16),  -- Path in call tree, e.g. [0,2,1] = first call -> third subcall -> second subcall
    from FixedString(20),
    to Nullable(FixedString(20)),  -- NULL for certain call types
    gas UInt32,
    gas_used UInt32,
    value UInt256,
    input String,
    output String,
    call_type LowCardinality(String),  -- CALL, DELEGATECALL, STATICCALL, CREATE, CREATE2, etc.
    tx_success Bool,  -- Transaction success status (denormalized from raw_txs)
    tx_from FixedString(20),  -- Original transaction sender (denormalized)
    tx_to Nullable(FixedString(20))  -- Original transaction target (denormalized)
) ENGINE = MergeTree()
ORDER BY (chain_id, block_number);

-- Logs table - event logs emitted by smart contracts
CREATE TABLE IF NOT EXISTS raw_logs (
    chain_id UInt32,  -- Multiple chains in same tables
    address FixedString(20),
    block_number UInt32,
    block_hash FixedString(32),  -- Needed for reorg detection and data integrity
    block_time DateTime64(3, 'UTC'),  -- Millisecond precision, UTC timezone
    transaction_hash FixedString(32),
    transaction_index UInt16,
    log_index UInt32,
    tx_from FixedString(20),  -- Denormalized from transactions for faster queries
    tx_to Nullable(FixedString(20)),  -- Denormalized from transactions
    topic0 FixedString(32),  -- Event signature hash (empty for rare anonymous events)
    topic1 Nullable(FixedString(32)),
    topic2 Nullable(FixedString(32)),
    topic3 Nullable(FixedString(32)),
    data String,  -- Non-indexed event data
    removed Bool  -- TODO: check if ever happen to be true
) ENGINE = MergeTree()
ORDER BY (chain_id, block_time, address, topic0);

-- Watermark table - tracks guaranteed sync progress per chain
CREATE TABLE IF NOT EXISTS sync_watermark (
    chain_id UInt32,
    block_number UInt32
) ENGINE = EmbeddedRocksDB
PRIMARY KEY chain_id;

-- Chain status table - tracks chain metadata and RPC connectivity
CREATE TABLE IF NOT EXISTS chain_status (
    chain_id UInt32,
    name String,
    last_updated DateTime64(3, 'UTC'),
    last_block_on_chain UInt64
) ENGINE = ReplacingMergeTree(last_updated)
PRIMARY KEY chain_id;

-- P-chain transactions table - simplified schema using ClickHouse JSON type
CREATE TABLE IF NOT EXISTS p_chain_txs (
    -- Core indexed columns for efficient queries
    tx_id FixedString(32),
    tx_type LowCardinality(String),
    block_number UInt64,
    block_time DateTime64(3, 'UTC'),
    p_chain_id UInt32,  -- Identifies which P-chain instance (e.g., mainnet vs testnet)
    
    -- Main JSON column storing the complete transaction data
    -- Type hints optimize storage and query performance for frequently accessed fields
    tx_data JSON(
        max_dynamic_paths=512,
        max_dynamic_types=32,
        
        -- Common validator/subnet fields with type hints
        -- Validator.NodeID String,
        -- Validator.Start UInt64,
        -- Validator.End UInt64,
        -- Validator.Wght UInt64,
        -- Subnet String,
        -- SubnetID String,
        -- ChainID String,
        
        -- L1 validator fields
        -- ValidationID String,
        -- Balance UInt64,
        
        -- SubnetValidator fields
        -- SubnetValidator.Subnet String,
        -- SubnetValidator.NodeID String,
        
        -- Other commonly queried fields
        -- TxID String,
        -- AssetID String,
        -- Owner String,
        -- Address String,
        -- ChainName String,
        -- VMID String,
        -- SourceChain String,
        -- DestinationChain String
    )
) ENGINE = MergeTree()
ORDER BY (p_chain_id, block_time, block_number, tx_type);

-- L1 Validator State table - tracks current state of L1 validators
CREATE TABLE IF NOT EXISTS l1_validator_state (
    -- Identifiers
    subnet_id FixedString(32),  -- The L1 subnet ID
    validation_id FixedString(32),  -- Unique validator ID for L1 validators
    node_id String,  -- Node ID in format "NodeID-xxx"

    -- Validator state
    balance UInt64,  -- Remaining balance for this validator
    weight UInt64,  -- Current validator weight/stake
    start_time DateTime64(3, 'UTC'),  -- When validator started
    end_time DateTime64(3, 'UTC'),  -- When validator ends
    uptime_percentage Float64,  -- Uptime percentage (0-100)

    -- Status
    active Bool,  -- Whether validator is currently active

    -- Metadata
    last_updated DateTime64(3, 'UTC'),  -- When this state was last updated
    p_chain_id UInt32  -- Which P-chain instance (mainnet vs testnet)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (p_chain_id, subnet_id, validation_id);

-- L1 Subnets table - tracks which subnets are L1 and should be monitored
CREATE TABLE IF NOT EXISTS l1_subnets (
    subnet_id FixedString(32),  -- The L1 subnet ID
    chain_id FixedString(32),  -- The associated chain ID
    conversion_block UInt64,  -- Block number when subnet was converted to L1
    conversion_time DateTime64(3, 'UTC'),  -- When subnet was converted to L1
    p_chain_id UInt32,  -- Which P-chain instance
    last_synced DateTime64(3, 'UTC')  -- Last time validators were synced for this subnet
) ENGINE = ReplacingMergeTree(last_synced)
PRIMARY KEY (p_chain_id, subnet_id);


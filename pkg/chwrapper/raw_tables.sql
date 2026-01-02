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
    tx_id String,  -- CB58-encoded transaction ID (e.g., "22FdhKfCTTWTfgBWiibGo8x2pEaCeWLdwHwwDCvK9M7eyxxNeV")
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
) ENGINE = ReplacingMergeTree(block_time)
ORDER BY (p_chain_id, tx_id);
-- Note: Using ReplacingMergeTree to deduplicate transactions that may be inserted multiple times
-- during syncer restarts. ORDER BY tx_id ensures uniqueness per transaction.
-- IMPORTANT: For existing tables, use FINAL or DISTINCT in queries to get deduplicated results.
-- Migration note: If migrating from MergeTree, recreate table and re-sync data.

-- L1 Validator State table - tracks current state of L1 validators
CREATE TABLE IF NOT EXISTS l1_validator_state (
    -- Identifiers
    subnet_id String,  -- The L1 subnet ID (CB58)
    validation_id String,  -- Unique validator ID for L1 validators (CB58)
    node_id String,  -- Node ID in format "NodeID-xxx"

    -- Validator state
    balance UInt64,  -- Remaining balance for this validator
    weight UInt64,  -- Current validator weight/stake
    start_time DateTime64(3, 'UTC'),  -- When validator started
    end_time DateTime64(3, 'UTC'),  -- When validator ends
    uptime_percentage Float64,  -- Uptime percentage (0-100)

    -- Status
    active Bool,  -- Whether validator is currently active

    -- Fee tracking (computed from balance transactions)
    initial_deposit UInt64 DEFAULT 0,  -- Initial balance at creation (in nAVAX)
    total_topups UInt64 DEFAULT 0,  -- Sum of all top-up transactions (in nAVAX)
    refund_amount UInt64 DEFAULT 0,  -- Refund when disabled (in nAVAX)
    fees_paid UInt64 DEFAULT 0,  -- Total fees consumed (deposited - refunded - balance)

    -- Metadata
    last_updated DateTime64(3, 'UTC'),  -- When this state was last updated
    p_chain_id UInt32  -- Which P-chain instance (mainnet vs testnet)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (p_chain_id, subnet_id, validation_id);

-- L1 Subnets table - tracks which subnets are L1 and should be monitored
CREATE TABLE IF NOT EXISTS l1_subnets (
    subnet_id String,  -- The L1 subnet ID (CB58)
    chain_id String,  -- The associated chain ID (CB58)
    conversion_block UInt64,  -- Block number when subnet was converted to L1
    conversion_time DateTime64(3, 'UTC'),  -- When subnet was converted to L1
    p_chain_id UInt32,  -- Which P-chain instance
    last_synced DateTime64(3, 'UTC')  -- Last time validators were synced for this subnet
) ENGINE = ReplacingMergeTree(last_synced)
PRIMARY KEY (p_chain_id, subnet_id);

-- L1 Registry table - metadata from external registry (L1Beat)
CREATE TABLE IF NOT EXISTS l1_registry (
    subnet_id String,
    name String,
    description String,
    logo_url String,
    website_url String,
    last_updated DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
PRIMARY KEY subnet_id;

-- Unified Subnets table - tracks all subnets with their lifecycle status
CREATE TABLE IF NOT EXISTS subnets (
    subnet_id String,  -- The subnet ID (CB58)

    -- Creation info (from CreateSubnetTx)
    created_block UInt64,  -- Block number when subnet was created
    created_time DateTime64(3, 'UTC'),  -- When subnet was created

    -- Subnet type/status
    subnet_type LowCardinality(String),  -- 'regular', 'elastic', 'l1'

    -- L1/Elastic conversion info (nullable, populated when converted)
    chain_id String,  -- Associated chain ID (empty for non-L1 subnets)
    converted_block UInt64,  -- Block number when converted (0 if not converted)
    converted_time DateTime64(3, 'UTC'),  -- When converted (epoch 0 if not converted)

    -- Metadata
    p_chain_id UInt32,  -- Which P-chain instance
    last_updated DateTime64(3, 'UTC')  -- Last time this record was updated
) ENGINE = ReplacingMergeTree(last_updated)
PRIMARY KEY (p_chain_id, subnet_id);

-- Subnet Chains table - tracks blockchains created within subnets
CREATE TABLE IF NOT EXISTS subnet_chains (
    chain_id String,  -- The blockchain ID (CB58)
    subnet_id String,  -- Parent subnet ID (CB58)
    chain_name String,  -- Chain name
    vm_id String,  -- VM ID (CB58)
    created_block UInt64,  -- Block number when chain was created
    created_time DateTime64(3, 'UTC'),  -- When chain was created
    p_chain_id UInt32,  -- Which P-chain instance
    last_updated DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
PRIMARY KEY (p_chain_id, chain_id);

-- L1 Fee Stats table - tracks total validation fees paid per L1
CREATE TABLE IF NOT EXISTS l1_fee_stats (
    subnet_id String,  -- The L1 subnet ID (CB58)

    -- Deposit tracking (all values in nAVAX)
    total_deposited UInt64,  -- Total AVAX deposited across all validators
    initial_deposits UInt64,  -- From ConvertSubnetToL1Tx + RegisterL1ValidatorTx
    top_up_deposits UInt64,  -- From IncreaseL1ValidatorBalanceTx
    total_refunded UInt64 DEFAULT 0,  -- Total refunds from DisableL1Validator

    -- Current state
    current_balance UInt64,  -- Sum of current remaining balances

    -- Calculated fee (deposited - refunded - current balance)
    total_fees_paid UInt64,  -- Total fees consumed by validators

    -- Counts
    deposit_tx_count UInt32,  -- Number of deposit transactions
    validator_count UInt32,  -- Number of validators (active + inactive)

    -- Metadata
    p_chain_id UInt32,
    last_updated DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
PRIMARY KEY (p_chain_id, subnet_id);

-- L1 Validator History table - tracks all L1 validators from creation
CREATE TABLE IF NOT EXISTS l1_validator_history (
    -- Identifiers
    subnet_id String,  -- The L1 subnet ID (CB58)
    node_id String,  -- Node ID (CB58 format, e.g., "NodeID-xxx...")
    validation_id String,  -- Computed validation ID (CB58)

    -- Creation info
    created_tx_id String,  -- Transaction that created this validator
    created_tx_type LowCardinality(String),  -- 'ConvertSubnetToL1' or 'RegisterL1Validator'
    created_block UInt64,  -- Block when validator was created
    created_time DateTime64(3, 'UTC'),  -- When validator was created

    -- Initial values at creation
    initial_balance UInt64,  -- Balance at creation (in nAVAX)
    initial_weight UInt64,  -- Weight at creation

    -- BLS key info (from creation tx)
    bls_public_key String,  -- BLS public key (hex)

    -- Owner info (for refunds when disabled)
    remaining_balance_owner String,  -- P-Chain address to receive remaining balance (CB58)

    -- Metadata
    p_chain_id UInt32,
    last_updated DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (p_chain_id, subnet_id, node_id, created_block);

-- L1 Validator Balance Transactions table - tracks all balance-affecting transactions
-- Indexed by validation_id and node_id for fast lookups from frontend
CREATE TABLE IF NOT EXISTS l1_validator_balance_txs (
    -- Identifiers
    validation_id String,  -- The validation ID (CB58), may be empty for disabled validators
    tx_id String,  -- Transaction ID
    tx_type LowCardinality(String),  -- 'ConvertSubnetToL1', 'RegisterL1Validator', or 'IncreaseL1ValidatorBalance'

    -- Transaction details
    block_number UInt64,
    block_time DateTime64(3, 'UTC'),
    amount UInt64,  -- Amount added to balance (in nAVAX)

    -- Additional context
    subnet_id String,  -- For filtering by subnet
    node_id String,  -- For correlation with validator

    -- Metadata
    p_chain_id UInt32,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (p_chain_id, node_id, tx_id);

-- L1 Validator Refunds table - tracks refunds when validators are disabled
CREATE TABLE IF NOT EXISTS l1_validator_refunds (
    -- Identifiers
    tx_id String,  -- DisableL1Validator transaction ID
    validation_id String,  -- The validation ID (CB58)
    subnet_id String,  -- The L1 subnet ID (CB58)

    -- Refund details
    refund_amount UInt64,  -- Actual refund amount (in nAVAX)
    refund_address String,  -- Address that received the refund (remainingBalanceOwner)

    -- Transaction details
    block_number UInt64,
    block_time DateTime64(3, 'UTC'),

    -- Metadata
    p_chain_id UInt32
) ENGINE = ReplacingMergeTree(block_time)
ORDER BY (p_chain_id, validation_id, tx_id);

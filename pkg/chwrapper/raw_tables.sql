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
    parent_beacon_block_root LowCardinality(FixedString(32))  -- Often all zeros
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

-- P-chain transactions table - all P-chain transaction types in one table
CREATE TABLE IF NOT EXISTS p_chain_txs (
    tx_id FixedString(32),
    tx_type LowCardinality(String),
    block_number UInt64,
    block_time DateTime64(3, 'UTC'),
    
    -- Validator fields (nullable, used by AddValidator, AddDelegator, etc.)
    node_id Nullable(FixedString(20)),
    start_time Nullable(UInt64),
    end_time Nullable(UInt64),
    weight Nullable(UInt64),
    
    -- Subnet fields
    subnet_id Nullable(FixedString(32)),
    chain_id Nullable(FixedString(32)),
    
    -- ConvertSubnetToL1Tx specific
    address Nullable(FixedString(20)),
    validators String,  -- JSON-encoded validator list
    
    -- CreateSubnetTx / TransferSubnetOwnershipTx
    owner String,  -- JSON-encoded owner structure
    
    -- CreateChainTx specific
    chain_name String,
    genesis_data String,
    vm_id Nullable(FixedString(32)),
    fx_ids String,  -- JSON-encoded array of IDs
    subnet_auth String,  -- JSON-encoded subnet authorization
    
    -- ImportTx / ExportTx
    source_chain Nullable(FixedString(32)),
    destination_chain Nullable(FixedString(32)),
    
    -- RewardValidatorTx
    reward_tx_id Nullable(FixedString(32)),
    
    -- TransformSubnetTx specific
    asset_id Nullable(FixedString(32)),
    initial_supply Nullable(UInt64),
    max_supply Nullable(UInt64),
    min_consumption_rate Nullable(UInt64),
    max_consumption_rate Nullable(UInt64),
    min_validator_stake Nullable(UInt64),
    max_validator_stake Nullable(UInt64),
    min_stake_duration Nullable(UInt32),
    max_stake_duration Nullable(UInt32),
    min_delegation_fee Nullable(UInt32),
    min_delegator_stake Nullable(UInt64),
    max_validator_weight_factor Nullable(UInt8),
    uptime_requirement Nullable(UInt32),
    
    -- AddPermissionlessValidatorTx / AddPermissionlessDelegatorTx
    signer String,  -- JSON-encoded signer
    stake_outs String,  -- JSON-encoded stake outputs
    validator_rewards_owner String,  -- JSON-encoded rewards owner
    delegator_rewards_owner String,  -- JSON-encoded rewards owner
    delegation_shares Nullable(UInt32),
    
    -- IncreaseL1ValidatorBalanceTx
    validation_id Nullable(FixedString(32)),
    balance Nullable(UInt64),
    
    -- SetL1ValidatorWeightTx
    message String,  -- Warp message with SetL1ValidatorWeight
    
    -- AdvanceTimeTx
    time Nullable(UInt64)  -- Unix time this block proposes increasing the timestamp to
) ENGINE = MergeTree()
ORDER BY (block_time, block_number, tx_type);


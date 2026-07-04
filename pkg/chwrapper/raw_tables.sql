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
    min_delay_excess UInt64,
    tx_count UInt32  -- Denormalized transaction count for fast list queries
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
    )),  -- Properly structured, not JSON
    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 1,  -- Fast tx lookup by hash
    INDEX idx_from `from` TYPE bloom_filter GRANULARITY 1,  -- Fast address lookup
    INDEX idx_to `to` TYPE bloom_filter GRANULARITY 1  -- Fast address lookup
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
    tx_to Nullable(FixedString(20)),  -- Original transaction target (denormalized)
    INDEX idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 1,  -- Fast trace lookup by tx hash
    INDEX idx_from `from` TYPE bloom_filter GRANULARITY 1,  -- Fast address lookup
    INDEX idx_to `to` TYPE bloom_filter GRANULARITY 1  -- Fast address lookup
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
    removed Bool,  -- TODO: check if ever happen to be true
    INDEX idx_block_number block_number TYPE minmax GRANULARITY 1,  -- Speeds up block range queries
    INDEX idx_transaction_hash transaction_hash TYPE bloom_filter GRANULARITY 1  -- Fast log lookup by tx hash
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
    validator_manager_address String DEFAULT '',  -- EVM address of the ValidatorManager contract (0x-prefixed hex)
    p_chain_id UInt32,  -- Which P-chain instance
    last_synced DateTime64(3, 'UTC')  -- Last time validators were synced for this subnet
) ENGINE = ReplacingMergeTree(last_synced)
PRIMARY KEY (p_chain_id, subnet_id);

-- L1 Registry table - metadata from external registry (l1beat-l1-registry npm package)
-- Keyed by chain_id (blockchainId from registry) so each chain gets its own metadata
CREATE TABLE IF NOT EXISTS l1_registry (
    blockchain_id String,                    -- blockchain ID (CB58) from registry chains[].blockchainId
    subnet_id String,
    name String,
    description String,
    logo_url String,
    website_url String,

    -- Extended fields from l1beat-l1-registry
    network String DEFAULT 'mainnet',       -- "mainnet" or "fuji"
    is_l1 Bool DEFAULT true,                -- L1 vs legacy subnet
    categories Array(String),               -- e.g. ["DeFi", "Gaming"]
    socials String DEFAULT '[]',            -- JSON array of {name, url}

    -- Per-chain fields
    evm_chain_id UInt64 DEFAULT 0,          -- EVM chain ID (e.g. 43114)
    rpc_url String DEFAULT '',              -- Primary RPC URL
    explorer_url String DEFAULT '',         -- Block explorer URL
    sybil_resistance_type String DEFAULT '',-- "Proof of Stake" or "Proof of Authority"

    -- Native token
    network_token_name String DEFAULT '',
    network_token_symbol String DEFAULT '',
    network_token_decimals UInt8 DEFAULT 18,
    network_token_logo_uri String DEFAULT '',

    -- Staking (PoS only): converts P-Chain validator weight to a whole-token
    -- staked amount as staked = weight / staking_weight_factor. Curated in the
    -- registry because the on-chain weightToValueFactor doesn't reconcile with
    -- token decimals for several L1s. 0 = not staked / unset (PoA). The staked
    -- token may differ from the gas token, so its symbol is stored explicitly.
    staking_weight_factor UInt64 DEFAULT 0,
    staking_token_symbol String DEFAULT '',

    last_updated DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
PRIMARY KEY blockchain_id;

-- Migrate existing l1_registry deployments to add staking fields (idempotent, no-op on fresh installs)
ALTER TABLE l1_registry ADD COLUMN IF NOT EXISTS staking_weight_factor UInt64 DEFAULT 0;
ALTER TABLE l1_registry ADD COLUMN IF NOT EXISTS staking_token_symbol String DEFAULT '';

-- Unified Subnets table - tracks all subnets with their lifecycle status
CREATE TABLE IF NOT EXISTS subnets (
    subnet_id String,  -- The subnet ID (CB58)

    -- Creation info (from CreateSubnetTx)
    created_block UInt64,  -- Block number when subnet was created
    created_time DateTime64(3, 'UTC'),  -- When subnet was created

    -- Subnet type/status
    subnet_type LowCardinality(String),  -- 'legacy', 'primary', 'l1'

    -- L1/Elastic conversion info (nullable, populated when converted)
    chain_id String,  -- Associated chain ID (empty for non-L1 subnets)
    converted_block UInt64,  -- Block number when converted (0 if not converted)
    converted_time DateTime64(3, 'UTC'),  -- When converted (epoch 0 if not converted)
    validator_manager_address String DEFAULT '',  -- EVM address of the ValidatorManager contract (0x-prefixed hex)
    validator_manager_owner String DEFAULT '',  -- Owner of the ValidatorManager contract (0x-prefixed hex)

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

-- Stablecoins registry - curated list of stablecoins by chain.
-- Populated via the embedded seed (stablecoins_seed.sql) on startup, and extensible
-- by manual INSERTs as new stablecoins launch. ReplacingMergeTree dedupes by (chain_id, token).
CREATE TABLE IF NOT EXISTS stablecoins (
    chain_id UInt32,
    token FixedString(20),
    symbol String,
    name String,
    decimals UInt8,
    peg LowCardinality(String) DEFAULT 'USD',
    issuer String DEFAULT '',
    bridged Bool DEFAULT false,
    -- doublecounted: reserves are themselves other counted stablecoins (e.g. avUSD,
    -- UTY, reUSD, FRXUSD), so they inflate a naive sum. Excluded from the NET total;
    -- still listed (GROSS). Frontend sources the exclusion set from this flag.
    doublecounted Bool DEFAULT false,
    added_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(added_at)
ORDER BY (chain_id, token);
ALTER TABLE stablecoins ADD COLUMN IF NOT EXISTS doublecounted Bool DEFAULT false;

-- Stablecoin excluded holders - wallets to subtract when computing circulating supply.
-- Used for treasury / issuance / bridge contracts that hold tokens which haven't been
-- distributed to end users (e.g. Tether treasury). Seeded with known addresses on startup;
-- extensible by manual INSERTs.
CREATE TABLE IF NOT EXISTS stablecoin_excluded_holders (
    chain_id UInt32,
    token FixedString(20),
    holder FixedString(20),
    reason String DEFAULT '',
    added_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(added_at)
ORDER BY (chain_id, token, holder);

-- Stablecoin time-series metrics (supply, volume, transfers, holders per token per period)
CREATE TABLE IF NOT EXISTS stablecoin_metrics (
    chain_id UInt32,
    token FixedString(20),
    metric_name LowCardinality(String),
    granularity LowCardinality(String),
    period DateTime64(3, 'UTC'),
    value String,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, token, metric_name, granularity, period)
PARTITION BY (chain_id, toYYYYMM(period));

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

-- L1 Validator Weight-change txs - maps each SetL1ValidatorWeight tx to the validationID
-- decoded from its signed Warp message (tx_data.message, an L1ValidatorWeightMessage),
-- which isn't a top-level JSON field. Lets these txs be resolved to a subnet (via
-- l1_validator_history) and gives a weight-change timeline per validator.
CREATE TABLE IF NOT EXISTS l1_validator_weight_txs (
    tx_id String,  -- SetL1ValidatorWeight transaction ID
    validation_id String,  -- validationID from the decoded Warp message (CB58)
    nonce UInt64,  -- message nonce (monotonic per validator)
    weight UInt64,  -- new weight (0 = removal)

    block_number UInt64,
    block_time DateTime64(3, 'UTC'),

    p_chain_id UInt32,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (p_chain_id, validation_id, tx_id);

-- Chain Risk table - ValidatorManager control & upgradeability, read from on-chain
-- contract state (eth_call / eth_getCode / eth_getStorageAt) by the risk syncer.
-- Keyed by chain_id (the blockchain ID used everywhere in the API). Nullable columns
-- hold "unknown" (we never fabricate); the /risk endpoint maps them to JSON null.
CREATE TABLE IF NOT EXISTS chain_risk (
    chain_id String,  -- Blockchain ID (CB58), same key as subnet_chains.chain_id
    validator_manager_address String,  -- The ValidatorManager contract (0x-prefixed hex)

    -- Manager type & owner
    manager_type LowCardinality(String) DEFAULT 'unknown',  -- PoA | PoS-native | PoS-erc20 | unknown
    manager_location LowCardinality(String) DEFAULT 'unknown',  -- where the manager contract has code: c-chain | self | unknown
    owner_address Nullable(String),  -- ValidatorManager owner() as 0x-prefixed hex, null if unresolved
    owner_kind LowCardinality(String) DEFAULT 'unknown',  -- eoa | multisig | timelock | dao | contract | unknown
    multisig_threshold Nullable(UInt16),  -- Gnosis Safe threshold (null if owner not a multisig)
    multisig_owners Nullable(UInt16),  -- Gnosis Safe owner count

    -- EIP-1967 proxy / upgradeability
    is_proxy Bool DEFAULT false,  -- true if the EIP-1967 implementation slot is non-zero
    proxy_implementation Nullable(String),  -- implementation address from slot 0x360894…bbc
    proxy_admin Nullable(String),  -- admin address from slot 0xb53127…103 (usually a ProxyAdmin)
    proxy_admin_owner Nullable(String),  -- ProxyAdmin.owner() — the real upgrade controller
    upgrade_delay_seconds Nullable(UInt64),  -- timelock delay on upgrades, 0 means instant, null if unknown

    -- Churn limits (read from the ValidatorManager)
    churn_period_seconds Nullable(UInt64),  -- getChurnPeriodSeconds()
    max_churn_percentage Nullable(UInt8),  -- maximumChurnPercentage from getChurnTracker()

    -- Metadata
    last_updated DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY chain_id;

-- Migrate existing chain_risk deployments to add manager_location (idempotent, no-op on fresh installs)
ALTER TABLE chain_risk ADD COLUMN IF NOT EXISTS manager_location LowCardinality(String) DEFAULT 'unknown';

-- Validator count snapshots - exact active validator counts per subnet at a
-- historical P-Chain height, sampled via platform.getValidatorsAt (the node's
-- authoritative validator diffs, not a reconstruction). Backfilled once via the
-- validator-backfill command and kept current by a daily sample from the
-- validator syncer. ReplacingMergeTree keyed by (subnet, date) so re-running a
-- backfill replaces rather than duplicates - fully idempotent.
CREATE TABLE IF NOT EXISTS validator_count_snapshots (
    snapshot_date Date,  -- The sample date (height resolved as of end of this day UTC)
    subnet_id String,  -- Primary Network = 11111111111111111111111111111111LpoYY, else the L1 subnet ID (CB58)
    p_chain_height UInt64,  -- P-Chain height the sample was taken at
    validator_count UInt32,  -- Exact active validator count at that height
    p_chain_id UInt32,  -- Which P-chain instance (mainnet vs testnet)
    sampled_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(sampled_at)
ORDER BY (p_chain_id, subnet_id, snapshot_date);

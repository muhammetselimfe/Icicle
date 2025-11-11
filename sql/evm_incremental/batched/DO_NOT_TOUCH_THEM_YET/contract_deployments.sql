-- This indexer finds contract creation transactions and stores
-- a mapping from contract address to its deployment transaction hash and block number.
-- This allows for efficient lookup of how a contract was deployed.

-- The table stores the deployment information for each contract.
-- It uses ReplacingMergeTree to handle potential re-processing of blocks,
-- ensuring that each contract address has only one entry.
CREATE TABLE IF NOT EXISTS contract_deployments (
    chain_id UInt32,
    contract_address FixedString(20),
    tx_hash FixedString(32),
    block_number UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, contract_address)
SETTINGS index_granularity = 1024;

-- Insert statement to populate the contract_deployments table.
-- It scans raw_traces for contract creation call types (CREATE, CREATE2, CREATE3).
-- The `to` field of a CREATE trace contains the new contract's address.
INSERT INTO contract_deployments (chain_id, contract_address, tx_hash, block_number)
SELECT DISTINCT
    {chain_id:UInt32} as chain_id,
    to as contract_address,
    tx_hash,
    block_number
FROM raw_traces
WHERE chain_id = {chain_id:UInt32}
  AND block_number >= {first_block:UInt64}
  AND block_number <= {last_block:UInt64}
  AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
  AND tx_success = true
  AND to IS NOT NULL
  AND to != unhex('0000000000000000000000000000000000000000');

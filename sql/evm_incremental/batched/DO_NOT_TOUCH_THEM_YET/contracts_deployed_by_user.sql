-- This indexer tracks which contracts were deployed by each address.
-- Maps deployer address -> contract addresses they deployed.
-- Useful for finding all contracts created by a specific deployer.

-- The table stores deployer-to-contract mappings.
-- Uses ReplacingMergeTree to handle potential re-processing of blocks.
CREATE TABLE IF NOT EXISTS contracts_deployed_by_user (
    chain_id UInt32,
    deployer FixedString(20),
    contract_address FixedString(20),
    tx_hash FixedString(32),
    block_number UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, deployer, contract_address)
SETTINGS index_granularity = 1024;

-- Insert statement to populate the contracts_deployed_by_user table.
-- Maps each deployer (tx_from = original transaction sender) to the contracts they created (to).
-- Uses tx_from instead of from to get the actual user, not the factory contract.
INSERT INTO contracts_deployed_by_user (chain_id, deployer, contract_address, tx_hash, block_number)
SELECT DISTINCT
    {chain_id:UInt32} as chain_id,
    tx_from as deployer,
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


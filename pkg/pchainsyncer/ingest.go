package pchainsyncer

import (
	"clickhouse-metrics-poc/pkg/pchainrpc"
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanchego/ids"
)

// idToBytes converts an ids.ID to a byte slice
func idToBytes(id ids.ID) []byte {
	return id[:]
}

// InsertPChainTxs inserts P-chain transaction data into the p_chain_txs table
func InsertPChainTxs(ctx context.Context, conn clickhouse.Conn, pchainID uint32, blocks []*pchainrpc.JSONBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO p_chain_txs (
		tx_id, tx_type, block_number, block_time, p_chain_id, tx_data
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, block := range blocks {
		for _, tx := range block.Transactions {
			txID := idToBytes(tx.TxID)

			err = batch.Append(
				txID,
				tx.TxType,
				tx.BlockHeight,
				tx.BlockTime,
				pchainID,
				string(tx.TxData), // ClickHouse JSON type expects string
			)
			if err != nil {
				return fmt.Errorf("failed to append tx %s: %w", tx.TxID, err)
			}
		}
	}

	return batch.Send()
}

// L1Subnet represents an L1 subnet to be tracked
type L1Subnet struct {
	SubnetID        ids.ID
	ChainID         ids.ID
	ConversionBlock uint64
	ConversionTime  time.Time
	PChainID        uint32
}

// InsertL1Subnets inserts or updates L1 subnet records
func InsertL1Subnets(ctx context.Context, conn clickhouse.Conn, subnets []L1Subnet) error {
	if len(subnets) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_subnets (
		subnet_id, chain_id, conversion_block, conversion_time, p_chain_id, last_synced
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, subnet := range subnets {
		err = batch.Append(
			idToBytes(subnet.SubnetID),
			idToBytes(subnet.ChainID),
			subnet.ConversionBlock,
			subnet.ConversionTime,
			subnet.PChainID,
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to append subnet %s: %w", subnet.SubnetID, err)
		}
	}

	return batch.Send()
}

// InsertValidatorStates inserts or updates validator state records
func InsertValidatorStates(ctx context.Context, conn clickhouse.Conn, pchainID uint32, states []*pchainrpc.ValidatorState) error {
	if len(states) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_validator_state (
		subnet_id, validation_id, node_id, balance, weight,
		start_time, end_time, uptime_percentage, active, last_updated, p_chain_id
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, state := range states {
		err = batch.Append(
			idToBytes(state.SubnetID),
			idToBytes(state.ValidationID),
			state.NodeID.String(),
			state.Balance,
			state.Weight,
			state.StartTime,
			state.EndTime,
			state.Uptime,
			state.Active,
			now,
			pchainID,
		)
		if err != nil {
			return fmt.Errorf("failed to append validator state %s: %w", state.ValidationID, err)
		}
	}

	return batch.Send()
}

// GetL1Subnets queries ClickHouse for all L1 subnets to monitor
func GetL1Subnets(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]ids.ID, error) {
	query := `
		SELECT DISTINCT subnet_id
		FROM l1_subnets
		WHERE p_chain_id = ?
	`

	rows, err := conn.Query(ctx, query, pchainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query l1_subnets: %w", err)
	}
	defer rows.Close()

	var subnetIDs []ids.ID
	for rows.Next() {
		var subnetBytes []byte
		if err := rows.Scan(&subnetBytes); err != nil {
			return nil, fmt.Errorf("failed to scan subnet_id: %w", err)
		}

		var subnetID ids.ID
		copy(subnetID[:], subnetBytes)
		subnetIDs = append(subnetIDs, subnetID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return subnetIDs, nil
}

// DiscoverL1SubnetsFromTransactions scans p_chain_txs for ConvertSubnetToL1 and TransformSubnet transactions
// and returns L1 subnet information
func DiscoverL1SubnetsFromTransactions(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]L1Subnet, error) {
	// We also look for TransformSubnet (which creates elastic subnets) as they are effectively L1s
	// or at least have validators we want to track.
	query := `
		SELECT
			tx_data.subnetID as subnet_id,
			coalesce(tx_data.chainID, '') as chain_id,
			block_number,
			block_time
		FROM p_chain_txs
		WHERE p_chain_id = ?
		  AND (tx_type = 'ConvertSubnetToL1' OR tx_type = 'TransformSubnet')
		ORDER BY block_number DESC
	`

	rows, err := conn.Query(ctx, query, pchainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query subnet transactions: %w", err)
	}
	defer rows.Close()

	var subnets []L1Subnet
	seen := make(map[ids.ID]bool) // Deduplicate by subnet_id

	for rows.Next() {
		var subnetIDStr, chainIDStr string
		var blockNumber uint64
		var blockTime time.Time

		if err := rows.Scan(&subnetIDStr, &chainIDStr, &blockNumber, &blockTime); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Parse subnet ID
		subnetID, err := ids.FromString(subnetIDStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse subnet ID %s: %w", subnetIDStr, err)
		}

		// Skip duplicates (keep the most recent one due to ORDER BY DESC)
		if seen[subnetID] {
			continue
		}
		seen[subnetID] = true

		// Parse chain ID (might be empty for TransformSubnet)
		var chainID ids.ID
		if chainIDStr != "" {
			chainID, err = ids.FromString(chainIDStr)
			if err != nil {
				// Log error but continue? Or fail? failing is safer to detect issues.
				// But TransformSubnet might not have chainID in the same field?
				// TransformSubnet doesn't have ChainID field. It's usually associated with the subnet.
				// We use empty ID if not found.
			}
		}

		subnets = append(subnets, L1Subnet{
			SubnetID:        subnetID,
			ChainID:         chainID,
			ConversionBlock: blockNumber,
			ConversionTime:  blockTime,
			PChainID:        pchainID,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return subnets, nil
}

package pchainsyncer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"icicle/pkg/pchainrpc"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/vms/platformvm/warp/message"
	"github.com/ava-labs/avalanchego/vms/platformvm/warp/payload"
)

// convertClickHouseArrayToJSON converts ClickHouse array string format to JSON array
// ClickHouse toString() on JSON arrays produces: ['{"key":"value"}', '{"key":"value"}']
// We need to convert this to: [{"key":"value"}, {"key":"value"}]
func convertClickHouseArrayToJSON(chArray string) string {
	// Handle empty arrays
	if chArray == "[]" || chArray == "" {
		return "[]"
	}

	// Remove the outer brackets if present
	chArray = strings.TrimSpace(chArray)
	if !strings.HasPrefix(chArray, "[") || !strings.HasSuffix(chArray, "]") {
		return "[]"
	}

	// Remove outer brackets
	inner := chArray[1 : len(chArray)-1]
	if inner == "" {
		return "[]"
	}

	// ClickHouse wraps each JSON object in single quotes: '{"json":...}'
	// We need to find these patterns and extract the JSON objects
	var result strings.Builder
	result.WriteString("[")

	// Split by "','" which separates elements in ClickHouse array notation
	// But we need to be careful because JSON can contain commas
	// Each element is wrapped in single quotes: '{"..."}', '{"..."}'
	inQuote := false
	braceDepth := 0
	elementStart := -1
	first := true

	for i := 0; i < len(inner); i++ {
		c := inner[i]

		if c == '\'' && (i == 0 || inner[i-1] != '\\') {
			if !inQuote {
				// Starting a new element
				inQuote = true
				elementStart = i + 1
			} else {
				// Ending an element
				if elementStart >= 0 && braceDepth == 0 {
					element := inner[elementStart:i]
					if len(element) > 0 {
						if !first {
							result.WriteString(",")
						}
						result.WriteString(element)
						first = false
					}
				}
				inQuote = false
				elementStart = -1
			}
		} else if inQuote {
			if c == '{' {
				braceDepth++
			} else if c == '}' {
				braceDepth--
			}
		}
	}

	result.WriteString("]")
	return result.String()
}

// hexToNodeID converts a hex-encoded node ID (with or without 0x prefix) to CB58 NodeID format
func hexToNodeID(hexStr string) (string, error) {
	// Remove 0x prefix if present
	hexStr = strings.TrimPrefix(hexStr, "0x")

	// Decode hex to bytes
	nodeBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", fmt.Errorf("failed to decode hex: %w", err)
	}

	// NodeID is 20 bytes
	if len(nodeBytes) != 20 {
		return "", fmt.Errorf("invalid node ID length: got %d, expected 20", len(nodeBytes))
	}

	// Create NodeID from bytes
	var nodeID ids.NodeID
	copy(nodeID[:], nodeBytes)

	return nodeID.String(), nil
}

// idToBytes converts an ids.ID to a byte slice
func idToBytes(id ids.ID) []byte {
	return id[:]
}

// MaxTxsPerInsertBatch limits the number of transactions per ClickHouse insert
// to avoid memory limit errors on servers with limited RAM
const MaxTxsPerInsertBatch = 5000

// InsertPChainTxs inserts P-chain transaction data into the p_chain_txs table
// It automatically splits large batches to avoid ClickHouse memory limits
func InsertPChainTxs(ctx context.Context, conn clickhouse.Conn, pchainID uint32, blocks []*pchainrpc.JSONBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	// Collect all transactions first
	type txData struct {
		txID        string
		txType      string
		blockHeight uint64
		blockTime   time.Time
		txDataJSON  string
	}
	var allTxs []txData

	for _, block := range blocks {
		for _, tx := range block.Transactions {
			allTxs = append(allTxs, txData{
				txID:        tx.TxID.String(),
				txType:      tx.TxType,
				blockHeight: tx.BlockHeight,
				blockTime:   tx.BlockTime,
				txDataJSON:  string(tx.TxData),
			})
		}
	}

	// Insert in smaller batches to avoid memory issues
	for i := 0; i < len(allTxs); i += MaxTxsPerInsertBatch {
		end := i + MaxTxsPerInsertBatch
		if end > len(allTxs) {
			end = len(allTxs)
		}
		chunk := allTxs[i:end]

		batch, err := conn.PrepareBatch(ctx, `INSERT INTO p_chain_txs (
			tx_id, tx_type, block_number, block_time, p_chain_id, tx_data
		)`)
		if err != nil {
			return fmt.Errorf("failed to prepare batch: %w", err)
		}

		for _, tx := range chunk {
			err = batch.Append(
				tx.txID,
				tx.txType,
				tx.blockHeight,
				tx.blockTime,
				pchainID,
				tx.txDataJSON,
			)
			if err != nil {
				return fmt.Errorf("failed to append tx %s: %w", tx.txID, err)
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
	}

	return nil
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
			subnet.SubnetID.String(), // Store as CB58 string
			subnet.ChainID.String(),  // Store as CB58 string
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
			state.SubnetID.String(),     // Store as CB58 string
			state.ValidationID.String(), // Store as CB58 string
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

// MarkInactiveValidators marks validators as inactive if they are no longer in the current RPC response.
// This handles validators whose staking period has ended and are no longer returned by getCurrentValidators.
func MarkInactiveValidators(ctx context.Context, conn clickhouse.Conn, pchainID uint32, subnetID string, activeValidationIDs []string) error {
	if len(activeValidationIDs) == 0 {
		return nil
	}

	// First, get all validators currently marked as active in the database for this subnet
	query := `
		SELECT validation_id, node_id, balance, weight, start_time, end_time, uptime_percentage
		FROM l1_validator_state FINAL
		WHERE p_chain_id = ? AND subnet_id = ? AND active = true
	`
	rows, err := conn.Query(ctx, query, pchainID, subnetID)
	if err != nil {
		return fmt.Errorf("failed to query active validators: %w", err)
	}
	defer rows.Close()

	// Build a set of active validation IDs from the RPC response
	activeSet := make(map[string]bool)
	for _, id := range activeValidationIDs {
		activeSet[id] = true
	}

	// Collect validators that are no longer active
	type inactiveValidator struct {
		ValidationID string
		NodeID       string
		Balance      uint64
		Weight       uint64
		StartTime    time.Time
		EndTime      time.Time
		Uptime       float64
	}
	var toDeactivate []inactiveValidator

	for rows.Next() {
		var v inactiveValidator
		if err := rows.Scan(&v.ValidationID, &v.NodeID, &v.Balance, &v.Weight, &v.StartTime, &v.EndTime, &v.Uptime); err != nil {
			return fmt.Errorf("failed to scan validator: %w", err)
		}

		// If this validator is not in the active set from RPC, mark it as inactive
		if !activeSet[v.ValidationID] {
			toDeactivate = append(toDeactivate, v)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating validators: %w", err)
	}

	if len(toDeactivate) == 0 {
		return nil
	}

	// Insert inactive records (ReplacingMergeTree will keep the latest version)
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_validator_state (
		subnet_id, validation_id, node_id, balance, weight,
		start_time, end_time, uptime_percentage, active, last_updated, p_chain_id
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch for inactive validators: %w", err)
	}

	now := time.Now()
	for _, v := range toDeactivate {
		err = batch.Append(
			subnetID,
			v.ValidationID,
			v.NodeID,
			v.Balance,
			v.Weight,
			v.StartTime,
			v.EndTime,
			v.Uptime,
			false, // Mark as inactive
			now,
			pchainID,
		)
		if err != nil {
			return fmt.Errorf("failed to append inactive validator %s: %w", v.ValidationID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send inactive validators batch: %w", err)
	}

	log.Printf("Marked %d validators as inactive for subnet %s", len(toDeactivate), subnetID)
	return nil
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
		var subnetIDStr string
		if err := rows.Scan(&subnetIDStr); err != nil {
			return nil, fmt.Errorf("failed to scan subnet_id: %w", err)
		}

		subnetID, err := ids.FromString(subnetIDStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse subnet_id %s: %w", subnetIDStr, err)
		}
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

// Subnet represents a subnet with its lifecycle information
type Subnet struct {
	SubnetID       ids.ID
	CreatedBlock   uint64
	CreatedTime    time.Time
	SubnetType     string // 'regular', 'elastic', 'l1'
	ChainID        ids.ID
	ConvertedBlock uint64
	ConvertedTime  time.Time
	PChainID       uint32
}

// SubnetChain represents a blockchain created within a subnet
type SubnetChain struct {
	ChainID      ids.ID
	SubnetID     ids.ID
	ChainName    string
	VMID         ids.ID
	CreatedBlock uint64
	CreatedTime  time.Time
	PChainID     uint32
}

// DiscoverAllSubnets scans p_chain_txs for all subnet-related transactions
// and returns a unified view of all subnets with their current status
func DiscoverAllSubnets(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]Subnet, error) {
	// Map of subnet_id -> Subnet
	subnetsMap := make(map[ids.ID]*Subnet)

	// Helper to update subnet map with earliest occurrence
	updateSubnetMap := func(subnetIDStr string, blockNumber uint64, blockTime time.Time) {
		if subnetIDStr == "" {
			return
		}
		subnetID, err := ids.FromString(subnetIDStr)
		if err != nil {
			return
		}
		existing, exists := subnetsMap[subnetID]
		if !exists || blockNumber < existing.CreatedBlock {
			subnetsMap[subnetID] = &Subnet{
				SubnetID:     subnetID,
				CreatedBlock: blockNumber,
				CreatedTime:  blockTime,
				SubnetType:   "regular",
				PChainID:     pchainID,
			}
		}
	}

	// First, get the max block we've already processed from the subnets table
	// This allows incremental discovery instead of re-scanning the entire history
	var lastProcessedBlock uint64
	err := conn.QueryRow(ctx, `
		SELECT COALESCE(max(created_block), 0) FROM subnets FINAL WHERE p_chain_id = ?
	`, pchainID).Scan(&lastProcessedBlock)
	if err != nil {
		log.Printf("WARNING: Could not get last processed block, will scan from start: %v", err)
		lastProcessedBlock = 0
	}

	// Query each transaction type separately, only for blocks after last processed
	txTypes := []string{"CreateChain", "AddSubnetValidator", "ConvertSubnetToL1", "TransformSubnet"}

	for _, txType := range txTypes {
		// Only query new blocks to avoid memory issues with full table scans
		query := `
			SELECT
				CAST(tx_data.subnetID AS String) as subnet_id,
				block_number,
				block_time
			FROM p_chain_txs
			WHERE p_chain_id = ?
			  AND tx_type = ?
			  AND block_number > ?
			  AND tx_data.subnetID != ''
			ORDER BY block_number ASC
		`

		rows, err := conn.Query(ctx, query, pchainID, txType, lastProcessedBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to query %s transactions: %w", txType, err)
		}

		for rows.Next() {
			var subnetIDStr string
			var blockNumber uint64
			var blockTime time.Time

			if err := rows.Scan(&subnetIDStr, &blockNumber, &blockTime); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan %s row: %w", txType, err)
			}
			updateSubnetMap(subnetIDStr, blockNumber, blockTime)
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%s discovery rows error: %w", txType, err)
		}
	}

	// Now check for conversions (TransformSubnet and ConvertSubnetToL1)
	// These are relatively small tables so we can scan them fully
	conversionQuery := `
		SELECT
			CAST(tx_data.subnetID AS String) as subnet_id,
			tx_type,
			CAST(coalesce(tx_data.chainID, '') AS String) as chain_id,
			block_number,
			block_time
		FROM p_chain_txs
		WHERE p_chain_id = ?
		  AND tx_type IN ('TransformSubnet', 'ConvertSubnetToL1')
		  AND tx_data.subnetID != ''
		ORDER BY block_number ASC
	`

	rows, err := conn.Query(ctx, conversionQuery, pchainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query conversion transactions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var subnetIDStr, txType, chainIDStr string
		var blockNumber uint64
		var blockTime time.Time

		if err := rows.Scan(&subnetIDStr, &txType, &chainIDStr, &blockNumber, &blockTime); err != nil {
			return nil, fmt.Errorf("failed to scan conversion row: %w", err)
		}

		// Skip empty subnet IDs
		if subnetIDStr == "" {
			continue
		}

		subnetID, err := ids.FromString(subnetIDStr)
		if err != nil {
			// Skip invalid subnet IDs (likely CB58 encoding issues)
			// log.Printf("WARNING: Skipping invalid subnet ID %s: %v", subnetIDStr, err)
			continue
		}

		subnet, exists := subnetsMap[subnetID]
		if !exists {
			// Subnet was converted but never had a CreateSubnet tx (shouldn't happen normally)
			// Create it anyway with conversion as creation
			subnet = &Subnet{
				SubnetID:     subnetID,
				CreatedBlock: blockNumber,
				CreatedTime:  blockTime,
				PChainID:     pchainID,
			}
			subnetsMap[subnetID] = subnet
		}

		// Update subnet type based on transaction
		if txType == "TransformSubnet" {
			subnet.SubnetType = "elastic"
			subnet.ConvertedBlock = blockNumber
			subnet.ConvertedTime = blockTime
		} else if txType == "ConvertSubnetToL1" {
			subnet.SubnetType = "l1"
			subnet.ConvertedBlock = blockNumber
			subnet.ConvertedTime = blockTime

			// Parse chain ID for L1 conversions
			if chainIDStr != "" {
				chainID, err := ids.FromString(chainIDStr)
				if err == nil {
					subnet.ChainID = chainID
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("conversion rows error: %w", err)
	}

	// Convert map to slice
	subnets := make([]Subnet, 0, len(subnetsMap))
	for _, subnet := range subnetsMap {
		subnets = append(subnets, *subnet)
	}

	return subnets, nil
}

// DiscoverSubnetChains scans p_chain_txs for CreateChain transactions
// and matches them with ConvertSubnetToL1 chain IDs
func DiscoverSubnetChains(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]SubnetChain, error) {
	// Get chain IDs from ConvertSubnetToL1 transactions (which have proper CB58-encoded chainID)
	// Then find the corresponding CreateChain transaction to get name and VM ID
	query := `
		WITH l1_chains AS (
			SELECT DISTINCT
				CAST(tx_data.chainID AS String) as chain_id,
				CAST(tx_data.subnetID AS String) as subnet_id
			FROM p_chain_txs
			WHERE p_chain_id = ?
			  AND tx_type = 'ConvertSubnetToL1'
			  AND tx_data.chainID != ''
			  AND tx_data.subnetID != ''
		),
		create_chain_info AS (
			SELECT
				CAST(tx_data.subnetID AS String) as subnet_id,
				CAST(coalesce(tx_data.chainName, '') AS String) as chain_name,
				CAST(coalesce(tx_data.vmID, '') AS String) as vm_id,
				block_number,
				block_time,
				ROW_NUMBER() OVER (PARTITION BY tx_data.subnetID ORDER BY block_number ASC) as rn
			FROM p_chain_txs
			WHERE p_chain_id = ?
			  AND tx_type = 'CreateChain'
			  AND tx_data.subnetID != ''
		)
		SELECT
			l.chain_id,
			l.subnet_id,
			COALESCE(c.chain_name, '') as chain_name,
			COALESCE(c.vm_id, '') as vm_id,
			COALESCE(c.block_number, 0) as created_block,
			COALESCE(c.block_time, toDateTime('1970-01-01 00:00:00')) as created_time
		FROM l1_chains l
		LEFT JOIN create_chain_info c ON l.subnet_id = c.subnet_id AND c.rn = 1
	`

	rows, err := conn.Query(ctx, query, pchainID, pchainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query chain info: %w", err)
	}
	defer rows.Close()

	var chains []SubnetChain
	seen := make(map[ids.ID]bool)

	for rows.Next() {
		var chainIDStr, subnetIDStr, chainName, vmIDStr string
		var blockNumber uint64
		var blockTime time.Time

		if err := rows.Scan(&chainIDStr, &subnetIDStr, &chainName, &vmIDStr, &blockNumber, &blockTime); err != nil {
			return nil, fmt.Errorf("failed to scan chain row: %w", err)
		}

		// Skip empty or invalid chain IDs
		if chainIDStr == "" || subnetIDStr == "" {
			continue
		}

		chainID, err := ids.FromString(chainIDStr)
		if err != nil {
			continue
		}

		// Skip duplicates
		if seen[chainID] {
			continue
		}
		seen[chainID] = true

		subnetID, err := ids.FromString(subnetIDStr)
		if err != nil {
			continue
		}

		var vmID ids.ID
		if vmIDStr != "" {
			vmID, err = ids.FromString(vmIDStr)
			if err != nil {
				vmID = ids.Empty
			}
		}

		chains = append(chains, SubnetChain{
			ChainID:      chainID,
			SubnetID:     subnetID,
			ChainName:    chainName,
			VMID:         vmID,
			CreatedBlock: blockNumber,
			CreatedTime:  blockTime,
			PChainID:     pchainID,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chain discovery rows error: %w", err)
	}

	return chains, nil
}

// InsertSubnets inserts or updates subnet records in the unified subnets table
func InsertSubnets(ctx context.Context, conn clickhouse.Conn, subnets []Subnet) error {
	if len(subnets) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO subnets (
		subnet_id, created_block, created_time, subnet_type,
		chain_id, converted_block, converted_time, p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, subnet := range subnets {
		err = batch.Append(
			subnet.SubnetID.String(),
			subnet.CreatedBlock,
			subnet.CreatedTime,
			subnet.SubnetType,
			subnet.ChainID.String(),
			subnet.ConvertedBlock,
			subnet.ConvertedTime,
			subnet.PChainID,
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to append subnet %s: %w", subnet.SubnetID, err)
		}
	}

	return batch.Send()
}

// Avalanche mainnet genesis timestamp: September 21, 2020 10:00:00 UTC
var AvalancheGenesisTime = time.Unix(1600714800, 0)

// InsertPrimaryNetwork inserts the Primary Network as a special genesis subnet
func InsertPrimaryNetwork(ctx context.Context, conn clickhouse.Conn, pchainID uint32) error {
	// Primary Network subnet ID (all 1s in CB58)
	primarySubnetID := "11111111111111111111111111111111LpoYY"

	subnet := Subnet{
		SubnetID:     ids.Empty, // Will use string directly
		CreatedBlock: 0,         // Genesis
		CreatedTime:  AvalancheGenesisTime,
		SubnetType:   "primary", // Special type for Primary Network
		PChainID:     pchainID,
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO subnets (
		subnet_id, created_block, created_time, subnet_type,
		chain_id, converted_block, converted_time, p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	err = batch.Append(
		primarySubnetID,
		subnet.CreatedBlock,
		subnet.CreatedTime,
		subnet.SubnetType,
		"",
		uint64(0),
		AvalancheGenesisTime,
		subnet.PChainID,
		now,
	)
	if err != nil {
		return fmt.Errorf("failed to append primary network: %w", err)
	}

	return batch.Send()
}

// InsertPrimaryNetworkChains inserts C-Chain, X-Chain, and P-Chain
func InsertPrimaryNetworkChains(ctx context.Context, conn clickhouse.Conn, pchainID uint32) error {
	primarySubnetID := "11111111111111111111111111111111LpoYY"

	chains := []struct {
		chainID   string
		chainName string
		vmID      string
	}{
		{
			chainID:   "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5", // C-Chain
			chainName: "C-Chain",
			vmID:      "mgj786NP7uDwBCcq6YwThhaN8FLyybkCa4zBWTQbNgmK6k9A6", // EVM
		},
		{
			chainID:   "2oYMBNV4eNHyqk2fjjV5nVQLDbtmNJzq5s3qs3Lo6ftnC6FByM", // X-Chain
			chainName: "X-Chain",
			vmID:      "avm", // AVM
		},
		{
			chainID:   "11111111111111111111111111111111LpoYY", // P-Chain
			chainName: "P-Chain",
			vmID:      "platform", // Platform VM
		},
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO subnet_chains (
		chain_id, subnet_id, chain_name, vm_id,
		created_block, created_time, p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, chain := range chains {
		err = batch.Append(
			chain.chainID,
			primarySubnetID,
			chain.chainName,
			chain.vmID,
			uint64(0), // Genesis
			AvalancheGenesisTime,
			pchainID,
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to append chain %s: %w", chain.chainName, err)
		}
	}

	return batch.Send()
}

// InsertSubnetChains inserts or updates subnet chain records
func InsertSubnetChains(ctx context.Context, conn clickhouse.Conn, chains []SubnetChain) error {
	if len(chains) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO subnet_chains (
		chain_id, subnet_id, chain_name, vm_id,
		created_block, created_time, p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, chain := range chains {
		err = batch.Append(
			chain.ChainID.String(),
			chain.SubnetID.String(),
			chain.ChainName,
			chain.VMID.String(),
			chain.CreatedBlock,
			chain.CreatedTime,
			chain.PChainID,
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to append chain %s: %w", chain.ChainID, err)
		}
	}

	return batch.Send()
}

// L1FeeStats represents fee statistics for an L1 subnet
type L1FeeStats struct {
	SubnetID        string
	TotalDeposited  uint64
	InitialDeposits uint64
	TopUpDeposits   uint64
	TotalRefunded   uint64
	CurrentBalance  uint64
	TotalFeesPaid   uint64
	DepositTxCount  uint32
	ValidatorCount  uint32
	PChainID        uint32
}

// CalculateL1FeeStats calculates fee statistics for all L1 subnets
func CalculateL1FeeStats(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]L1FeeStats, error) {
	// This query calculates fee stats using deposits, refunds, and current balances
	query := `
		WITH
		-- All deposits from l1_validator_balance_txs (deduplicated)
		deposits AS (
			SELECT
				subnet_id,
				sumIf(amount, tx_type IN ('ConvertSubnetToL1', 'RegisterL1Validator')) as initial_deposits,
				sumIf(amount, tx_type = 'IncreaseL1ValidatorBalance') as topup_deposits,
				sum(amount) as total_deposits,
				countIf(tx_type IN ('ConvertSubnetToL1', 'RegisterL1Validator')) as validator_count,
				count(*) as tx_count
			FROM l1_validator_balance_txs FINAL
			WHERE p_chain_id = ?
			GROUP BY subnet_id
		),
		-- Total refunds from l1_validator_refunds
		refunds AS (
			SELECT
				subnet_id,
				sum(refund_amount) as total_refunds
			FROM l1_validator_refunds FINAL
			WHERE p_chain_id = ?
			GROUP BY subnet_id
		),
		-- Current balances from l1_validator_state
		current_balances AS (
			SELECT
				subnet_id,
				sum(balance) as current_balance
			FROM l1_validator_state FINAL
			WHERE p_chain_id = ?
			GROUP BY subnet_id
		),
		-- All L1 subnets
		l1_subnets AS (
			SELECT DISTINCT subnet_id
			FROM subnets FINAL
			WHERE p_chain_id = ? AND subnet_type = 'l1'
		)
		SELECT
			l1.subnet_id,
			COALESCE(d.initial_deposits, 0) as initial_deposits,
			COALESCE(d.topup_deposits, 0) as topup_deposits,
			COALESCE(d.total_deposits, 0) as total_deposited,
			COALESCE(r.total_refunds, 0) as total_refunded,
			COALESCE(cb.current_balance, 0) as current_balance,
			COALESCE(d.tx_count, 0) as deposit_tx_count,
			COALESCE(d.validator_count, 0) as validator_count
		FROM l1_subnets l1
		LEFT JOIN deposits d ON l1.subnet_id = d.subnet_id
		LEFT JOIN refunds r ON l1.subnet_id = r.subnet_id
		LEFT JOIN current_balances cb ON l1.subnet_id = cb.subnet_id
		ORDER BY total_deposited DESC
	`

	rows, err := conn.Query(ctx, query, pchainID, pchainID, pchainID, pchainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query fee stats: %w", err)
	}
	defer rows.Close()

	var stats []L1FeeStats
	for rows.Next() {
		var s L1FeeStats
		var initialDeposits, topupDeposits, totalDeposited, totalRefunded, currentBalance uint64
		var depositTxCount, validatorCount uint64

		if err := rows.Scan(
			&s.SubnetID,
			&initialDeposits,
			&topupDeposits,
			&totalDeposited,
			&totalRefunded,
			&currentBalance,
			&depositTxCount,
			&validatorCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan fee stats row: %w", err)
		}

		s.InitialDeposits = initialDeposits
		s.TopUpDeposits = topupDeposits
		s.TotalDeposited = totalDeposited
		s.TotalRefunded = totalRefunded
		s.CurrentBalance = currentBalance
		s.DepositTxCount = uint32(depositTxCount)
		s.ValidatorCount = uint32(validatorCount)
		s.PChainID = pchainID

		// Calculate fees paid: deposited - refunded - current balance
		// This gives accurate fees for both active and disabled validators
		if totalDeposited > totalRefunded+currentBalance {
			s.TotalFeesPaid = totalDeposited - totalRefunded - currentBalance
		} else {
			s.TotalFeesPaid = 0
		}

		stats = append(stats, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fee stats rows error: %w", err)
	}

	return stats, nil
}

// InsertL1FeeStats inserts or updates L1 fee statistics
func InsertL1FeeStats(ctx context.Context, conn clickhouse.Conn, stats []L1FeeStats) error {
	if len(stats) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_fee_stats (
		subnet_id, total_deposited, initial_deposits, top_up_deposits, total_refunded,
		current_balance, total_fees_paid, deposit_tx_count, validator_count,
		p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, s := range stats {
		err = batch.Append(
			s.SubnetID,
			s.TotalDeposited,
			s.InitialDeposits,
			s.TopUpDeposits,
			s.TotalRefunded,
			s.CurrentBalance,
			s.TotalFeesPaid,
			s.DepositTxCount,
			s.ValidatorCount,
			s.PChainID,
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to append fee stats for %s: %w", s.SubnetID, err)
		}
	}

	return batch.Send()
}

// L1ValidatorHistory represents a historical L1 validator record
type L1ValidatorHistory struct {
	SubnetID              string
	NodeID                string
	ValidationID          string // Computed validation ID (CB58)
	CreatedTxID           string
	CreatedTxType         string
	CreatedBlock          uint64
	CreatedTime           time.Time
	InitialBalance        uint64
	InitialWeight         uint64
	BLSPublicKey          string
	RemainingBalanceOwner string // P-Chain address for refunds (CB58)
	PChainID              uint32
}

// ConvertSubnetValidator represents a validator from ConvertSubnetToL1Tx JSON
type ConvertSubnetValidator struct {
	NodeID  string `json:"nodeID"`
	Weight  uint64 `json:"weight"`
	Balance uint64 `json:"balance"`
	Signer  struct {
		PublicKey string `json:"publicKey"`
	} `json:"signer"`
	RemainingBalanceOwner struct {
		Threshold uint32   `json:"threshold"`
		Addresses []string `json:"addresses"`
	} `json:"remainingBalanceOwner"`
}

// DiscoverL1ValidatorHistory discovers all L1 validators from historical transactions
func DiscoverL1ValidatorHistory(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]L1ValidatorHistory, error) {
	// Get last processed block for incremental discovery
	var lastProcessedBlock uint64
	err := conn.QueryRow(ctx, `
		SELECT COALESCE(max(created_block), 0) FROM l1_validator_history FINAL WHERE p_chain_id = ?
	`, pchainID).Scan(&lastProcessedBlock)
	if err != nil {
		log.Printf("WARNING: Could not get last processed block for validator history: %v", err)
		lastProcessedBlock = 0
	}

	// Query validators from ConvertSubnetToL1Tx transactions (only new blocks)
	// Fetch the raw validators JSON and parse it in Go for reliable extraction
	query := `
		SELECT
			tx_id,
			block_number,
			block_time,
			toString(tx_data.subnetID) as subnet_id,
			toString(tx_data.validators) as validators_json
		FROM p_chain_txs
		WHERE p_chain_id = ?
		  AND tx_type = 'ConvertSubnetToL1'
		  AND block_number > ?
		ORDER BY block_number ASC
	`

	rows, err := conn.Query(ctx, query, pchainID, lastProcessedBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to query validator history: %w", err)
	}
	defer rows.Close()

	var validators []L1ValidatorHistory
	for rows.Next() {
		var txID, subnetID, validatorsJSON string
		var blockNumber uint64
		var blockTime time.Time

		if err := rows.Scan(&txID, &blockNumber, &blockTime, &subnetID, &validatorsJSON); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Parse the validators JSON array in Go
		// ClickHouse toString() on JSON array produces ['{"json":...}', '{"json":...}'] format
		// We need to convert this to proper JSON array format
		validatorsJSON = convertClickHouseArrayToJSON(validatorsJSON)

		var txValidators []ConvertSubnetValidator
		if err := json.Unmarshal([]byte(validatorsJSON), &txValidators); err != nil {
			log.Printf("WARNING: Failed to parse validators JSON for tx %s: %v (json: %s)", txID, err, validatorsJSON[:min(200, len(validatorsJSON))])
			continue
		}

		// Parse subnet ID for computing validation_id
		subnetIDParsed, err := ids.FromString(subnetID)
		if err != nil {
			log.Printf("WARNING: Failed to parse subnet ID %s for tx %s: %v", subnetID, txID, err)
			continue
		}

		// Create a validator record for each validator in the array
		for i, v := range txValidators {
			// Convert hex node ID to CB58 format
			nodeIDCB58, err := hexToNodeID(v.NodeID)
			if err != nil {
				log.Printf("WARNING: Failed to convert node ID %s to CB58 for tx %s: %v", v.NodeID, txID, err)
				continue
			}

			// Compute validation_id: subnetID.Append(validatorIndex) as per ACP-77
			validationID := subnetIDParsed.Append(uint32(i))

			// Extract remainingBalanceOwner address (first address if available)
			var remainingBalanceOwner string
			if len(v.RemainingBalanceOwner.Addresses) > 0 {
				remainingBalanceOwner = v.RemainingBalanceOwner.Addresses[0]
			}

			validators = append(validators, L1ValidatorHistory{
				SubnetID:              subnetID,
				NodeID:                nodeIDCB58,
				ValidationID:          validationID.String(),
				CreatedTxID:           txID,
				CreatedTxType:         "ConvertSubnetToL1",
				CreatedBlock:          blockNumber,
				CreatedTime:           blockTime,
				InitialBalance:        v.Balance,
				InitialWeight:         v.Weight,
				BLSPublicKey:          v.Signer.PublicKey,
				RemainingBalanceOwner: remainingBalanceOwner,
				PChainID:              pchainID,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Check if we've ever processed RegisterL1Validator transactions
	var registerCount uint64
	_ = conn.QueryRow(ctx, `
		SELECT count(*) FROM l1_validator_history FINAL
		WHERE p_chain_id = ? AND created_tx_type = 'RegisterL1Validator'
	`, pchainID).Scan(&registerCount)

	// If no RegisterL1Validator records exist, process all of them (one-time backfill)
	// Otherwise, only process new blocks
	var registerQuery string
	var registerRows driver.Rows
	if registerCount == 0 {
		log.Printf("No RegisterL1Validator records found, performing full backfill...")
		registerQuery = `
			SELECT
				tx_id,
				block_number,
				block_time,
				toString(tx_data.message) as message,
				toUInt64OrZero(toString(tx_data.balance)) as balance
			FROM p_chain_txs
			WHERE p_chain_id = ?
			  AND tx_type = 'RegisterL1Validator'
			ORDER BY block_number ASC
		`
		registerRows, err = conn.Query(ctx, registerQuery, pchainID)
	} else {
		registerQuery = `
			SELECT
				tx_id,
				block_number,
				block_time,
				toString(tx_data.message) as message,
				toUInt64OrZero(toString(tx_data.balance)) as balance
			FROM p_chain_txs
			WHERE p_chain_id = ?
			  AND tx_type = 'RegisterL1Validator'
			  AND block_number > ?
			ORDER BY block_number ASC
		`
		registerRows, err = conn.Query(ctx, registerQuery, pchainID, lastProcessedBlock)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query RegisterL1Validator txs: %w", err)
	}
	defer registerRows.Close()

	for registerRows.Next() {
		var txID, messageHex string
		var blockNumber, balance uint64
		var blockTime time.Time

		if err := registerRows.Scan(&txID, &blockNumber, &blockTime, &messageHex, &balance); err != nil {
			log.Printf("WARNING: Failed to scan RegisterL1Validator row: %v", err)
			continue
		}

		// Parse the Warp message to extract validator info
		// Remove 0x prefix if present
		messageHex = strings.TrimPrefix(messageHex, "0x")
		messageBytes, err := hex.DecodeString(messageHex)
		if err != nil {
			log.Printf("WARNING: Failed to decode message hex for tx %s: %v", txID, err)
			continue
		}

		// The message contains an unsigned warp message followed by BLS proof of possession.
		// We need to manually extract just the warp message portion.
		// Unsigned warp message format:
		// - 2 bytes: codec version (0x0000)
		// - 4 bytes: network ID
		// - 32 bytes: source chain ID
		// - 4 bytes: payload length
		// - N bytes: payload
		if len(messageBytes) < 42 { // 2 + 4 + 32 + 4 minimum
			log.Printf("WARNING: Message too short for tx %s: %d bytes", txID, len(messageBytes))
			continue
		}

		// Skip codec version (2 bytes), network ID (4 bytes), source chain ID (32 bytes)
		payloadLenOffset := 2 + 4 + 32
		if len(messageBytes) < payloadLenOffset+4 {
			log.Printf("WARNING: Message too short for payload length for tx %s", txID)
			continue
		}

		// Read payload length (big endian uint32)
		payloadLen := uint32(messageBytes[payloadLenOffset])<<24 |
			uint32(messageBytes[payloadLenOffset+1])<<16 |
			uint32(messageBytes[payloadLenOffset+2])<<8 |
			uint32(messageBytes[payloadLenOffset+3])

		payloadStart := payloadLenOffset + 4
		payloadEnd := payloadStart + int(payloadLen)

		if payloadEnd > len(messageBytes) {
			log.Printf("WARNING: Payload extends beyond message for tx %s: need %d, have %d", txID, payloadEnd, len(messageBytes))
			continue
		}

		warpPayload := messageBytes[payloadStart:payloadEnd]

		// The warp payload is an AddressedCall which wraps the actual message
		addressedCall, err := payload.ParseAddressedCall(warpPayload)
		if err != nil {
			log.Printf("WARNING: Failed to parse AddressedCall for tx %s: %v", txID, err)
			continue
		}

		// Parse the inner payload as RegisterL1Validator message
		regMsg, err := message.ParseRegisterL1Validator(addressedCall.Payload)
		if err != nil {
			log.Printf("WARNING: Failed to parse RegisterL1Validator payload for tx %s: %v", txID, err)
			continue
		}

		// Validate NodeID is not empty
		if len(regMsg.NodeID) == 0 {
			log.Printf("WARNING: Empty NodeID in RegisterL1Validator tx %s, skipping", txID)
			continue
		}

		// Convert node ID bytes to CB58 format
		var nodeID ids.NodeID
		if len(regMsg.NodeID) != len(nodeID) {
			log.Printf("WARNING: Invalid NodeID length %d (expected %d) in tx %s, skipping", len(regMsg.NodeID), len(nodeID), txID)
			continue
		}
		copy(nodeID[:], regMsg.NodeID)

		// Validate the resulting NodeID is not all zeros
		var zeroNodeID ids.NodeID
		if nodeID == zeroNodeID {
			log.Printf("WARNING: All-zero NodeID in RegisterL1Validator tx %s, skipping", txID)
			continue
		}

		nodeIDStr := nodeID.String()

		// Compute validation_id using regMsg.ValidationID()
		validationID := regMsg.ValidationID()

		// Extract remainingBalanceOwner address (first address if available)
		var remainingBalanceOwner string
		if len(regMsg.RemainingBalanceOwner.Addresses) > 0 {
			remainingBalanceOwner = regMsg.RemainingBalanceOwner.Addresses[0].String()
		}

		log.Printf("DEBUG: RegisterL1Validator tx %s: NodeID=%s, SubnetID=%s, Weight=%d, ValidationID=%s",
			txID, nodeIDStr, regMsg.SubnetID.String(), regMsg.Weight, validationID.String())

		// Extract validator info
		validators = append(validators, L1ValidatorHistory{
			SubnetID:              regMsg.SubnetID.String(),
			NodeID:                nodeIDStr,
			ValidationID:          validationID.String(),
			CreatedTxID:           txID,
			CreatedTxType:         "RegisterL1Validator",
			CreatedBlock:          blockNumber,
			CreatedTime:           blockTime,
			InitialBalance:        balance,
			InitialWeight:         regMsg.Weight,
			BLSPublicKey:          "0x" + hex.EncodeToString(regMsg.BLSPublicKey[:]),
			RemainingBalanceOwner: remainingBalanceOwner,
			PChainID:              pchainID,
		})
	}

	if err := registerRows.Err(); err != nil {
		return nil, fmt.Errorf("RegisterL1Validator rows error: %w", err)
	}

	return validators, nil
}

// InsertL1ValidatorHistory inserts historical validator records
func InsertL1ValidatorHistory(ctx context.Context, conn clickhouse.Conn, validators []L1ValidatorHistory) error {
	if len(validators) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_validator_history (
		subnet_id, node_id, validation_id, created_tx_id, created_tx_type, created_block, created_time,
		initial_balance, initial_weight, bls_public_key, remaining_balance_owner, p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	for _, v := range validators {
		err = batch.Append(
			v.SubnetID,
			v.NodeID,
			v.ValidationID,
			v.CreatedTxID,
			v.CreatedTxType,
			v.CreatedBlock,
			v.CreatedTime,
			v.InitialBalance,
			v.InitialWeight,
			v.BLSPublicKey,
			v.RemainingBalanceOwner,
			v.PChainID,
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to append validator %s: %w", v.NodeID, err)
		}
	}

	return batch.Send()
}

// L1ValidatorBalanceTx represents a balance-affecting transaction for a validator
type L1ValidatorBalanceTx struct {
	ValidationID string
	TxID         string
	TxType       string
	BlockNumber  uint64
	BlockTime    time.Time
	Amount       uint64
	SubnetID     string
	NodeID       string
	PChainID     uint32
}

// SyncL1ValidatorBalanceTxs syncs all balance-affecting transactions to the l1_validator_balance_txs table
// This includes: ConvertSubnetToL1, RegisterL1Validator, and IncreaseL1ValidatorBalance transactions
func SyncL1ValidatorBalanceTxs(ctx context.Context, conn clickhouse.Conn, pchainID uint32) error {
	// Check last synced block
	var lastSyncedBlock uint64
	err := conn.QueryRow(ctx, `
		SELECT COALESCE(max(block_number), 0) FROM l1_validator_balance_txs WHERE p_chain_id = ?
	`, pchainID).Scan(&lastSyncedBlock)
	if err != nil {
		log.Printf("WARNING: Could not get last synced block for balance txs: %v", err)
		lastSyncedBlock = 0
	}

	var txs []L1ValidatorBalanceTx

	// 1. Sync IncreaseL1ValidatorBalance transactions (top-ups)
	topUpQuery := `
		SELECT
			toString(t.tx_data.validationID) as validation_id,
			t.tx_id,
			'IncreaseL1ValidatorBalance' as tx_type,
			t.block_number,
			t.block_time,
			toUInt64OrZero(toString(t.tx_data.balance)) as amount,
			vs.subnet_id,
			vs.node_id
		FROM p_chain_txs t
		JOIN l1_validator_state vs FINAL ON toString(t.tx_data.validationID) = vs.validation_id AND vs.p_chain_id = t.p_chain_id
		WHERE t.p_chain_id = ?
		  AND t.tx_type = 'IncreaseL1ValidatorBalance'
		  AND t.block_number > ?
		ORDER BY t.block_number ASC
	`

	topUpRows, err := conn.Query(ctx, topUpQuery, pchainID, lastSyncedBlock)
	if err != nil {
		return fmt.Errorf("failed to query IncreaseL1ValidatorBalance txs: %w", err)
	}

	for topUpRows.Next() {
		var tx L1ValidatorBalanceTx
		if err := topUpRows.Scan(
			&tx.ValidationID, &tx.TxID, &tx.TxType, &tx.BlockNumber,
			&tx.BlockTime, &tx.Amount, &tx.SubnetID, &tx.NodeID,
		); err != nil {
			log.Printf("WARNING: Failed to scan top-up tx: %v", err)
			continue
		}
		tx.PChainID = pchainID
		txs = append(txs, tx)
	}
	topUpRows.Close()

	// 2. If this is the first sync (lastSyncedBlock == 0), also sync initial deposits
	if lastSyncedBlock == 0 {
		// Get initial deposits from l1_validator_history
		// Now we have validation_id directly in history table, no need to join with state
		initialQuery := `
			SELECT
				vh.validation_id,
				vh.created_tx_id as tx_id,
				vh.created_tx_type as tx_type,
				vh.created_block as block_number,
				vh.created_time as block_time,
				vh.initial_balance as amount,
				vh.subnet_id,
				vh.node_id
			FROM l1_validator_history vh FINAL
			WHERE vh.p_chain_id = ?
			  AND vh.initial_balance > 0
		`

		initialRows, err := conn.Query(ctx, initialQuery, pchainID)
		if err != nil {
			return fmt.Errorf("failed to query initial deposits: %w", err)
		}

		for initialRows.Next() {
			var tx L1ValidatorBalanceTx
			if err := initialRows.Scan(
				&tx.ValidationID, &tx.TxID, &tx.TxType, &tx.BlockNumber,
				&tx.BlockTime, &tx.Amount, &tx.SubnetID, &tx.NodeID,
			); err != nil {
				log.Printf("WARNING: Failed to scan initial deposit: %v", err)
				continue
			}
			tx.PChainID = pchainID
			txs = append(txs, tx)
		}
		initialRows.Close()
	}

	// Insert all transactions
	if len(txs) > 0 {
		log.Printf("Syncing %d balance transactions to l1_validator_balance_txs", len(txs))
		if err := InsertL1ValidatorBalanceTxs(ctx, conn, txs); err != nil {
			return fmt.Errorf("failed to insert balance txs: %w", err)
		}
	}

	return nil
}

// InsertL1ValidatorBalanceTxs inserts balance transactions into the table
func InsertL1ValidatorBalanceTxs(ctx context.Context, conn clickhouse.Conn, txs []L1ValidatorBalanceTx) error {
	if len(txs) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_validator_balance_txs (
		validation_id, tx_id, tx_type, block_number, block_time, amount,
		subnet_id, node_id, p_chain_id
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, tx := range txs {
		err = batch.Append(
			tx.ValidationID,
			tx.TxID,
			tx.TxType,
			tx.BlockNumber,
			tx.BlockTime,
			tx.Amount,
			tx.SubnetID,
			tx.NodeID,
			tx.PChainID,
		)
		if err != nil {
			return fmt.Errorf("failed to append tx %s: %w", tx.TxID, err)
		}
	}

	return batch.Send()
}

// UpdatePerValidatorFeeStats calculates and updates fee statistics for each L1 validator
func UpdatePerValidatorFeeStats(ctx context.Context, conn clickhouse.Conn, pchainID uint32) error {
	// This query calculates per-validator fee stats and updates l1_validator_state
	// We calculate: initial_deposit, total_topups, refund_amount, fees_paid
	query := `
		WITH
		-- Get initial deposits from history
		initial AS (
			SELECT
				subnet_id,
				node_id,
				initial_balance as initial_deposit
			FROM l1_validator_history FINAL
			WHERE p_chain_id = ?
		),
		-- Get top-ups per validator
		topups AS (
			SELECT
				subnet_id,
				node_id,
				sum(amount) as total_topups
			FROM l1_validator_balance_txs FINAL
			WHERE p_chain_id = ? AND tx_type = 'IncreaseL1ValidatorBalance'
			GROUP BY subnet_id, node_id
		),
		-- Get refunds per validator (use validation_id to join)
		refunds AS (
			SELECT
				validation_id,
				refund_amount
			FROM l1_validator_refunds
			WHERE p_chain_id = ?
		)
		SELECT
			v.subnet_id,
			v.validation_id,
			v.node_id,
			v.balance,
			v.weight,
			v.start_time,
			v.end_time,
			v.uptime_percentage,
			v.active,
			COALESCE(i.initial_deposit, 0) as initial_deposit,
			COALESCE(t.total_topups, 0) as total_topups,
			COALESCE(rf.refund_amount, 0) as refund_amount
		FROM l1_validator_state v FINAL
		LEFT JOIN initial i ON v.subnet_id = i.subnet_id AND v.node_id = i.node_id
		LEFT JOIN topups t ON v.subnet_id = t.subnet_id AND v.node_id = t.node_id
		LEFT JOIN refunds rf ON v.validation_id = rf.validation_id
		WHERE v.p_chain_id = ?
		  AND v.subnet_id != '11111111111111111111111111111111LpoYY'
	`

	rows, err := conn.Query(ctx, query, pchainID, pchainID, pchainID, pchainID)
	if err != nil {
		return fmt.Errorf("failed to query validator fee data: %w", err)
	}
	defer rows.Close()

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_validator_state (
		subnet_id, validation_id, node_id, balance, weight,
		start_time, end_time, uptime_percentage, active, last_updated, p_chain_id,
		initial_deposit, total_topups, refund_amount, fees_paid
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	now := time.Now()
	updateCount := 0

	for rows.Next() {
		var subnetID, validationID, nodeID string
		var balance, weight uint64
		var startTime, endTime time.Time
		var uptime float64
		var active bool
		var initialDeposit, totalTopups, refundAmount uint64

		if err := rows.Scan(
			&subnetID, &validationID, &nodeID, &balance, &weight,
			&startTime, &endTime, &uptime, &active,
			&initialDeposit, &totalTopups, &refundAmount,
		); err != nil {
			log.Printf("WARNING: Failed to scan validator row: %v", err)
			continue
		}

		// Calculate fees paid: deposited - refunded - current balance
		totalDeposited := initialDeposit + totalTopups
		var feesPaid uint64
		if totalDeposited > (refundAmount + balance) {
			feesPaid = totalDeposited - refundAmount - balance
		}

		err = batch.Append(
			subnetID, validationID, nodeID, balance, weight,
			startTime, endTime, uptime, active, now, pchainID,
			initialDeposit, totalTopups, refundAmount, feesPaid,
		)
		if err != nil {
			log.Printf("WARNING: Failed to append validator %s: %v", nodeID, err)
			continue
		}
		updateCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	if updateCount > 0 {
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
		log.Printf("Updated fee stats for %d validators", updateCount)
	}

	return nil
}

// L1ValidatorRefund represents a refund when a validator exits
type L1ValidatorRefund struct {
	TxID          string
	ValidationID  string
	SubnetID      string
	RefundAmount  uint64
	RefundAddress string
	BlockNumber   uint64
	BlockTime     time.Time
	PChainID      uint32
}

// SyncL1ValidatorRefunds syncs refund transactions from DisableL1Validator txs
// It fetches the actual refund amount from UTXOs via RPC
func SyncL1ValidatorRefunds(ctx context.Context, conn clickhouse.Conn, fetcher *pchainrpc.Fetcher, pchainID uint32) error {
	// Check last synced block
	var lastSyncedBlock uint64
	err := conn.QueryRow(ctx, `
		SELECT COALESCE(max(block_number), 0) FROM l1_validator_refunds WHERE p_chain_id = ?
	`, pchainID).Scan(&lastSyncedBlock)
	if err != nil {
		log.Printf("WARNING: Could not get last synced block for refunds: %v", err)
		lastSyncedBlock = 0
	}

	// Query DisableL1Validator transactions
	query := `
		SELECT
			tx_id,
			toString(tx_data.validationID) as validation_id,
			block_number,
			block_time
		FROM p_chain_txs
		WHERE p_chain_id = ?
		  AND tx_type = 'DisableL1Validator'
		  AND block_number > ?
		ORDER BY block_number ASC
	`

	rows, err := conn.Query(ctx, query, pchainID, lastSyncedBlock)
	if err != nil {
		return fmt.Errorf("failed to query DisableL1Validator txs: %w", err)
	}
	defer rows.Close()

	var refunds []L1ValidatorRefund
	for rows.Next() {
		var r L1ValidatorRefund
		if err := rows.Scan(&r.TxID, &r.ValidationID, &r.BlockNumber, &r.BlockTime); err != nil {
			log.Printf("WARNING: Failed to scan refund tx: %v", err)
			continue
		}
		r.PChainID = pchainID
		refunds = append(refunds, r)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("refund rows error: %w", err)
	}

	if len(refunds) == 0 {
		return nil
	}

	// For each refund, fetch the actual refund amount from UTXOs via RPC
	for i := range refunds {
		// Get subnet_id and remaining_balance_owner from l1_validator_history (preferred)
		// This works for disabled validators where RPC fails
		var subnetID, refundAddress string
		err := conn.QueryRow(ctx, `
			SELECT subnet_id, remaining_balance_owner FROM l1_validator_history FINAL
			WHERE validation_id = ? AND p_chain_id = ?
		`, refunds[i].ValidationID, pchainID).Scan(&subnetID, &refundAddress)
		if err != nil || refundAddress == "" {
			// Fall back to l1_validator_state for subnet_id if not found in history
			if subnetID == "" {
				err = conn.QueryRow(ctx, `
					SELECT subnet_id FROM l1_validator_state FINAL
					WHERE validation_id = ? AND p_chain_id = ?
				`, refunds[i].ValidationID, pchainID).Scan(&subnetID)
				if err != nil {
					log.Printf("WARNING: Could not find subnet for validation_id %s: %v", refunds[i].ValidationID, err)
					continue
				}
			}

			// Try to get remainingBalanceOwner from RPC if not in history
			if refundAddress == "" {
				validatorInfo, rpcErr := fetcher.GetL1Validator(ctx, refunds[i].ValidationID)
				if rpcErr != nil {
					log.Printf("WARNING: Could not get L1 validator info for %s (not in history and RPC failed): %v", refunds[i].ValidationID, rpcErr)
					continue
				}

				if len(validatorInfo.RemainingBalanceOwner.Addresses) == 0 {
					log.Printf("WARNING: No remainingBalanceOwner address for validator %s", refunds[i].ValidationID)
					continue
				}
				refundAddress = validatorInfo.RemainingBalanceOwner.Addresses[0]
			}
		}

		refunds[i].SubnetID = subnetID
		refunds[i].RefundAddress = refundAddress

		// Calculate refund from deposits minus fees
		// Formula: refund = deposits - (activeSeconds * 512 nAVAX/sec)
		// This is reliable because fee rate has always been 512 nAVAX/sec (network < 10k validators)
		refundAmount, err := calculateRefundFromDeposits(ctx, conn, refunds[i].ValidationID, pchainID, refunds[i].BlockTime)
		if err != nil {
			log.Printf("WARNING: Could not calculate refund for %s: %v", refunds[i].ValidationID, err)
			continue
		}
		refunds[i].RefundAmount = refundAmount
	}

	// Insert refunds
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_validator_refunds (
		tx_id, validation_id, subnet_id, refund_amount, refund_address, block_number, block_time, p_chain_id
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	insertCount := 0
	for _, r := range refunds {
		if r.SubnetID == "" || r.RefundAmount == 0 {
			continue // Skip if we couldn't find the subnet or refund amount
		}
		err = batch.Append(
			r.TxID,
			r.ValidationID,
			r.SubnetID,
			r.RefundAmount,
			r.RefundAddress,
			r.BlockNumber,
			r.BlockTime,
			r.PChainID,
		)
		if err != nil {
			return fmt.Errorf("failed to append refund %s: %w", r.TxID, err)
		}
		insertCount++
	}

	if insertCount > 0 {
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send refunds batch: %w", err)
		}
		log.Printf("Synced %d validator refunds", insertCount)
	}

	return nil
}

// calculateRefundFromDeposits calculates refund when UTXO has been spent
// Uses: refund = total_deposits - fees_paid (based on time active and min fee rate)
// disableTime is the block_time from the DisableL1Validator transaction
func calculateRefundFromDeposits(ctx context.Context, conn clickhouse.Conn, validationID string, pchainID uint32, disableTime time.Time) (uint64, error) {
	// Find the start time for this validation period
	// If this validator was disabled before, the start time is the previous disable time
	// (when it was re-activated by depositing more funds)
	// Otherwise, use the original creation time
	var startTime time.Time
	var previousDisableTime time.Time

	// Check if there was a previous disable for this validator (before current disableTime)
	// Query p_chain_txs directly since l1_validator_refunds may not have been populated yet
	err := conn.QueryRow(ctx, `
		SELECT MAX(block_time)
		FROM p_chain_txs FINAL
		WHERE tx_type = 'DisableL1Validator'
		  AND p_chain_id = ?
		  AND block_time < ?
		  AND toString(tx_data.validationID) = ?
	`, pchainID, disableTime, validationID).Scan(&previousDisableTime)

	// ClickHouse returns epoch time (1970-01-01) when MAX() finds no rows
	// We need to check if this is a real previous disable or just the epoch default
	epochTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	hasPreviousDisable := err == nil && !previousDisableTime.IsZero() && previousDisableTime.After(epochTime)

	if hasPreviousDisable {
		// This is a subsequent disable - start time is when the validator was re-activated
		// (right after the previous disable)
		startTime = previousDisableTime
	} else {
		// First disable - use original creation time
		err = conn.QueryRow(ctx, `
			SELECT created_time
			FROM l1_validator_history FINAL
			WHERE validation_id = ? AND p_chain_id = ?
		`, validationID, pchainID).Scan(&startTime)
		if err != nil {
			// Fallback to l1_validator_state
			err = conn.QueryRow(ctx, `
				SELECT start_time
				FROM l1_validator_state FINAL
				WHERE validation_id = ? AND p_chain_id = ?
			`, validationID, pchainID).Scan(&startTime)
			if err != nil {
				return 0, fmt.Errorf("failed to get validator start time: %w", err)
			}
		}
	}

	// Get total deposits for this validation period only
	// For first period: include deposits from creation time (>=) through disable time
	// For subsequent periods: include deposits after previous disable (>) through current disable
	var totalDeposits uint64
	if !hasPreviousDisable {
		// First period - include the initial deposit at creation time
		err = conn.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)
			FROM l1_validator_balance_txs FINAL
			WHERE validation_id = ? AND p_chain_id = ?
			  AND block_time >= ? AND block_time <= ?
		`, validationID, pchainID, startTime, disableTime).Scan(&totalDeposits)
	} else {
		// Subsequent period - exclude deposits from before the previous disable
		err = conn.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)
			FROM l1_validator_balance_txs FINAL
			WHERE validation_id = ? AND p_chain_id = ?
			  AND block_time > ? AND block_time <= ?
		`, validationID, pchainID, startTime, disableTime).Scan(&totalDeposits)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get deposits: %w", err)
	}

	// Calculate fees using minimum rate (512 nAVAX/second)
	// This is an approximation - actual rate may be higher if network had >10k validators
	const minFeeRate = 512 // nAVAX per second
	activeSeconds := uint64(disableTime.Sub(startTime).Seconds())
	feesConsumed := activeSeconds * minFeeRate

	if feesConsumed >= totalDeposits {
		return 0, nil // All deposits consumed by fees
	}

	refund := totalDeposits - feesConsumed
	log.Printf("Calculated refund for %s: deposits=%d, start=%s, active=%ds, fees=%d, refund=%d nAVAX (%.6f AVAX)",
		validationID, totalDeposits, startTime.Format("2006-01-02 15:04:05"), activeSeconds, feesConsumed, refund, float64(refund)/1e9)

	return refund, nil
}

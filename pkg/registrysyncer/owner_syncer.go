package registrysyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"icicle/pkg/chwrapper"
)

const ownerSelector = "0x8da5cb5b"                                    // keccak256("owner()")
const cChainRPC = "https://api.avax.network/ext/bc/C/rpc" // C-Chain public RPC fallback

// l1OwnerInfo holds the data needed to fetch and store a validator manager owner
type l1OwnerInfo struct {
	// Full subnet row (needed for ReplacingMergeTree re-insert)
	SubnetID                string
	CreatedBlock            uint64
	CreatedTime             time.Time
	SubnetType              string
	ChainID                 string
	ConvertedBlock          uint64
	ConvertedTime           time.Time
	ValidatorManagerAddress string
	PChainID                uint32
	// From registry
	RpcURL string
}

// SyncValidatorManagerOwners fetches the owner() of each L1's ValidatorManager contract
// and stores it in the subnets table.
func SyncValidatorManagerOwners(ctx context.Context, conn driver.Conn) error {
	slog.Info("Starting validator manager owner sync")
	startTime := time.Now()

	// Get all L1 subnets with a validator manager address and an RPC URL
	// Include full subnet row data for ReplacingMergeTree re-insert
	rows, err := conn.Query(ctx, `
		SELECT
			s.subnet_id, s.created_block, s.created_time, s.subnet_type,
			s.chain_id, s.converted_block, s.converted_time,
			s.validator_manager_address, s.p_chain_id,
			r.rpc_url
		FROM (SELECT * FROM subnets FINAL) s
		INNER JOIN (SELECT * FROM subnet_chains FINAL) sc ON s.subnet_id = sc.subnet_id
		INNER JOIN (SELECT * FROM l1_registry FINAL) r ON sc.chain_id = r.blockchain_id
		WHERE s.validator_manager_address != ''
		  AND r.rpc_url != ''
	`)
	if err != nil {
		return fmt.Errorf("failed to query subnets: %w", err)
	}
	defer rows.Close()

	var l1s []l1OwnerInfo
	for rows.Next() {
		var info l1OwnerInfo
		if err := rows.Scan(
			&info.SubnetID, &info.CreatedBlock, &info.CreatedTime, &info.SubnetType,
			&info.ChainID, &info.ConvertedBlock, &info.ConvertedTime,
			&info.ValidatorManagerAddress, &info.PChainID,
			&info.RpcURL,
		); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		l1s = append(l1s, info)
	}

	if len(l1s) == 0 {
		slog.Info("No L1 subnets with validator manager and RPC URL found")
		return nil
	}

	slog.Info("Fetching validator manager owners", "count", len(l1s))

	httpClient := &http.Client{Timeout: 10 * time.Second}
	var updated int

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO subnets (
		subnet_id, created_block, created_time, subnet_type,
		chain_id, converted_block, converted_time,
		validator_manager_address, validator_manager_owner,
		p_chain_id, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer func() { _ = batch.Abort() }()

	now := time.Now()
	for _, info := range l1s {
		owner, err := fetchOwner(httpClient, info.RpcURL, info.ValidatorManagerAddress)
		if err != nil {
			// Fallback: some L1s deploy their ValidatorManager on C-Chain
			owner, err = fetchOwner(httpClient, cChainRPC, info.ValidatorManagerAddress)
			if err != nil {
				slog.Warn("Failed to fetch owner", "subnet_id", info.SubnetID, "error", err)
				continue
			}
		}

		if err := batch.Append(
			info.SubnetID, info.CreatedBlock, info.CreatedTime, info.SubnetType,
			info.ChainID, info.ConvertedBlock, info.ConvertedTime,
			info.ValidatorManagerAddress, owner,
			info.PChainID, now,
		); err != nil {
			slog.Warn("Failed to append owner", "subnet_id", info.SubnetID, "error", err)
			continue
		}
		updated++
	}

	if updated > 0 {
		if err := chwrapper.RetryableBatchSend(batch); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
	}

	slog.Info("Validator manager owner sync (with RPC)", "updated", updated, "total", len(l1s))

	// Second pass: try C-Chain for subnets without an RPC URL or not in registry
	noRpcRows, err := conn.Query(ctx, `
		SELECT
			subnet_id, created_block, created_time, subnet_type,
			chain_id, converted_block, converted_time,
			validator_manager_address, p_chain_id
		FROM (SELECT * FROM subnets FINAL)
		WHERE validator_manager_address != ''
		  AND validator_manager_owner = ''
	`)
	if err != nil {
		return fmt.Errorf("failed to query subnets without owner: %w", err)
	}
	defer noRpcRows.Close()

	var noRpcL1s []l1OwnerInfo
	for noRpcRows.Next() {
		var info l1OwnerInfo
		if err := noRpcRows.Scan(
			&info.SubnetID, &info.CreatedBlock, &info.CreatedTime, &info.SubnetType,
			&info.ChainID, &info.ConvertedBlock, &info.ConvertedTime,
			&info.ValidatorManagerAddress, &info.PChainID,
		); err != nil {
			return fmt.Errorf("failed to scan no-rpc row: %w", err)
		}
		noRpcL1s = append(noRpcL1s, info)
	}

	if len(noRpcL1s) > 0 {
		slog.Info("Trying C-Chain fallback for remaining subnets", "count", len(noRpcL1s))

		batch2, err := conn.PrepareBatch(ctx, `INSERT INTO subnets (
			subnet_id, created_block, created_time, subnet_type,
			chain_id, converted_block, converted_time,
			validator_manager_address, validator_manager_owner,
			p_chain_id, last_updated
		)`)
		if err != nil {
			return fmt.Errorf("failed to prepare batch: %w", err)
		}
		defer func() { _ = batch2.Abort() }()

		var updated2 int
		for _, info := range noRpcL1s {
			owner, err := fetchOwner(httpClient, cChainRPC, info.ValidatorManagerAddress)
			if err != nil {
				continue // silently skip — contract likely only exists on the L1
			}

			if err := batch2.Append(
				info.SubnetID, info.CreatedBlock, info.CreatedTime, info.SubnetType,
				info.ChainID, info.ConvertedBlock, info.ConvertedTime,
				info.ValidatorManagerAddress, owner,
				info.PChainID, now,
			); err != nil {
				continue
			}
			updated2++
		}

		if updated2 > 0 {
			if err := chwrapper.RetryableBatchSend(batch2); err != nil {
				return fmt.Errorf("failed to send C-Chain fallback batch: %w", err)
			}
		}
		updated += updated2
		slog.Info("C-Chain fallback complete", "updated", updated2, "remaining", len(noRpcL1s)-updated2)
	}

	slog.Info("Validator manager owner sync complete", "total_updated", updated, "duration", time.Since(startTime))
	return nil
}

// fetchOwner calls owner() on a contract via eth_call and returns the 0x-prefixed address
func fetchOwner(client *http.Client, rpcURL, contractAddr string) (string, error) {
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_call",
		"params": []interface{}{
			map[string]string{
				"to":   contractAddr,
				"data": ownerSelector,
			},
			"latest",
		},
		"id": 1,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", err
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return "", err
	}

	if rpcResp.Error != nil {
		return "", fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return decodeAddress(rpcResp.Result)
}

// decodeAddress extracts a 0x-prefixed address from an ABI-encoded 32-byte word
func decodeAddress(hexData string) (string, error) {
	hexData = strings.TrimPrefix(hexData, "0x")
	if len(hexData) != 64 {
		return "", fmt.Errorf("unexpected result length: %d", len(hexData))
	}
	// Address is in the last 40 hex chars (20 bytes) of the 32-byte word
	addr := "0x" + hexData[24:]
	return strings.ToLower(addr), nil
}

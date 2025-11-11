package evmsyncer

import (
	"clickhouse-metrics-poc/pkg/evmrpc"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// AccessListEntry represents an access list entry for EIP-2930 transactions
type AccessListEntry struct {
	Address     string   `json:"address"`
	StorageKeys []string `json:"storageKeys"`
}

// Helper functions for hex string conversion

// hexToUint32 converts a hex string to uint32
func hexToUint32(s string) (uint32, error) {
	if s == "" || s == "0x" {
		return 0, nil
	}
	s = strings.TrimPrefix(s, "0x")
	val, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse hex to uint32 %s: %w", s, err)
	}
	return uint32(val), nil
}

// hexToUint64 converts a hex string to uint64
func hexToUint64(s string) (uint64, error) {
	if s == "" || s == "0x" {
		return 0, nil
	}
	s = strings.TrimPrefix(s, "0x")
	val, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse hex to uint64 %s: %w", s, err)
	}
	return val, nil
}

// hexToUint16 converts a hex string to uint16
func hexToUint16(s string) (uint16, error) {
	if s == "" || s == "0x" {
		return 0, nil
	}
	s = strings.TrimPrefix(s, "0x")
	val, err := strconv.ParseUint(s, 16, 16)
	if err != nil {
		return 0, fmt.Errorf("failed to parse hex to uint16 %s: %w", s, err)
	}
	return uint16(val), nil
}

// hexToUint8 converts a hex string to uint8
func hexToUint8(s string) (uint8, error) {
	if s == "" || s == "0x" {
		return 0, nil
	}
	s = strings.TrimPrefix(s, "0x")
	val, err := strconv.ParseUint(s, 16, 8)
	if err != nil {
		return 0, fmt.Errorf("failed to parse hex to uint8 %s: %w", s, err)
	}
	return uint8(val), nil
}

// hexToBigInt converts a hex string to *big.Int for UInt256
func hexToBigInt(s string) (*big.Int, error) {
	if s == "" || s == "0x" || s == "0x0" {
		return big.NewInt(0), nil
	}
	s = strings.TrimPrefix(s, "0x")
	val, ok := new(big.Int).SetString(s, 16)
	if !ok {
		return nil, fmt.Errorf("failed to parse hex to big.Int: %s", s)
	}
	return val, nil
}

// hexToBytes converts a hex string to byte slice
func hexToBytes(s string) ([]byte, error) {
	if s == "" || s == "0x" {
		return []byte{}, nil
	}
	s = strings.TrimPrefix(s, "0x")
	if len(s)%2 != 0 {
		s = "0" + s // Pad with leading zero if odd length
	}

	bytes := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		val, err := strconv.ParseUint(s[i:i+2], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("failed to parse hex to bytes: %w", err)
		}
		bytes[i/2] = byte(val)
	}
	return bytes, nil
}

// hexToFixedBytes converts hex to fixed-size byte array, padding with zeros if needed
func hexToFixedBytes(s string, size int) ([]byte, error) {
	bytes, err := hexToBytes(s)
	if err != nil {
		return nil, err
	}

	if len(bytes) > size {
		return nil, fmt.Errorf("hex string too long for fixed size %d: got %d bytes", size, len(bytes))
	}

	// Pad with leading zeros if needed
	if len(bytes) < size {
		padded := make([]byte, size)
		copy(padded[size-len(bytes):], bytes)
		return padded, nil
	}

	return bytes, nil
}

// computePriorityFee calculates the priority fee for EIP-1559 transactions
func computePriorityFee(gasPrice, baseFee, maxPriority uint64) uint64 {
	if baseFee == 0 {
		return 0 // Pre-EIP-1559 block
	}

	effectivePriority := gasPrice - baseFee
	if maxPriority > 0 && effectivePriority > maxPriority {
		return maxPriority
	}
	return effectivePriority
}

// InsertBlocks inserts block data into the blocks table
func InsertBlocks(ctx context.Context, conn clickhouse.Conn, chainID uint32, blocks []*evmrpc.NormalizedBlock, maxBlock uint32) error {
	if len(blocks) == 0 {
		return nil
	}

	// Filter out blocks already in the table
	var filteredBlocks []*evmrpc.NormalizedBlock
	for _, b := range blocks {
		blockNum, err := hexToUint32(b.Block.Number)
		if err != nil {
			continue
		}
		if blockNum > maxBlock {
			filteredBlocks = append(filteredBlocks, b)
		}
	}

	if len(filteredBlocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO raw_blocks (
		chain_id, block_number, hash, parent_hash, block_time, miner,
		difficulty, total_difficulty, size, gas_limit, gas_used, base_fee_per_gas,
		block_gas_cost, state_root, transactions_root, receipts_root, extra_data,
		block_extra_data, ext_data_hash, ext_data_gas_used, mix_hash, nonce,
		sha3_uncles, uncles, blob_gas_used, excess_blob_gas, parent_beacon_block_root
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, normalizedBlock := range filteredBlocks {
		block := normalizedBlock.Block

		// Convert block number
		blockNumber, err := hexToUint32(block.Number)
		if err != nil {
			return fmt.Errorf("failed to parse block number: %w", err)
		}

		// Convert timestamp to time.Time
		timestamp, err := hexToUint64(block.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to parse timestamp: %w", err)
		}
		blockTime := time.Unix(int64(timestamp), 0).UTC()

		// Convert hashes
		hash, err := hexToFixedBytes(block.Hash, 32)
		if err != nil {
			return fmt.Errorf("failed to parse block hash: %w", err)
		}

		parentHash, err := hexToFixedBytes(block.ParentHash, 32)
		if err != nil {
			return fmt.Errorf("failed to parse parent hash: %w", err)
		}

		// Convert miner address
		miner, err := hexToFixedBytes(block.Miner, 20)
		if err != nil {
			return fmt.Errorf("failed to parse miner: %w", err)
		}

		// Difficulty (always 1 for PoS)
		difficulty := uint8(1)
		if block.Difficulty != "" && block.Difficulty != "0x1" {
			diffVal, _ := hexToUint64(block.Difficulty)
			if diffVal > 1 {
				difficulty = 2 // Non-standard, but handle it
			}
		}

		// Total difficulty (equals block number on PoS)
		totalDifficulty, err := hexToUint64(block.TotalDifficulty)
		if err != nil {
			totalDifficulty = uint64(blockNumber) // Fallback to block number
		}

		// Size
		size, err := hexToUint32(block.Size)
		if err != nil {
			return fmt.Errorf("failed to parse size: %w", err)
		}

		// Gas fields
		gasLimit, err := hexToUint32(block.GasLimit)
		if err != nil {
			return fmt.Errorf("failed to parse gas limit: %w", err)
		}

		gasUsed, err := hexToUint32(block.GasUsed)
		if err != nil {
			return fmt.Errorf("failed to parse gas used: %w", err)
		}

		baseFeePerGas, _ := hexToUint64(block.BaseFeePerGas) // Can be 0 for pre-EIP-1559
		blockGasCost, _ := hexToUint64(block.BlockGasCost)   // Can be 0

		// Roots
		stateRoot, err := hexToFixedBytes(block.StateRoot, 32)
		if err != nil {
			return fmt.Errorf("failed to parse state root: %w", err)
		}

		transactionsRoot, err := hexToFixedBytes(block.TransactionsRoot, 32)
		if err != nil {
			return fmt.Errorf("failed to parse transactions root: %w", err)
		}

		receiptsRoot, err := hexToFixedBytes(block.ReceiptsRoot, 32)
		if err != nil {
			return fmt.Errorf("failed to parse receipts root: %w", err)
		}

		// Extra data fields
		extraData, _ := hexToBytes(block.ExtraData)
		blockExtraData, _ := hexToBytes(block.BlockExtraData)

		extDataHash, _ := hexToFixedBytes(block.ExtDataHash, 32)
		if len(extDataHash) == 0 {
			extDataHash = make([]byte, 32) // Zero hash if empty
		}

		extDataGasUsed, _ := hexToUint32(block.ExtDataGasUsed)

		// Mix hash and nonce
		mixHash, err := hexToFixedBytes(block.MixHash, 32)
		if err != nil {
			return fmt.Errorf("failed to parse mix hash: %w", err)
		}

		nonceBytes, err := hexToFixedBytes(block.Nonce, 8)
		if err != nil {
			return fmt.Errorf("failed to parse nonce: %w", err)
		}
		nonce := string(nonceBytes) // LowCardinality requires string

		// Uncles
		sha3Uncles, err := hexToFixedBytes(block.Sha3Uncles, 32)
		if err != nil {
			return fmt.Errorf("failed to parse sha3 uncles: %w", err)
		}

		uncles := make([][]byte, len(block.Uncles))
		for i, uncle := range block.Uncles {
			uncleBytes, err := hexToFixedBytes(uncle, 32)
			if err != nil {
				return fmt.Errorf("failed to parse uncle %d: %w", i, err)
			}
			uncles[i] = uncleBytes
		}

		// Blob gas fields (EIP-4844)
		blobGasUsed, _ := hexToUint32(block.BlobGasUsed)
		excessBlobGas, _ := hexToUint64(block.ExcessBlobGas)

		// Parent beacon block root
		parentBeaconRootBytes, _ := hexToFixedBytes(block.ParentBeaconBlockRoot, 32)
		if len(parentBeaconRootBytes) == 0 {
			parentBeaconRootBytes = make([]byte, 32) // Zero hash if empty
		}
		parentBeaconRoot := string(parentBeaconRootBytes) // LowCardinality requires string

		// Append to batch
		err = batch.Append(
			chainID,
			blockNumber,
			hash,
			parentHash,
			blockTime,
			miner,
			difficulty,
			totalDifficulty,
			size,
			gasLimit,
			gasUsed,
			baseFeePerGas,
			blockGasCost,
			stateRoot,
			transactionsRoot,
			receiptsRoot,
			string(extraData),
			string(blockExtraData),
			extDataHash,
			extDataGasUsed,
			mixHash,
			nonce,
			sha3Uncles,
			uncles,
			blobGasUsed,
			excessBlobGas,
			parentBeaconRoot,
		)
		if err != nil {
			return fmt.Errorf("failed to append block %d: %w", blockNumber, err)
		}
	}

	return batch.Send()
}

// InsertTransactions inserts transaction data merged with receipts into the transactions table
func InsertTransactions(ctx context.Context, conn clickhouse.Conn, chainID uint32, blocks []*evmrpc.NormalizedBlock, maxBlock uint32) error {
	if len(blocks) == 0 {
		return nil
	}

	// Filter out blocks already in the table
	var filteredBlocks []*evmrpc.NormalizedBlock
	for _, b := range blocks {
		blockNum, err := hexToUint32(b.Block.Number)
		if err != nil {
			continue
		}
		if blockNum > maxBlock {
			filteredBlocks = append(filteredBlocks, b)
		}
	}

	if len(filteredBlocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO raw_txs (
		chain_id, hash, block_number, block_hash, block_time,
		transaction_index, nonce, from, to, value, gas_limit, gas_price,
		gas_used, success, input, type, max_fee_per_gas, max_priority_fee_per_gas,
		priority_fee_per_gas, base_fee_per_gas, contract_address, access_list
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, normalizedBlock := range filteredBlocks {
		block := normalizedBlock.Block
		receipts := normalizedBlock.Receipts

		// Verify receipts count matches transactions
		if len(receipts) != len(block.Transactions) {
			return fmt.Errorf("receipts count mismatch for block %s: %d receipts, %d transactions",
				block.Number, len(receipts), len(block.Transactions))
		}

		// Parse block data once
		blockNumber, err := hexToUint32(block.Number)
		if err != nil {
			return fmt.Errorf("failed to parse block number: %w", err)
		}

		blockHash, err := hexToFixedBytes(block.Hash, 32)
		if err != nil {
			return fmt.Errorf("failed to parse block hash: %w", err)
		}

		timestamp, err := hexToUint64(block.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to parse timestamp: %w", err)
		}
		blockTime := time.Unix(int64(timestamp), 0).UTC()

		baseFeePerGas, _ := hexToUint64(block.BaseFeePerGas)

		// Process each transaction with its receipt
		for i, tx := range block.Transactions {
			receipt := receipts[i]

			// Transaction hash
			txHash, err := hexToFixedBytes(tx.Hash, 32)
			if err != nil {
				return fmt.Errorf("failed to parse tx hash: %w", err)
			}

			// Transaction index
			txIndex, err := hexToUint16(tx.TransactionIndex)
			if err != nil {
				return fmt.Errorf("failed to parse tx index: %w", err)
			}

			// Nonce
			nonce, err := hexToUint64(tx.Nonce)
			if err != nil {
				return fmt.Errorf("failed to parse nonce: %w", err)
			}

			// From address
			from, err := hexToFixedBytes(tx.From, 20)
			if err != nil {
				return fmt.Errorf("failed to parse from address: %w", err)
			}

			// To address (nullable for contract creation)
			var to any = nil
			if tx.To != "" && tx.To != "0x" {
				toBytes, err := hexToFixedBytes(tx.To, 20)
				if err != nil {
					return fmt.Errorf("failed to parse to address: %w", err)
				}
				to = toBytes
			}

			// Value
			value, err := hexToBigInt(tx.Value)
			if err != nil {
				return fmt.Errorf("failed to parse value: %w", err)
			}

			// Gas limit
			gasLimit, err := hexToUint32(tx.Gas)
			if err != nil {
				return fmt.Errorf("failed to parse gas limit: %w", err)
			}

			// Gas price
			gasPrice, err := hexToUint64(tx.GasPrice)
			if err != nil {
				return fmt.Errorf("failed to parse gas price: %w", err)
			}

			// Gas used (from receipt)
			gasUsed, err := hexToUint32(receipt.GasUsed)
			if err != nil {
				return fmt.Errorf("failed to parse gas used: %w", err)
			}

			// Success status (from receipt)
			success := receipt.Status == "0x1"

			// Input data
			input, err := hexToBytes(tx.Input)
			if err != nil {
				return fmt.Errorf("failed to parse input: %w", err)
			}

			// Transaction type
			txType := uint8(0) // Legacy by default
			if tx.Type != "" {
				txType, err = hexToUint8(tx.Type)
				if err != nil {
					txType = 0 // Fallback to legacy
				}
			}

			// EIP-1559 fields (nullable)
			var maxFeePerGas *uint64
			var maxPriorityFeePerGas *uint64
			var priorityFeePerGas *uint64

			if tx.MaxFeePerGas != "" {
				val, err := hexToUint64(tx.MaxFeePerGas)
				if err == nil {
					maxFeePerGas = &val
				}
			}

			if tx.MaxPriorityFeePerGas != "" {
				val, err := hexToUint64(tx.MaxPriorityFeePerGas)
				if err == nil {
					maxPriorityFeePerGas = &val

					// Calculate priority fee
					priority := computePriorityFee(gasPrice, baseFeePerGas, val)
					priorityFeePerGas = &priority
				}
			}

			// Contract address (from receipt, for contract creation)
			var contractAddr any = nil
			if receipt.ContractAddress != nil && *receipt.ContractAddress != "" && *receipt.ContractAddress != "0x" {
				contractAddrBytes, err := hexToFixedBytes(*receipt.ContractAddress, 20)
				if err == nil {
					contractAddr = contractAddrBytes
				}
			}

			// Access list
			accessList := make([]map[string]interface{}, 0)
			if tx.AccessList != nil {
				// Parse access list from raw JSON
				var accessListEntries []AccessListEntry
				if err := json.Unmarshal(tx.AccessList, &accessListEntries); err == nil {
					for _, entry := range accessListEntries {
						addr, err := hexToFixedBytes(entry.Address, 20)
						if err != nil {
							continue
						}

						storageKeys := make([][]byte, len(entry.StorageKeys))
						for j, key := range entry.StorageKeys {
							keyBytes, err := hexToFixedBytes(key, 32)
							if err != nil {
								continue
							}
							storageKeys[j] = keyBytes
						}

						accessList = append(accessList, map[string]interface{}{
							"address":      addr,
							"storage_keys": storageKeys,
						})
					}
				}
			}

			// Append to batch
			err = batch.Append(
				chainID,
				txHash,
				blockNumber,
				blockHash,
				blockTime,
				txIndex,
				nonce,
				from,
				to,
				value,
				gasLimit,
				gasPrice,
				gasUsed,
				success,
				string(input),
				txType,
				maxFeePerGas,
				maxPriorityFeePerGas,
				priorityFeePerGas,
				baseFeePerGas,
				contractAddr,
				accessList,
			)
			if err != nil {
				return fmt.Errorf("failed to append tx %s: %w", tx.Hash, err)
			}
		}
	}

	return batch.Send()
}

// FlattenedTrace represents a flattened trace with its address path
type FlattenedTrace struct {
	TxHash           string
	BlockNumber      uint32
	BlockTime        time.Time
	TransactionIndex uint16
	TraceAddress     []uint16
	From             []byte
	To               []byte
	Gas              uint32
	GasUsed          uint32
	Value            *big.Int
	Input            []byte
	Output           []byte
	CallType         string
	TxSuccess        bool
	TxFrom           []byte
	TxTo             any
}

// flattenTrace recursively flattens a CallTrace into multiple FlattenedTrace entries
func flattenTrace(trace *evmrpc.CallTrace, txHash string, blockNum uint32, blockTime time.Time,
	txIndex uint16, address []uint16, txSuccess bool, txFrom []byte, txTo any) ([]FlattenedTrace, error) {

	if trace == nil {
		return nil, nil
	}

	// Convert current trace
	from, err := hexToFixedBytes(trace.From, 20)
	if err != nil {
		return nil, fmt.Errorf("failed to parse from: %w", err)
	}

	var to []byte
	if trace.To != "" && trace.To != "0x" {
		to, err = hexToFixedBytes(trace.To, 20)
		if err != nil {
			return nil, fmt.Errorf("failed to parse to: %w", err)
		}
	} else {
		to = make([]byte, 20) // Zero address for CREATE
	}

	gas, err := hexToUint32(trace.Gas)
	if err != nil {
		gas = 0 // Default to 0 if parsing fails
	}

	gasUsed, err := hexToUint32(trace.GasUsed)
	if err != nil {
		gasUsed = 0 // Default to 0 if parsing fails
	}

	value, err := hexToBigInt(trace.Value)
	if err != nil {
		value = big.NewInt(0) // Default to 0 if parsing fails
	}

	input, err := hexToBytes(trace.Input)
	if err != nil {
		input = []byte{} // Empty input if parsing fails
	}

	output, err := hexToBytes(trace.Output)
	if err != nil {
		output = []byte{} // Empty output if parsing fails
	}

	// Determine call type
	callType := strings.ToUpper(trace.Type)
	if callType == "" {
		callType = "CALL" // Default
	}

	// Create flattened trace for current level
	flattened := []FlattenedTrace{
		{
			TxHash:           txHash,
			BlockNumber:      blockNum,
			BlockTime:        blockTime,
			TransactionIndex: txIndex,
			TraceAddress:     append([]uint16{}, address...), // Copy the slice
			From:             from,
			To:               to,
			Gas:              gas,
			GasUsed:          gasUsed,
			Value:            value,
			Input:            input,
			Output:           output,
			CallType:         callType,
			TxSuccess:        txSuccess,
			TxFrom:           txFrom,
			TxTo:             txTo,
		},
	}

	// Recursively process child calls
	for i, call := range trace.Calls {
		childAddress := append(append([]uint16{}, address...), uint16(i))
		childTraces, err := flattenTrace(&call, txHash, blockNum, blockTime, txIndex, childAddress, txSuccess, txFrom, txTo)
		if err != nil {
			return nil, fmt.Errorf("failed to flatten child trace: %w", err)
		}
		flattened = append(flattened, childTraces...)
	}

	return flattened, nil
}

// InsertTraces inserts trace data into the traces table
func InsertTraces(ctx context.Context, conn clickhouse.Conn, chainID uint32, blocks []*evmrpc.NormalizedBlock, maxBlock uint32) error {
	if len(blocks) == 0 {
		return nil
	}

	// Filter out blocks already in the table
	var filteredBlocks []*evmrpc.NormalizedBlock
	for _, b := range blocks {
		blockNum, err := hexToUint32(b.Block.Number)
		if err != nil {
			continue
		}
		if blockNum > maxBlock {
			filteredBlocks = append(filteredBlocks, b)
		}
	}

	if len(filteredBlocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO raw_traces (
		chain_id, tx_hash, block_number, block_time, transaction_index,
		trace_address, from, to, gas, gas_used, value, input, output, call_type, tx_success,
		tx_from, tx_to
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, normalizedBlock := range filteredBlocks {
		block := normalizedBlock.Block

		// Parse block data
		blockNumber, err := hexToUint32(block.Number)
		if err != nil {
			return fmt.Errorf("failed to parse block number: %w", err)
		}

		timestamp, err := hexToUint64(block.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to parse timestamp: %w", err)
		}
		blockTime := time.Unix(int64(timestamp), 0).UTC()

		// Process traces for each transaction
		for i, tx := range block.Transactions {
			txIndex := uint16(i)

			// Check if we have a trace for this transaction
			if i >= len(normalizedBlock.Traces) {
				continue // No trace for this transaction
			}

			traceResult := normalizedBlock.Traces[i]
			if traceResult.Result == nil {
				continue // Nil trace (e.g., precompile)
			}

			// Get transaction success status from receipt
			txSuccess := false
			if i < len(normalizedBlock.Receipts) {
				txSuccess = normalizedBlock.Receipts[i].Status == "0x1"
			}

			// Parse transaction sender and recipient for denormalization
			txFrom, err := hexToFixedBytes(tx.From, 20)
			if err != nil {
				return fmt.Errorf("failed to parse tx from: %w", err)
			}

			var txTo any = nil
			if tx.To != "" && tx.To != "0x" {
				txToBytes, err := hexToFixedBytes(tx.To, 20)
				if err == nil {
					txTo = txToBytes
				}
			}

			// Flatten the trace
			flattenedTraces, err := flattenTrace(traceResult.Result, tx.Hash, blockNumber,
				blockTime, txIndex, []uint16{}, txSuccess, txFrom, txTo)
			if err != nil {
				return fmt.Errorf("failed to flatten trace for tx %s: %w", tx.Hash, err)
			}

			// Insert each flattened trace
			for _, trace := range flattenedTraces {
				txHash, err := hexToFixedBytes(trace.TxHash, 32)
				if err != nil {
					return fmt.Errorf("failed to parse tx hash: %w", err)
				}

				var to any = nil
				if len(trace.To) > 0 {
					to = trace.To
				}

				err = batch.Append(
					chainID,
					txHash,
					trace.BlockNumber,
					trace.BlockTime,
					trace.TransactionIndex,
					trace.TraceAddress,
					trace.From,
					to,
					trace.Gas,
					trace.GasUsed,
					trace.Value,
					string(trace.Input),
					string(trace.Output),
					trace.CallType,
					trace.TxSuccess,
					trace.TxFrom,
					trace.TxTo,
				)
				if err != nil {
					return fmt.Errorf("failed to append trace: %w", err)
				}
			}
		}
	}

	return batch.Send()
}

// InsertLogs inserts log data into the logs table
func InsertLogs(ctx context.Context, conn clickhouse.Conn, chainID uint32, blocks []*evmrpc.NormalizedBlock, maxBlock uint32) error {
	if len(blocks) == 0 {
		return nil
	}

	// Filter out blocks already in the table
	var filteredBlocks []*evmrpc.NormalizedBlock
	for _, b := range blocks {
		blockNum, err := hexToUint32(b.Block.Number)
		if err != nil {
			continue
		}
		if blockNum > maxBlock {
			filteredBlocks = append(filteredBlocks, b)
		}
	}

	if len(filteredBlocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO raw_logs (
		chain_id, address, block_number, block_hash, block_time,
		transaction_hash, transaction_index, log_index, tx_from, tx_to,
		topic0, topic1, topic2, topic3, data, removed
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, normalizedBlock := range filteredBlocks {
		block := normalizedBlock.Block
		receipts := normalizedBlock.Receipts

		// Parse block data
		blockNumber, err := hexToUint32(block.Number)
		if err != nil {
			return fmt.Errorf("failed to parse block number: %w", err)
		}

		blockHash, err := hexToFixedBytes(block.Hash, 32)
		if err != nil {
			return fmt.Errorf("failed to parse block hash: %w", err)
		}

		timestamp, err := hexToUint64(block.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to parse timestamp: %w", err)
		}
		blockTime := time.Unix(int64(timestamp), 0).UTC()

		// Process logs from each transaction
		for i, receipt := range receipts {
			if i >= len(block.Transactions) {
				continue // Safety check
			}

			tx := block.Transactions[i]

			// Parse tx data for denormalization
			txFrom, err := hexToFixedBytes(tx.From, 20)
			if err != nil {
				return fmt.Errorf("failed to parse tx from: %w", err)
			}

			var txTo any = nil
			if tx.To != "" && tx.To != "0x" {
				txToBytes, err := hexToFixedBytes(tx.To, 20)
				if err == nil {
					txTo = txToBytes
				}
			}

			// Process each log in the receipt
			for _, log := range receipt.Logs {
				// Log address
				address, err := hexToFixedBytes(log.Address, 20)
				if err != nil {
					return fmt.Errorf("failed to parse log address: %w", err)
				}

				// Transaction hash
				txHash, err := hexToFixedBytes(log.TransactionHash, 32)
				if err != nil {
					return fmt.Errorf("failed to parse tx hash: %w", err)
				}

				// Transaction index
				txIndex, err := hexToUint16(log.TransactionIndex)
				if err != nil {
					return fmt.Errorf("failed to parse tx index: %w", err)
				}

				// Log index
				logIndex, err := hexToUint32(log.LogIndex)
				if err != nil {
					return fmt.Errorf("failed to parse log index: %w", err)
				}

				// Topics: topic0 is non-nullable (empty for anonymous events), others nullable
				var topic0 []byte
				var topic1, topic2, topic3 any = nil, nil, nil

				if len(log.Topics) > 0 && log.Topics[0] != "" {
					topic0, err = hexToFixedBytes(log.Topics[0], 32)
					if err != nil {
						return fmt.Errorf("failed to parse topic0: %w", err)
					}
				} else {
					topic0 = make([]byte, 32) // Empty for anonymous events
				}

				if len(log.Topics) > 1 && log.Topics[1] != "" {
					topic1Bytes, err := hexToFixedBytes(log.Topics[1], 32)
					if err == nil {
						topic1 = topic1Bytes
					}
				}

				if len(log.Topics) > 2 && log.Topics[2] != "" {
					topic2Bytes, err := hexToFixedBytes(log.Topics[2], 32)
					if err == nil {
						topic2 = topic2Bytes
					}
				}

				if len(log.Topics) > 3 && log.Topics[3] != "" {
					topic3Bytes, err := hexToFixedBytes(log.Topics[3], 32)
					if err == nil {
						topic3 = topic3Bytes
					}
				}

				// Data
				data, err := hexToBytes(log.Data)
				if err != nil {
					data = []byte{} // Empty data if parsing fails
				}

				// Append to batch
				err = batch.Append(
					chainID,
					address,
					blockNumber,
					blockHash,
					blockTime,
					txHash,
					txIndex,
					logIndex,
					txFrom,
					txTo,
					topic0,
					topic1,
					topic2,
					topic3,
					string(data),
					log.Removed,
				)
				if err != nil {
					return fmt.Errorf("failed to append log: %w", err)
				}
			}
		}
	}

	return batch.Send()
}

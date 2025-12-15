package cmd

import (
	"context"
	"fmt"
	"log"

	"icicle/pkg/chwrapper"

	"github.com/fatih/color"
)

func RunDuplicates() {
	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	chainID := uint32(43114)
	allClean := true

	// Check raw_blocks - unique key: (chain_id, block_number)
	fmt.Printf("\nBlocks (Chain %d):\n", chainID)
	fmt.Printf("--------------------------------\n")
	var totalBlocks, duplicateBlocks uint64
	err = conn.QueryRow(ctx, `
		SELECT count() as total, countIf(cnt > 1) as duplicates
		FROM (
			SELECT chain_id, block_number, count() as cnt
			FROM raw_blocks
			WHERE chain_id = ?
			GROUP BY chain_id, block_number
		)
	`, chainID).Scan(&totalBlocks, &duplicateBlocks)
	if err != nil {
		log.Printf("Error querying blocks: %v", err)
	} else {
		fmt.Printf("Total blocks:     %d\n", totalBlocks)
		fmt.Printf("Duplicates:       %d\n", duplicateBlocks)
		if duplicateBlocks == 0 {
			fmt.Printf("%s No duplicates\n", color.GreenString("✓"))
		} else {
			allClean = false
			fmt.Printf("%s Found duplicates!\n", color.RedString("✗"))
		}
	}

	// Check raw_txs - unique key: (chain_id, hash)
	fmt.Printf("\nTransactions (Chain %d):\n", chainID)
	fmt.Printf("--------------------------------\n")
	var totalTxs, duplicateTxs uint64
	err = conn.QueryRow(ctx, `
		SELECT count() as total, countIf(cnt > 1) as duplicates
		FROM (
			SELECT chain_id, hash, count() as cnt
			FROM raw_txs
			WHERE chain_id = ?
			GROUP BY chain_id, hash
		)
	`, chainID).Scan(&totalTxs, &duplicateTxs)
	if err != nil {
		log.Printf("Error querying txs: %v", err)
	} else {
		fmt.Printf("Total txs:        %d\n", totalTxs)
		fmt.Printf("Duplicates:       %d\n", duplicateTxs)
		if duplicateTxs == 0 {
			fmt.Printf("%s No duplicates\n", color.GreenString("✓"))
		} else {
			allClean = false
			fmt.Printf("%s Found duplicates!\n", color.RedString("✗"))
		}
	}

	// Check raw_traces - unique key: (chain_id, block_number, transaction_index, trace_address)
	fmt.Printf("\nTraces (Chain %d):\n", chainID)
	fmt.Printf("--------------------------------\n")
	var totalTraces, duplicateTraces uint64
	err = conn.QueryRow(ctx, `
		SELECT count() as total, countIf(cnt > 1) as duplicates
		FROM (
			SELECT chain_id, block_number, transaction_index, trace_address, count() as cnt
			FROM raw_traces
			WHERE chain_id = ?
			GROUP BY chain_id, block_number, transaction_index, trace_address
		)
	`, chainID).Scan(&totalTraces, &duplicateTraces)
	if err != nil {
		log.Printf("Error querying traces: %v", err)
	} else {
		fmt.Printf("Total traces:     %d\n", totalTraces)
		fmt.Printf("Duplicates:       %d\n", duplicateTraces)
		if duplicateTraces == 0 {
			fmt.Printf("%s No duplicates\n", color.GreenString("✓"))
		} else {
			allClean = false
			fmt.Printf("%s Found duplicates!\n", color.RedString("✗"))
		}
	}

	// Check raw_logs - unique key: (chain_id, transaction_hash, log_index)
	fmt.Printf("\nLogs (Chain %d):\n", chainID)
	fmt.Printf("--------------------------------\n")
	var totalLogs, duplicateLogs uint64
	err = conn.QueryRow(ctx, `
		SELECT count() as total, countIf(cnt > 1) as duplicates
		FROM (
			SELECT chain_id, transaction_hash, log_index, count() as cnt
			FROM raw_logs
			WHERE chain_id = ?
			GROUP BY chain_id, transaction_hash, log_index
		)
	`, chainID).Scan(&totalLogs, &duplicateLogs)
	if err != nil {
		log.Printf("Error querying logs: %v", err)
	} else {
		fmt.Printf("Total logs:       %d\n", totalLogs)
		fmt.Printf("Duplicates:       %d\n", duplicateLogs)
		if duplicateLogs == 0 {
			fmt.Printf("%s No duplicates\n", color.GreenString("✓"))
		} else {
			allClean = false
			fmt.Printf("%s Found duplicates!\n", color.RedString("✗"))
		}
	}

	if allClean {
		fmt.Printf("\n%s All tables are clean - no duplicates found\n", color.GreenString("✓"))
	} else {
		fmt.Printf("\n%s Duplicates detected - data integrity issue!\n", color.RedString("✗"))
	}

	fmt.Println()
}

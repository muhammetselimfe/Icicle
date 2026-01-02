package cmd

import (
	"context"
	"fmt"
	"log"

	"icicle/pkg/chwrapper"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func RunWipe(all bool, chainID uint32, pchain bool) {
	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// If pchain flag is specified, wipe P-chain calculated tables
	if pchain {
		if err := wipePChainTables(conn, all); err != nil {
			log.Fatalf("Failed to wipe P-chain tables: %v", err)
		}
		if all {
			fmt.Println("All P-chain tables wiped successfully (including raw transactions)")
		} else {
			fmt.Println("P-chain calculated tables wiped successfully (validator history, fee stats, subnets)")
		}
		return
	}

	// If chainID is specified, wipe data for that specific chain
	if chainID > 0 {
		if !all {
			log.Fatalf("--chain flag requires --all flag to be set. Use: wipe --all --chain=%d", chainID)
		}
		if err := wipeChainData(conn, chainID); err != nil {
			log.Fatalf("Failed to wipe chain %d data: %v", chainID, err)
		}
		fmt.Printf("All data for chain %d wiped successfully\n", chainID)
		return
	}

	// Otherwise, wipe calculated tables as usual
	if err := wipeCalculatedTables(conn, all); err != nil {
		log.Fatalf("Failed to wipe tables: %v", err)
	}

	if all {
		fmt.Println("All tables dropped successfully")
	} else {
		fmt.Println("Calculated tables dropped successfully")
	}
}

// wipePChainTables wipes P-chain specific calculated tables
// If all is true, also wipes the p_chain_txs table (raw data)
func wipePChainTables(conn driver.Conn, all bool) error {
	ctx := context.Background()

	// P-chain calculated tables (always wiped)
	calculatedTables := []string{
		"l1_validator_history",
		"l1_validator_state",
		"l1_validator_balance_txs",
		"l1_validator_refunds",
		"l1_fee_stats",
		"l1_subnets",
		"l1_registry",
		"subnets",
		"subnet_chains",
		"subnet_hex_map",
	}

	fmt.Println("Wiping P-chain calculated tables...")

	for _, table := range calculatedTables {
		query := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", table)
		fmt.Printf("Truncating %s...\n", table)

		if err := conn.Exec(ctx, query); err != nil {
			// Table might not exist, just log and continue
			fmt.Printf("  Note: %s (may not exist)\n", err)
		}
	}

	// If all flag is set, also wipe raw P-chain transactions and reset sync state
	if all {
		fmt.Println("Wiping P-chain raw transactions...")
		if err := conn.Exec(ctx, "TRUNCATE TABLE IF EXISTS p_chain_txs"); err != nil {
			fmt.Printf("  Note: %s (may not exist)\n", err)
		}

		// Reset P-chain sync watermark (p_chain_id = 0 for mainnet)
		fmt.Println("Resetting P-chain sync watermark...")
		if err := conn.Exec(ctx, "DELETE FROM sync_watermark WHERE chain_id = 0"); err != nil {
			fmt.Printf("  Note: %s (may not exist)\n", err)
		}

		// Reset P-chain chain status
		fmt.Println("Resetting P-chain chain status...")
		if err := conn.Exec(ctx, "ALTER TABLE chain_status DELETE WHERE chain_id = 0"); err != nil {
			fmt.Printf("  Note: %s (may not exist)\n", err)
		}
	}

	return nil
}

func wipeChainData(conn driver.Conn, chainID uint32) error {
	ctx := context.Background()

	tables := []string{
		"raw_blocks",
		"raw_txs",
		"raw_traces",
		"raw_logs",
	}

	fmt.Printf("Wiping data for chain %d...\n", chainID)

	for _, table := range tables {
		query := fmt.Sprintf("ALTER TABLE %s DELETE WHERE chain_id = %d", table, chainID)
		fmt.Printf("Deleting from %s where chain_id = %d...\n", table, chainID)

		if err := conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to delete from %s: %w", table, err)
		}
	}

	// Delete from sync_watermark
	deleteWatermark := fmt.Sprintf("DELETE FROM sync_watermark WHERE chain_id = %d", chainID)
	fmt.Printf("Deleting watermark for chain %d...\n", chainID)
	if err := conn.Exec(ctx, deleteWatermark); err != nil {
		return fmt.Errorf("failed to delete from sync_watermark: %w", err)
	}

	// Delete from chain_status
	deleteStatus := fmt.Sprintf("ALTER TABLE chain_status DELETE WHERE chain_id = %d", chainID)
	fmt.Printf("Deleting chain status for chain %d...\n", chainID)
	if err := conn.Exec(ctx, deleteStatus); err != nil {
		return fmt.Errorf("failed to delete from chain_status: %w", err)
	}

	return nil
}

func wipeCalculatedTables(conn driver.Conn, all bool) error {
	ctx := context.Background()

	query := `
		SELECT name, database 
		FROM system.tables 
		WHERE database = currentDatabase()
		AND engine != 'System'
		ORDER BY engine = 'MaterializedView' DESC, name
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	keepTables := map[string]bool{}

	if !all {
		keepTables["raw_blocks"] = true
		keepTables["raw_txs"] = true
		keepTables["raw_traces"] = true
		keepTables["raw_logs"] = true
		keepTables["p_chain_txs"] = true
		keepTables["sync_watermark"] = true
	}

	var tables []struct {
		name     string
		database string
	}

	for rows.Next() {
		var name, database string
		if err := rows.Scan(&name, &database); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if !keepTables[name] {
			tables = append(tables, struct {
				name     string
				database string
			}{name: name, database: database})
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	if len(tables) == 0 {
		fmt.Println("No calculated tables found to drop")
		return nil
	}

	fmt.Printf("Found %d calculated tables to drop\n", len(tables))

	for _, table := range tables {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` SETTINGS max_table_size_to_drop=0", table.database, table.name)
		fmt.Printf("Dropping %s.%s...\n", table.database, table.name)

		if err := conn.Exec(ctx, dropQuery); err != nil {
			return fmt.Errorf("failed to drop %s.%s: %w", table.database, table.name, err)
		}
	}

	return nil
}

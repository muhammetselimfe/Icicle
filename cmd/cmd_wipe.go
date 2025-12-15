package cmd

import (
	"context"
	"fmt"
	"log"

	"icicle/pkg/chwrapper"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func RunWipe(all bool, chainID uint32) {
	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

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

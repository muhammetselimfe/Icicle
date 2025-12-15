package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"icicle/pkg/chwrapper"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type tableSize struct {
	database      string
	name          string
	rowsThousands float64
	sizeMB        float64
}

type dirSize struct {
	path   string
	sizeMB float64
}

func formatNumber(num float64) string {
	s := fmt.Sprintf("%.2f", num)
	parts := strings.Split(s, ".")
	intPart := parts[0]

	var result strings.Builder
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(c)
	}

	if len(parts) > 1 {
		result.WriteString(".")
		result.WriteString(parts[1])
	}

	return result.String()
}

func RunSize() {
	fmt.Println("=== ClickHouse Table Size ===")
	fmt.Println()

	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	if err := showTableSize(conn); err != nil {
		log.Fatalf("Failed to show table size: %v", err)
	}

	fmt.Println()
	fmt.Println("=== Disk Usage: ./rpc_cache/ ===")
	fmt.Println()
	if err := showRpcCacheSize("./rpc_cache"); err != nil {
		log.Fatalf("Failed to show rpc_cache size: %v", err)
	}
}

func showTableSize(conn driver.Conn) error {
	ctx := context.Background()

	query := `
		SELECT 
			database,
			name,
			total_rows / 1000.0 as rows_thousands,
			total_bytes / (1024.0 * 1024.0) as size_mb
		FROM system.tables
		WHERE database = currentDatabase()
		ORDER BY total_bytes DESC
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []tableSize
	for rows.Next() {
		var t tableSize
		if err := rows.Scan(&t.database, &t.name, &t.rowsThousands, &t.sizeMB); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		tables = append(tables, t)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	if len(tables) == 0 {
		fmt.Println("No tables found")
		return nil
	}

	const maxNameLen = 50
	fmt.Printf("%-*s %15s %15s\n", maxNameLen, "Table", "Rows (K)", "Size (MB)")
	fmt.Println(strings.Repeat("-", maxNameLen+32))

	var totalRows, totalSize float64
	for _, t := range tables {
		name := t.name
		if len(name) > maxNameLen {
			name = name[:maxNameLen-3] + "..."
		}
		fmt.Printf("%-*s %15s %15s\n", maxNameLen, name, formatNumber(t.rowsThousands), formatNumber(t.sizeMB))
		totalRows += t.rowsThousands
		totalSize += t.sizeMB
	}

	fmt.Println(strings.Repeat("-", maxNameLen+32))
	fmt.Printf("%-*s %15s %15s\n", maxNameLen, "TOTAL", formatNumber(totalRows), formatNumber(totalSize))

	return nil
}

func showRpcCacheSize(rootPath string) error {
	info, err := os.Stat(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Directory %s does not exist\n", rootPath)
			return nil
		}
		return fmt.Errorf("failed to stat %s: %w", rootPath, err)
	}

	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", rootPath)
	}

	// Read only top-level entries
	entries, err := os.ReadDir(rootPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	var dirs []dirSize

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		fullPath := filepath.Join(rootPath, entry.Name())
		size, err := calculateDirSize(fullPath)
		if err != nil {
			return fmt.Errorf("failed to calculate size for %s: %w", fullPath, err)
		}

		dirs = append(dirs, dirSize{
			path:   entry.Name(),
			sizeMB: float64(size) / (1024.0 * 1024.0),
		})
	}

	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].sizeMB > dirs[j].sizeMB
	})

	fmt.Printf("%-50s %15s\n", "Directory", "Size (MB)")
	fmt.Println("------------------------------------------------------------------------")

	var totalSize float64
	for _, d := range dirs {
		if d.sizeMB > 0 {
			fmt.Printf("%-50s %15s\n", d.path, formatNumber(d.sizeMB))
			totalSize += d.sizeMB
		}
	}

	fmt.Println("------------------------------------------------------------------------")
	fmt.Printf("%-50s %15s\n", "TOTAL", formatNumber(totalSize))

	return nil
}

func calculateDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

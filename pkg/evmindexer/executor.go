package evmindexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// executeSQLFile reads and executes a SQL file with parameter substitution and binding
func executeSQLFile(conn driver.Conn, sqlDir string, filename string, templateParams []struct{ key, value string }, bindParams map[string]interface{}) error {
	sqlPath := filepath.Join(sqlDir, filename)
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file %s: %w", filename, err)
	}

	// Split by semicolon and execute each statement
	statements := splitSQL(string(sqlBytes))

	for _, stmt := range statements {
		// Skip empty statements
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Replace template placeholders (things like {granularity})
		sql := stmt
		for _, param := range templateParams {
			sql = strings.ReplaceAll(sql, param.key, param.value)
		}

		// Convert bindParams map to clickhouse.Named parameters
		var namedParams []interface{}
		for key, value := range bindParams {
			namedParams = append(namedParams, clickhouse.Named(key, value))
		}

		// Execute statement with parameter binding
		if err := conn.Exec(context.Background(), sql, namedParams...); err != nil {
			// Check if it's a CREATE TABLE that already exists (not an error)
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w\nStatement: %s", err, sql)
			}
		}
	}

	return nil
}

// splitSQL splits SQL content by semicolons, removing comments
func splitSQL(content string) []string {
	lines := strings.Split(content, "\n")
	var cleanLines []string

	for _, line := range lines {
		// Remove comments
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cleanLines = append(cleanLines, line)
	}

	// Join and split by semicolon
	cleaned := strings.Join(cleanLines, "\n")
	statements := strings.Split(cleaned, ";")

	// Filter empty statements
	var result []string
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) != "" {
			result = append(result, stmt)
		}
	}

	return result
}

// capitalize returns string with first letter capitalized
func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// discoverSQLFiles finds all .sql files in a directory
func discoverSQLFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		// Directory might not exist yet (e.g., no immediate indexers)
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var sqlFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			// Remove .sql extension
			name := strings.TrimSuffix(file.Name(), ".sql")
			sqlFiles = append(sqlFiles, name)
		}
	}

	return sqlFiles, nil
}

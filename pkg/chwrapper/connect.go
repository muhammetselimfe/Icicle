package chwrapper

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func Connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "indexer-poc", Version: "0.1"},
				},
			},
			// Connection pool settings for high-throughput sync
			// Validator sync (353 subnets) + EVM sync (4 parallel tables) + P-Chain + indexers
			MaxOpenConns:    100,
			MaxIdleConns:    50,
			DialTimeout:     30 * time.Second, // Wait longer for connection
			ConnMaxLifetime: 1 * time.Hour,    // Recycle connections periodically
			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

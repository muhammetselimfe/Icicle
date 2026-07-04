package chwrapper

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Default pool settings - increased to handle concurrent syncer load
const (
	DefaultMaxOpenConns = 200 // Increased from 100 to handle multiple concurrent syncers
	DefaultMaxIdleConns = 100 // Increased from 50 to reduce connection churn
	DefaultDialTimeout  = 30 * time.Second
	// DefaultReadTimeout bounds a single read from a connection. The library
	// applies it as a per-read SetReadDeadline, so it caps how long a half-open
	// socket can block a goroutine on the read path. Kept above the server-side
	// max_execution_time (60s) so it never pre-empts a legitimately slow query.
	DefaultReadTimeout = 120 * time.Second
	// DefaultWriteTimeout bounds a whole batch insert (PrepareBatch + Send,
	// including the internal Send retries). clickhouse-go only installs a write
	// deadline when the operation's context carries one, so without this an
	// INSERT into a severed connection blocks forever and drains the pool. See
	// WriteContext.
	DefaultWriteTimeout = 120 * time.Second
	// DefaultQueryTimeout bounds a whole read operation (query send + result
	// stream) run through ReadContext. As with writes, clickhouse-go only
	// installs a socket deadline on the query-send path when the operation's
	// context carries one, so a read issued with a deadline-less context
	// (context.Background) can block forever writing the query into a severed
	// connection and leak the pooled conn — the same pool drain WriteContext
	// prevents for inserts (2026-07-04 recurrence, via the token-metadata and
	// incremental read paths). Kept well above the server max_execution_time
	// (60s) so it never pre-empts a legitimately slow analytical read; it is
	// purely a severed-socket backstop.
	DefaultQueryTimeout = 300 * time.Second
)

// getEnvInt reads an integer from environment variable with a default value
func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultVal
}

func Connect() (driver.Conn, error) {
	// Read pool settings from environment variables
	maxOpenConns := getEnvInt("CH_MAX_OPEN_CONNS", DefaultMaxOpenConns)
	maxIdleConns := getEnvInt("CH_MAX_IDLE_CONNS", DefaultMaxIdleConns)
	dialTimeoutSec := getEnvInt("CH_DIAL_TIMEOUT_SEC", int(DefaultDialTimeout.Seconds()))
	readTimeoutSec := getEnvInt("CH_READ_TIMEOUT_SEC", int(DefaultReadTimeout.Seconds()))

	slog.Info("ClickHouse pool config", "max_open_conns", maxOpenConns, "max_idle_conns", maxIdleConns, "dial_timeout_sec", dialTimeoutSec, "read_timeout_sec", readTimeoutSec)

	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
				"max_memory_usage":   uint64(4_000_000_000),
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "indexer-poc", Version: "0.1"},
				},
			},
			// Connection pool settings - configurable via environment variables:
			// CH_MAX_OPEN_CONNS - Maximum open connections (default: 200)
			// CH_MAX_IDLE_CONNS - Maximum idle connections (default: 100)
			// CH_DIAL_TIMEOUT_SEC - Connection timeout in seconds (default: 30)
			// CH_READ_TIMEOUT_SEC - Per-read deadline in seconds (default: 120)
			MaxOpenConns:    maxOpenConns,
			MaxIdleConns:    maxIdleConns,
			DialTimeout:     time.Duration(dialTimeoutSec) * time.Second,
			ReadTimeout:     time.Duration(readTimeoutSec) * time.Second,
			ConnMaxLifetime: 1 * time.Hour, // Recycle connections periodically
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

	slog.Info("ClickHouse connected successfully")
	return conn, nil
}

// WriteContext derives a context with a bounded write deadline from parent.
//
// clickhouse-go installs a socket write deadline only when the operation's
// context carries one (see (*connect).startReadWriteTimeout). A batch INSERT
// run with a deadline-less context (e.g. context.Background) into a connection
// that was severed mid-stream therefore blocks in the TCP write forever, the
// goroutine never returns its connection to the pool, and once every pooled
// connection is parked this way all writes fail with "acquire conn timeout"
// (the 2026-06-11 stall, L1B-51). With a deadline, batch.Send's context
// watchdog closes the dead socket when it fires, the goroutine unblocks, and
// the syncer's retry-on-next-flush path heals automatically.
//
// The timeout is configurable via CH_WRITE_TIMEOUT_SEC (default 120s).
func WriteContext(parent context.Context) (context.Context, context.CancelFunc) {
	sec := getEnvInt("CH_WRITE_TIMEOUT_SEC", int(DefaultWriteTimeout.Seconds()))
	return context.WithTimeout(parent, time.Duration(sec)*time.Second)
}

// ReadContext derives a context with a bounded deadline for read/query
// operations, mirroring WriteContext for the read path. A query issued with a
// deadline-less context can block forever writing the query into a severed
// connection and leak the pooled conn (see DefaultQueryTimeout); with a
// deadline the socket watchdog closes the dead connection and the caller's
// retry heals. Use for every SELECT/Query that runs on a background context.
//
// The timeout is configurable via CH_QUERY_TIMEOUT_SEC (default 300s).
func ReadContext(parent context.Context) (context.Context, context.CancelFunc) {
	sec := getEnvInt("CH_QUERY_TIMEOUT_SEC", int(DefaultQueryTimeout.Seconds()))
	return context.WithTimeout(parent, time.Duration(sec)*time.Second)
}

package evmindexer

import (
	"context"
	_ "embed"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed indexer_tables.sql
var indexerTablesSQL string

// IndexRunner processes indexers for a single chain
type IndexRunner struct {
	chainId uint32
	conn    driver.Conn
	sqlDir  string

	// Block state (updated by OnBlock)
	latestBlockNum  uint64
	latestBlockTime time.Time

	// Watermarks (in-memory cache, backed by DB)
	watermarks map[string]*Watermark

	// Throttling (wall time tracking)
	lastRunTime map[string]time.Time

	// Discovered indexers (loaded once at startup)
	granularMetrics   []string
	batchedIndexers   []string
	immediateIndexers []string
}

// NewIndexRunner creates a new indexer runner for a single chain
func NewIndexRunner(chainId uint32, conn driver.Conn, sqlDir string) (*IndexRunner, error) {
	// Create tables from indexer_tables.sql (metrics and indexer_watermarks)
	// Execute each CREATE TABLE statement
	statements := splitSQL(indexerTablesSQL)
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := conn.Exec(context.Background(), stmt); err != nil {
			// Ignore "already exists" errors
			if !strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("failed to create table from indexer_tables.sql: %w", err)
			}
		}
	}

	runner := &IndexRunner{
		chainId:     chainId,
		conn:        conn,
		sqlDir:      sqlDir,
		watermarks:  make(map[string]*Watermark),
		lastRunTime: make(map[string]time.Time),
	}

	// Discover indexers
	if err := runner.discoverIndexers(); err != nil {
		return nil, fmt.Errorf("failed to discover indexers: %w", err)
	}

	// Load watermarks from DB
	if err := runner.loadWatermarks(); err != nil {
		return nil, fmt.Errorf("failed to load watermarks: %w", err)
	}

	fmt.Printf("[Chain %d] IndexRunner initialized - %d granular metrics, %d batched, %d immediate indexers\n",
		chainId, len(runner.granularMetrics), len(runner.batchedIndexers), len(runner.immediateIndexers))

	return runner, nil
}

// discoverIndexers scans filesystem for SQL files
func (r *IndexRunner) discoverIndexers() error {
	var err error

	// Discover granular metrics
	r.granularMetrics, err = discoverSQLFiles(filepath.Join(r.sqlDir, "evm_metrics"))
	if err != nil {
		return err
	}

	// Discover batched incrementals
	r.batchedIndexers, err = discoverSQLFiles(filepath.Join(r.sqlDir, "evm_incremental/batched"))
	if err != nil {
		return err
	}

	// Discover immediate incrementals
	r.immediateIndexers, err = discoverSQLFiles(filepath.Join(r.sqlDir, "evm_incremental/immediate"))
	if err != nil {
		return err
	}

	return nil
}

// OnBlock updates the runner with latest block information
func (r *IndexRunner) OnBlock(blockNum uint64, blockTime time.Time) {
	r.latestBlockNum = blockNum
	r.latestBlockTime = blockTime
}

// Start begins the indexer loop (runs forever)
func (r *IndexRunner) Start() {
	fmt.Printf("[Chain %d] Starting indexer loop\n", r.chainId)

	for {
		r.processAllIndexers()
		time.Sleep(200 * time.Millisecond)
	}
}

// processAllIndexers checks and runs all indexers
func (r *IndexRunner) processAllIndexers() {
	// Only process if we have block data
	if r.latestBlockNum == 0 {
		return
	}

	// 1. Granular metrics (time-based, period-driven)
	r.processGranularMetrics()

	// 2. Batched incrementals (block-based, 5min throttle)
	r.processBatchedIncrementals()

	// 3. Immediate incrementals (block-based, 0.9s throttle)
	r.processImmediateIncrementals()
}

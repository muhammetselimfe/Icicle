package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

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
	// Create watermark table if not exists
	watermarkSQL := `
	CREATE TABLE IF NOT EXISTS indexer_watermarks (
		chain_id UInt32,
		indexer_name String,
		last_period DateTime64(3, 'UTC'),
		last_block_num UInt64,
		updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (chain_id, indexer_name)`

	if err := conn.Exec(context.Background(), watermarkSQL); err != nil {
		return nil, fmt.Errorf("failed to create watermark table: %w", err)
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
	r.granularMetrics, err = discoverSQLFiles(filepath.Join(r.sqlDir, "metrics"))
	if err != nil {
		return err
	}

	// Discover batched incrementals
	r.batchedIndexers, err = discoverSQLFiles(filepath.Join(r.sqlDir, "incremental/batched"))
	if err != nil {
		return err
	}

	// Discover immediate incrementals
	r.immediateIndexers, err = discoverSQLFiles(filepath.Join(r.sqlDir, "incremental/immediate"))
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


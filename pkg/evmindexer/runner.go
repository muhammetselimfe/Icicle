package evmindexer

import (
	"context"
	_ "embed"
	"fmt"
	"icicle/pkg/chwrapper"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed indexer_tables.sql
var indexerTablesSQL string

// IndexRunner processes indexers for a single chain
type IndexRunner struct {
	chainId    uint32
	conn       driver.Conn
	sqlDir     string
	startBlock uint64 // First block to index (from config)
	rpcURL     string // RPC URL for token metadata fetching

	// Block state (updated by OnBlock)
	mu              sync.RWMutex
	latestBlockNum  uint64
	latestBlockTime time.Time

	// Watermarks (in-memory cache, backed by DB)
	watermarks map[string]*Watermark

	// Discovered indexers (loaded once at startup)
	granularMetrics     []string
	incrementalIndexers []string

	// Token metadata fetcher
	tokenMetadataFetcher *TokenMetadataFetcher
	lastMetadataFetch    time.Time
}

// NewIndexRunner creates a new indexer runner for a single chain
func NewIndexRunner(chainId uint32, conn driver.Conn, sqlDir string, startBlock uint64, rpcURL string) (*IndexRunner, error) {
	// Create tables from indexer_tables.sql (metrics and indexer_watermarks)
	// Execute each CREATE TABLE statement
	statements := splitSQL(indexerTablesSQL)
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		ctx, cancel := chwrapper.WriteContext(context.Background())
		err := conn.Exec(ctx, stmt)
		cancel()
		if err != nil {
			// Ignore "already exists" errors
			if !strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("failed to create table from indexer_tables.sql: %w", err)
			}
		}
	}

	// Create token_metadata table
	tokenMetadataSQL := `
		CREATE TABLE IF NOT EXISTS token_metadata (
			chain_id UInt32,
			token FixedString(20),
			name String,
			symbol String,
			decimals UInt8,
			computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree(computed_at)
		ORDER BY (chain_id, token)
	`
	ctxTM, cancelTM := chwrapper.WriteContext(context.Background())
	errTM := conn.Exec(ctxTM, tokenMetadataSQL)
	cancelTM()
	if errTM != nil {
		if !strings.Contains(errTM.Error(), "already exists") {
			return nil, fmt.Errorf("failed to create token_metadata table: %w", errTM)
		}
	}

	runner := &IndexRunner{
		chainId:    chainId,
		conn:       conn,
		sqlDir:     sqlDir,
		startBlock: startBlock,
		rpcURL:     rpcURL,
		watermarks: make(map[string]*Watermark),
	}

	// Initialize token metadata fetcher if RPC URL provided
	if rpcURL != "" {
		runner.tokenMetadataFetcher = NewTokenMetadataFetcher(chainId, conn, rpcURL)
		slog.Info("Token metadata fetcher initialized", "chain_id", chainId, "rpc_url", rpcURL)
	} else {
		slog.Info("Token metadata fetcher NOT initialized (no RPC URL)", "chain_id", chainId)
	}

	// Discover indexers
	if err := runner.discoverIndexers(); err != nil {
		return nil, fmt.Errorf("failed to discover indexers: %w", err)
	}

	// Load watermarks from DB
	if err := runner.loadWatermarks(); err != nil {
		return nil, fmt.Errorf("failed to load watermarks: %w", err)
	}

	slog.Info("IndexRunner initialized", "chain_id", chainId, "granular_metrics", len(runner.granularMetrics), "incremental_indexers", len(runner.incrementalIndexers))

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

	// Discover incremental indexers
	r.incrementalIndexers, err = discoverSQLFiles(filepath.Join(r.sqlDir, "evm_incremental"))
	if err != nil {
		return err
	}

	return nil
}

// OnBlock updates the runner with latest block information
func (r *IndexRunner) OnBlock(blockNum uint64, blockTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.latestBlockNum = blockNum
	r.latestBlockTime = blockTime
}

func (r *IndexRunner) latestBlock() (uint64, time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.latestBlockNum, r.latestBlockTime
}

// Start begins the indexer loop.
func (r *IndexRunner) Start(ctx context.Context) {
	slog.Info("Starting indexer loop", "chain_id", r.chainId, "token_metadata_fetcher_enabled", r.tokenMetadataFetcher != nil)

	for {
		if err := ctx.Err(); err != nil {
			slog.Info("Stopping indexer loop", "chain_id", r.chainId, "reason", err)
			return
		}

		latestBlockNum, latestBlockTime := r.latestBlock()

		// Only process if we have block data
		if latestBlockNum == 0 {
			if !sleepOrDone(ctx, 100*time.Millisecond) {
				slog.Info("Stopping indexer loop", "chain_id", r.chainId, "reason", ctx.Err())
				return
			}
			continue
		}

		// Process all pending blocks for incremental indexers
		hasWork := r.processIncrementalBatch(latestBlockNum)

		// Process granular metrics (time-based)
		r.processGranularMetrics(latestBlockTime)

		// Fetch token metadata periodically (every 10 seconds)
		if r.tokenMetadataFetcher != nil && time.Since(r.lastMetadataFetch) > 10*time.Second {
			r.lastMetadataFetch = time.Now()
			count, err := r.tokenMetadataFetcher.FetchMissingMetadata(500)
			if err != nil {
				slog.Error("Token metadata fetch error", "chain_id", r.chainId, "error", err)
			} else if count > 0 {
				slog.Info("Fetched token metadata", "chain_id", r.chainId, "count", count)
			} else {
				slog.Debug("Token metadata: no missing tokens found", "chain_id", r.chainId)
			}
		}

		// Sleep only if no incremental work was done
		if !hasWork {
			if !sleepOrDone(ctx, 100*time.Millisecond) {
				slog.Info("Stopping indexer loop", "chain_id", r.chainId, "reason", ctx.Err())
				return
			}
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

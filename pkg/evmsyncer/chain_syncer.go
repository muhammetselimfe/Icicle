package evmsyncer

import (
	"clickhouse-metrics-poc/pkg/cache"
	"clickhouse-metrics-poc/pkg/chwrapper"
	"clickhouse-metrics-poc/pkg/evmindexer"
	"clickhouse-metrics-poc/pkg/evmrpc"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
)

const (
	// BufferSize is the maximum number of batches that can be buffered in the channel
	BufferSize = 200_000
	// FlushInterval is how often to flush blocks to ClickHouse
	FlushInterval = 1 * time.Second
)

// Config holds configuration for ChainSyncer
type Config struct {
	ChainID        uint32
	RpcURL         string
	StartBlock     int64        // Starting block number when no watermark exists, default 68000000
	MaxConcurrency int          // Maximum concurrent RPC and debug requests, default 20
	FetchBatchSize int          // Blocks per fetch, default 100
	CHConn         driver.Conn  // ClickHouse connection
	Cache          *cache.Cache // Cache for RPC calls
	Name           string       // Chain name for display and tracking
}

// ChainSyncer manages blockchain sync for a single chain
type ChainSyncer struct {
	chainId        uint32
	chainName      string
	fetcher        *evmrpc.Fetcher
	conn           driver.Conn
	blockChan      chan []*evmrpc.NormalizedBlock // Bounded channel for backpressure
	watermark      uint32                         // Current sync position
	startBlock     int64                          // Starting block when no watermark
	fetchBatchSize int
	flushInterval  time.Duration

	// Max block numbers in each table (queried once at startup)
	maxBlockBlocks       uint32
	maxBlockTransactions uint32
	maxBlockTraces       uint32
	maxBlockLogs         uint32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Progress tracking
	mu            sync.Mutex
	blocksFetched int64
	blocksWritten int64
	lastPrintTime time.Time
	startTime     time.Time

	// Indexer runner (one per chain)
	indexerRunner *evmindexer.IndexRunner
}

// NewChainSyncer creates a new chain syncer
func NewChainSyncer(cfg Config) (*ChainSyncer, error) {
	if cfg.FetchBatchSize == 0 {
		cfg.FetchBatchSize = 500
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = 20
	}
	if cfg.StartBlock == 0 {
		cfg.StartBlock = 1
	}

	// Create fetcher
	fetcher := evmrpc.NewFetcher(evmrpc.FetcherOptions{
		RpcURL:         cfg.RpcURL,
		MaxConcurrency: cfg.MaxConcurrency,
		MaxRetries:     100,
		RetryDelay:     100 * time.Millisecond,
		BatchSize:      1,
		DebugBatchSize: 1,
		Cache:          cfg.Cache,
	})

	ctx, cancel := context.WithCancel(context.Background())

	cs := &ChainSyncer{
		chainId:        cfg.ChainID,
		chainName:      cfg.Name,
		fetcher:        fetcher,
		conn:           cfg.CHConn,
		blockChan:      make(chan []*evmrpc.NormalizedBlock, BufferSize),
		startBlock:     cfg.StartBlock,
		fetchBatchSize: cfg.FetchBatchSize,
		flushInterval:  FlushInterval,
		ctx:            ctx,
		cancel:         cancel,
		lastPrintTime:  time.Now(),
		startTime:      time.Now(),
	}

	// Initialize indexer runner - one per chain
	indexerRunner, err := evmindexer.NewIndexRunner(cfg.ChainID, cfg.CHConn, "sql")
	if err != nil {
		return nil, fmt.Errorf("failed to create indexer runner: %w", err)
	}
	cs.indexerRunner = indexerRunner
	log.Printf("[Chain %d] Indexer runner initialized", cfg.ChainID)

	return cs, nil
}

// Start begins syncing
func (cs *ChainSyncer) Start() error {
	log.Printf("[Chain %d] Starting syncer...", cs.chainId)

	// Get starting position
	startBlock, err := cs.getStartingBlock()
	if err != nil {
		return fmt.Errorf("failed to determine starting block: %w", err)
	}

	// Query max block for each table once at startup
	cs.maxBlockBlocks, err = chwrapper.GetLatestBlockForChain(cs.conn, "raw_blocks", cs.chainId)
	if err != nil {
		return fmt.Errorf("failed to get max block from blocks table: %w", err)
	}

	cs.maxBlockTransactions, err = chwrapper.GetLatestBlockForChain(cs.conn, "raw_txs", cs.chainId)
	if err != nil {
		return fmt.Errorf("failed to get max block from transactions table: %w", err)
	}

	cs.maxBlockTraces, err = chwrapper.GetLatestBlockForChain(cs.conn, "raw_traces", cs.chainId)
	if err != nil {
		return fmt.Errorf("failed to get max block from traces table: %w", err)
	}

	cs.maxBlockLogs, err = chwrapper.GetLatestBlockForChain(cs.conn, "raw_logs", cs.chainId)
	if err != nil {
		return fmt.Errorf("failed to get max block from logs table: %w", err)
	}

	log.Printf("[Chain %d] Starting from block %d", cs.chainId, startBlock)

	// Get latest block from RPC
	latestBlock, err := cs.fetcher.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	log.Printf("[Chain %d] Latest block on chain: %d", cs.chainId, latestBlock)

	// Initialize chain status in database
	if err := chwrapper.UpsertChainStatus(cs.conn, cs.chainId, cs.chainName, uint64(latestBlock)); err != nil {
		return fmt.Errorf("failed to upsert chain status: %w", err)
	}

	// Start producer (fetcher) goroutine
	cs.wg.Add(1)
	go cs.fetcherLoop(startBlock, latestBlock)

	// Start consumer (writer) goroutine
	cs.wg.Add(1)
	go cs.writerLoop()

	// Start progress printer
	cs.wg.Add(1)
	go cs.printProgress()

	// Start indexer loop
	cs.wg.Add(1)
	go func() {
		defer cs.wg.Done()
		cs.indexerRunner.Start()
	}()

	return nil
}

// Stop gracefully shuts down the syncer
func (cs *ChainSyncer) Stop() {
	log.Printf("[Chain %d] Stopping syncer...", cs.chainId)
	cs.cancel()
	close(cs.blockChan)
	cs.wg.Wait()
	log.Printf("[Chain %d] Syncer stopped", cs.chainId)
}

// Wait blocks until syncer completes
func (cs *ChainSyncer) Wait() {
	cs.wg.Wait()
}

// getStartingBlock determines where to start syncing from
func (cs *ChainSyncer) getStartingBlock() (int64, error) {
	// Get watermark
	watermark, err := chwrapper.GetWatermark(cs.conn, cs.chainId)
	if err != nil {
		return 0, fmt.Errorf("failed to get watermark: %w", err)
	}
	cs.watermark = watermark

	// If no watermark, start from configured start block
	if watermark == 0 {
		return cs.startBlock, nil
	}

	// Start from watermark+1
	return int64(watermark + 1), nil
}

// fetcherLoop is the producer goroutine that fetches blocks
func (cs *ChainSyncer) fetcherLoop(startBlock, latestBlock int64) {
	defer cs.wg.Done()

	currentBlock := startBlock

	for {
		select {
		case <-cs.ctx.Done():
			return
		default:
			// Check if we're caught up
			if currentBlock > latestBlock {
				// Poll for new blocks
				time.Sleep(2 * time.Second)

				newLatest, err := cs.fetcher.GetLatestBlock()
				if err != nil {
					log.Printf("[Chain %d] Error getting latest block: %v", cs.chainId, err)
					continue
				}

				// Update chain status with latest block from RPC
				if err := chwrapper.UpdateLatestBlock(cs.conn, cs.chainId, cs.chainName, uint64(newLatest)); err != nil {
					log.Printf("[Chain %d] Error updating chain status: %v", cs.chainId, err)
				}

				if newLatest > latestBlock {
					latestBlock = newLatest
				} else {
					continue
				}
			}

			// Calculate batch range
			endBlock := currentBlock + int64(cs.fetchBatchSize) - 1
			if endBlock > latestBlock {
				endBlock = latestBlock
			}

			// Fetch blocks
			blocks, err := cs.fetcher.FetchBlockRange(currentBlock, endBlock)
			if err != nil {
				log.Printf("[Chain %d] Error fetching blocks %d-%d: %v",
					cs.chainId, currentBlock, endBlock, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Update fetched counter
			cs.mu.Lock()
			cs.blocksFetched += int64(len(blocks))
			cs.mu.Unlock()

			// Send to channel (will block if buffer is full - backpressure)
			select {
			case cs.blockChan <- blocks:
				currentBlock = endBlock + 1
			case <-cs.ctx.Done():
				return
			}
		}
	}
}

// writerLoop is the consumer goroutine that writes to ClickHouse
func (cs *ChainSyncer) writerLoop() {
	defer cs.wg.Done()

	var buffer []*evmrpc.NormalizedBlock
	var lastFlushTime time.Time
	flushTimer := time.NewTimer(cs.flushInterval)
	defer flushTimer.Stop()

	// flush writes buffered blocks and ensures minimum interval between writes
	flush := func() time.Duration {
		if len(buffer) == 0 {
			return cs.flushInterval
		}

		start := time.Now()
		if err := cs.writeBlocks(buffer); err != nil {
			log.Printf("[Chain %d] Error writing blocks: %v", cs.chainId, err)
			// TODO: Implement retry logic
			return cs.flushInterval
		}

		elapsed := time.Since(start)
		if elapsed > 10*time.Second {
			log.Printf("[Chain %d] WARNING: Write took %v, exceeds 10 second threshold",
				cs.chainId, elapsed)
		}

		// Update counters and clear buffer
		cs.mu.Lock()
		cs.blocksWritten += int64(len(buffer))
		cs.mu.Unlock()
		buffer = nil

		// Calculate next flush time to maintain minimum interval
		lastFlushTime = start
		nextFlush := cs.flushInterval - elapsed
		if nextFlush < 0 {
			nextFlush = 0
		}
		return nextFlush
	}

	for {
		select {
		case <-cs.ctx.Done():
			flush()
			return

		case blocks, ok := <-cs.blockChan:
			if !ok {
				flush()
				return
			}

			buffer = append(buffer, blocks...)

			// Flush immediately if interval has passed
			if !lastFlushTime.IsZero() && time.Since(lastFlushTime) >= cs.flushInterval {
				flushTimer.Stop()
				nextInterval := flush()
				flushTimer.Reset(nextInterval)
			}

		case <-flushTimer.C:
			nextInterval := flush()
			flushTimer.Reset(nextInterval)
		}
	}
}

// writeBlocks writes blocks to all tables in parallel and updates watermark
func (cs *ChainSyncer) writeBlocks(blocks []*evmrpc.NormalizedBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	start := time.Now()

	// Insert to blocks table
	g.Go(func() error {
		return InsertBlocks(ctx, cs.conn, cs.chainId, blocks, cs.maxBlockBlocks)
	})

	// Insert to transactions table
	g.Go(func() error {
		return InsertTransactions(ctx, cs.conn, cs.chainId, blocks, cs.maxBlockTransactions)
	})

	// Insert to traces table
	g.Go(func() error {
		return InsertTraces(ctx, cs.conn, cs.chainId, blocks, cs.maxBlockTraces)
	})

	// Insert to logs table
	g.Go(func() error {
		return InsertLogs(ctx, cs.conn, cs.chainId, blocks, cs.maxBlockLogs)
	})

	// Wait for all inserts to complete
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to insert blocks: %w", err)
	}

	elapsed := time.Since(start)
	txCount := 0
	for _, b := range blocks {
		txCount += len(b.Block.Transactions)
	}
	log.Printf("[Chain %d] Inserted %d blocks and %d txs in %v", cs.chainId, len(blocks), txCount, elapsed)

	// Update watermark to the highest block number in this batch
	maxBlock := uint32(0)
	for _, b := range blocks {
		blockNum, err := hexToUint32(b.Block.Number)
		if err != nil {
			continue
		}
		if blockNum > maxBlock {
			maxBlock = blockNum
		}
	}

	if maxBlock > cs.watermark {
		if err := chwrapper.SetWatermark(cs.conn, cs.chainId, maxBlock); err != nil {
			return fmt.Errorf("failed to update watermark: %w", err)
		}
		cs.watermark = maxBlock
	}

	// Update indexer runner with latest block info (only once per batch)
	if len(blocks) > 0 {
		// Find the latest block by number
		var latestBlock *evmrpc.NormalizedBlock
		latestBlockNum := uint32(0)
		for _, b := range blocks {
			blockNum, err := hexToUint32(b.Block.Number)
			if err != nil {
				continue
			}
			if blockNum > latestBlockNum {
				latestBlockNum = blockNum
				latestBlock = b
			}
		}

		if latestBlock != nil {
			// Convert hex timestamp to uint64
			timestamp, err := hexToUint64(latestBlock.Block.Timestamp)
			if err != nil {
				return fmt.Errorf("failed to parse block timestamp: %w", err)
			}

			// Call OnBlock with block number and timestamp
			blockTime := time.Unix(int64(timestamp), 0).UTC()
			cs.indexerRunner.OnBlock(uint64(latestBlockNum), blockTime)
		}
	}

	return nil
}

// printProgress prints sync progress periodically
func (cs *ChainSyncer) printProgress() {
	defer cs.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
			cs.mu.Lock()
			fetched := cs.blocksFetched
			written := cs.blocksWritten
			cs.mu.Unlock()

			elapsed := time.Since(cs.startTime)
			fetchRate := float64(fetched) / elapsed.Seconds()
			writeRate := float64(written) / elapsed.Seconds()
			lag := fetched - written

			log.Printf("[Chain %d] Fetched: %d (%.1f/s) | Written: %d (%.1f/s) | Lag: %d | Watermark: %d",
				cs.chainId, fetched, fetchRate, written, writeRate, lag, cs.watermark)
		}
	}
}

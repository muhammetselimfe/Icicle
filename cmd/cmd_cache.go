package cmd

import (
	"clickhouse-metrics-poc/pkg/cache"
	"clickhouse-metrics-poc/pkg/evmrpc"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

type CacheConfig struct {
	ChainID        uint32 `json:"chainID"`
	RpcURL         string `json:"rpcURL"`
	StartBlock     int64  `json:"startBlock"`
	EndBlock       int64  `json:"endBlock,omitempty"` // Optional: if not set, cache up to latest
	MaxConcurrency int    `json:"maxConcurrency,omitempty"`
	FetchBatchSize int    `json:"fetchBatchSize,omitempty"`
}

func RunCache() {
	log.Println("Starting cache-only mode (no ClickHouse)...")

	// Load configuration
	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read config.json: %v", err)
	}

	var configs []CacheConfig
	if err := json.Unmarshal(configData, &configs); err != nil {
		log.Fatalf("Failed to parse config.json: %v", err)
	}

	if len(configs) == 0 {
		log.Fatal("No chain configurations found in config.json")
	}

	var wg sync.WaitGroup

	// Start a cacher for each chain
	for _, cfg := range configs {
		wg.Add(1)
		go func(config CacheConfig) {
			defer wg.Done()
			if err := runChainCache(config); err != nil {
				log.Printf("[Chain %d] Cache failed: %v", config.ChainID, err)
			}
		}(cfg)
	}

	wg.Wait()
	log.Println("Cache complete!")
}

func runChainCache(cfg CacheConfig) error {
	// Defaults
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = 100 // Much higher for cache-only mode
	}
	if cfg.FetchBatchSize == 0 {
		cfg.FetchBatchSize = 1000 // Larger batches for max speed
	}

	log.Printf("[Chain %d] Creating cache at ./rpc_cache/%d", cfg.ChainID, cfg.ChainID)
	cacheInstance, err := cache.New("./rpc_cache", cfg.ChainID)
	if err != nil {
		return fmt.Errorf("failed to create cache: %w", err)
	}
	defer cacheInstance.Close()

	// Check for existing checkpoint
	checkpoint, err := cacheInstance.GetCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to read checkpoint: %w", err)
	}

	if checkpoint > 0 {
		log.Printf("[Chain %d] Found checkpoint at block %d, resuming from there", cfg.ChainID, checkpoint)
	}

	log.Printf("[Chain %d] Creating fetcher with concurrency=%d, batchSize=%d",
		cfg.ChainID, cfg.MaxConcurrency, cfg.FetchBatchSize)
	fetcher := evmrpc.NewFetcher(evmrpc.FetcherOptions{
		RpcURL:         cfg.RpcURL,
		MaxConcurrency: cfg.MaxConcurrency,
		MaxRetries:     100,
		RetryDelay:     100 * time.Millisecond,
		BatchSize:      cfg.FetchBatchSize,
		DebugBatchSize: 1,
		Cache:          cacheInstance,
	})

	// Get latest block from RPC
	latestBlock, err := fetcher.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	originalStartBlock := cfg.StartBlock
	if originalStartBlock == 0 {
		originalStartBlock = 1
	}

	startBlock := originalStartBlock
	// Resume from checkpoint if it's ahead
	if checkpoint > startBlock {
		startBlock = checkpoint + 1
	}

	endBlock := cfg.EndBlock
	if endBlock == 0 || endBlock > latestBlock {
		endBlock = latestBlock
	}

	if startBlock > endBlock {
		log.Printf("[Chain %d] Already cached up to block %d (target: %d). Nothing to do!",
			cfg.ChainID, checkpoint, endBlock)
		return nil
	}

	totalBlocks := endBlock - originalStartBlock + 1
	remainingBlocks := endBlock - startBlock + 1
	if checkpoint > 0 {
		log.Printf("[Chain %d] Resuming: caching blocks %d to %d (%d blocks remaining, %d total)",
			cfg.ChainID, startBlock, endBlock, remainingBlocks, totalBlocks)
	} else {
		log.Printf("[Chain %d] Caching blocks %d to %d (%d total blocks)",
			cfg.ChainID, startBlock, endBlock, totalBlocks)
	}

	// Progress tracking
	var blocksCached atomic.Int64
	startTime := time.Now()
	alreadyCached := startBlock - originalStartBlock // blocks already done from previous runs

	// Progress printer
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cached := blocksCached.Load()
				totalCachedSoFar := alreadyCached + cached
				elapsed := time.Since(startTime)
				rate := float64(cached) / elapsed.Seconds()
				progress := float64(totalCachedSoFar) / float64(totalBlocks) * 100

				blocksRemaining := totalBlocks - totalCachedSoFar
				var etaStr string
				if rate > 0 && blocksRemaining > 0 {
					etaSeconds := float64(blocksRemaining) / rate
					etaDuration := time.Duration(etaSeconds * float64(time.Second))
					etaStr = fmt.Sprintf(" | ETA: %s", etaDuration.Round(time.Second))
				}

				log.Printf("[Chain %d] Progress: %s/%s blocks (%.1f%%) | Rate: %.1f blocks/sec | Elapsed: %s%s",
					cfg.ChainID, humanize.Comma(totalCachedSoFar), humanize.Comma(totalBlocks), progress, rate, elapsed.Round(time.Second), etaStr)
			case <-done:
				return
			}
		}
	}()

	// Fetch in parallel chunks
	chunkSize := int64(cfg.FetchBatchSize)
	var fetchWg sync.WaitGroup

	// Limit concurrent fetch operations (each has internal concurrency via MaxConcurrency)
	semaphore := make(chan struct{}, 10)

	// Track highest block cached for checkpoint
	var highestBlockMu sync.Mutex
	highestBlock := startBlock - 1
	checkpointInterval := int64(1000) // Save checkpoint every 10k blocks
	lastCheckpoint := highestBlock

	for current := startBlock; current <= endBlock; {
		batchEnd := current + chunkSize - 1
		if batchEnd > endBlock {
			batchEnd = endBlock
		}

		fetchWg.Add(1)
		semaphore <- struct{}{}

		go func(from, to int64) {
			defer fetchWg.Done()
			defer func() { <-semaphore }()

			blocks, err := fetcher.FetchBlockRange(from, to)
			if err != nil {
				log.Printf("[Chain %d] Error fetching blocks %d-%d: %v", cfg.ChainID, from, to, err)
				return
			}

			blocksCached.Add(int64(len(blocks)))

			// Update highest block and checkpoint if needed
			highestBlockMu.Lock()
			if to > highestBlock {
				highestBlock = to
			}

			// Save checkpoint every 100k blocks
			if highestBlock-lastCheckpoint >= checkpointInterval {
				if err := cacheInstance.SetCheckpoint(highestBlock); err != nil {
					log.Printf("[Chain %d] Failed to save checkpoint at block %d: %v", cfg.ChainID, highestBlock, err)
				} else {
					log.Printf("[Chain %d] Checkpoint saved at block %s", cfg.ChainID, humanize.Comma(int64(highestBlock)))
					lastCheckpoint = highestBlock
				}
			}
			highestBlockMu.Unlock()
		}(current, batchEnd)

		current = batchEnd + 1
	}

	fetchWg.Wait()
	close(done)

	// Save final checkpoint
	if err := cacheInstance.SetCheckpoint(endBlock); err != nil {
		log.Printf("[Chain %d] Failed to save final checkpoint: %v", cfg.ChainID, err)
	} else {
		log.Printf("[Chain %d] Final checkpoint saved at block %d", cfg.ChainID, endBlock)
	}

	elapsed := time.Since(startTime)
	finalCount := blocksCached.Load()
	avgRate := float64(finalCount) / elapsed.Seconds()

	log.Printf("[Chain %d] ✓ Cached %d blocks in %s (avg %.1f blocks/sec)",
		cfg.ChainID, finalCount, elapsed.Round(time.Second), avgRate)

	// Show cache metrics
	log.Printf("[Chain %d] Cache metrics:\n%s", cfg.ChainID, cacheInstance.GetMetrics())

	return nil
}

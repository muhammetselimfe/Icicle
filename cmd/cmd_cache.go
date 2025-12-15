package cmd

import (
	"icicle/pkg/cache"
	"icicle/pkg/evmrpc"
	"icicle/pkg/pchainrpc"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

func RunCache() {
	log.Println("Starting cache-only mode (no ClickHouse)...")

	// Load configuration from YAML
	configs, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(configs) == 0 {
		log.Fatal("No chain configurations found in config.yaml")
	}

	var wg sync.WaitGroup

	// Start a cacher for each chain
	for _, cfg := range configs {
		wg.Add(1)
		go func(chainCfg ChainConfig) {
			defer wg.Done()

			var err error
			switch chainCfg.VM {
			case "evm":
				err = runEVMCache(chainCfg)
			case "p":
				err = runPChainCache(chainCfg)
			default:
				log.Printf("[Chain %d] Unsupported VM type: %s", chainCfg.ChainID, chainCfg.VM)
				return
			}

			if err != nil {
				log.Printf("[Chain %d - %s] Cache failed: %v", chainCfg.ChainID, chainCfg.Name, err)
			}
		}(cfg)
	}

	wg.Wait()
}

func runEVMCache(cfg ChainConfig) error {
	// Defaults
	maxConcurrency := cfg.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = 100
	}
	fetchBatchSize := cfg.FetchBatchSize
	if fetchBatchSize == 0 {
		fetchBatchSize = 1000
	}

	log.Printf("[Chain %d - %s] Creating cache at ./rpc_cache/%d", cfg.ChainID, cfg.Name, cfg.ChainID)
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
		log.Printf("[Chain %d - %s] Found checkpoint at block %d, resuming from there", cfg.ChainID, cfg.Name, checkpoint)
	}

	log.Printf("[Chain %d - %s] Creating fetcher with concurrency=%d, batchSize=%d",
		cfg.ChainID, cfg.Name, maxConcurrency, fetchBatchSize)
	fetcher := evmrpc.NewFetcher(evmrpc.FetcherOptions{
		RpcURL:         cfg.RpcURL,
		ChainID:        cfg.ChainID,
		ChainName:      cfg.Name,
		MaxConcurrency: maxConcurrency,
		MaxRetries:     100,
		RetryDelay:     100 * time.Millisecond,
		BatchSize:      fetchBatchSize,
		DebugBatchSize: 1,
		Cache:          cacheInstance,
	})
	defer fetcher.Close()

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

	// For cache mode, always cache up to the latest block
	endBlock := latestBlock

	if startBlock > endBlock {
		log.Printf("[Chain %d - %s] Already caught up to block %d, nothing to do",
			cfg.ChainID, cfg.Name, endBlock)
		select {} // Block forever
	}

	totalBlocks := endBlock - originalStartBlock + 1
	remainingBlocks := endBlock - startBlock + 1
	if checkpoint > 0 {
		log.Printf("[Chain %d - %s] Resuming: caching blocks %d to %d (%s blocks remaining, %s total)",
			cfg.ChainID, cfg.Name, startBlock, endBlock, humanize.Comma(remainingBlocks), humanize.Comma(totalBlocks))
	} else {
		log.Printf("[Chain %d - %s] Caching blocks %d to %d (%s total blocks)",
			cfg.ChainID, cfg.Name, startBlock, endBlock, humanize.Comma(totalBlocks))
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

				log.Printf("[Chain %d - %s] Progress: %s/%s blocks (%.1f%%) | Rate: %.1f blocks/sec | Elapsed: %s%s",
					cfg.ChainID, cfg.Name, humanize.Comma(totalCachedSoFar), humanize.Comma(totalBlocks), progress, rate, elapsed.Round(time.Second), etaStr)
			case <-done:
				return
			}
		}
	}()

	// Fetch in parallel chunks
	chunkSize := int64(fetchBatchSize)
	var fetchWg sync.WaitGroup

	// Limit concurrent fetch operations (each has internal concurrency via MaxConcurrency)
	semaphore := make(chan struct{}, 10)

	// Track highest block cached for checkpoint
	var highestBlockMu sync.Mutex
	highestBlock := startBlock - 1
	checkpointInterval := int64(1000) // Save checkpoint every 1k blocks
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
				log.Printf("[Chain %d - %s] Error fetching blocks %d-%d: %v", cfg.ChainID, cfg.Name, from, to, err)
				return
			}

			blocksCached.Add(int64(len(blocks)))

			// Update highest block and checkpoint if needed
			highestBlockMu.Lock()
			if to > highestBlock {
				highestBlock = to
			}

			// Save checkpoint every interval
			if highestBlock-lastCheckpoint >= checkpointInterval {
				if err := cacheInstance.SetCheckpoint(highestBlock); err != nil {
					log.Printf("[Chain %d - %s] Failed to save checkpoint at block %d: %v", cfg.ChainID, cfg.Name, highestBlock, err)
				} else {
					log.Printf("[Chain %d - %s] Checkpoint saved at block %s", cfg.ChainID, cfg.Name, humanize.Comma(int64(highestBlock)))
					lastCheckpoint = highestBlock
				}
			}
			highestBlockMu.Unlock()
		}(current, batchEnd)

		current = batchEnd + 1
	}

	fetchWg.Wait()
	close(done)

	// Save checkpoint after initial sync
	if err := cacheInstance.SetCheckpoint(endBlock); err != nil {
		log.Printf("[Chain %d - %s] Failed to save checkpoint: %v", cfg.ChainID, cfg.Name, err)
	} else {
		log.Printf("[Chain %d - %s] Checkpoint saved at block %d", cfg.ChainID, cfg.Name, endBlock)
	}

	elapsed := time.Since(startTime)
	finalCount := blocksCached.Load()
	avgRate := float64(finalCount) / elapsed.Seconds()

	log.Printf("[Chain %d - %s] ✓ Initial sync complete: cached %d blocks in %s (avg %.1f blocks/sec)",
		cfg.ChainID, cfg.Name, finalCount, elapsed.Round(time.Second), avgRate)

	// Show cache metrics
	log.Printf("[Chain %d - %s] Cache metrics:\n%s", cfg.ChainID, cfg.Name, cacheInstance.GetMetrics())

	// Done caching, block forever
	log.Printf("[Chain %d - %s] ✓ Cache complete, nothing more to do", cfg.ChainID, cfg.Name)
	select {} // Block forever
}

func runPChainCache(cfg ChainConfig) error {
	// Defaults
	maxConcurrency := cfg.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = 100
	}
	fetchBatchSize := cfg.FetchBatchSize
	if fetchBatchSize == 0 {
		fetchBatchSize = 1000
	}

	log.Printf("[Chain %d - %s] Creating cache at ./rpc_cache/%d", cfg.ChainID, cfg.Name, cfg.ChainID)
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
		log.Printf("[Chain %d - %s] Found checkpoint at block %d, resuming from there", cfg.ChainID, cfg.Name, checkpoint)
	}

	log.Printf("[Chain %d - %s] Creating fetcher with concurrency=%d, batchSize=%d",
		cfg.ChainID, cfg.Name, maxConcurrency, fetchBatchSize)
	fetcher := pchainrpc.NewFetcher(pchainrpc.FetcherOptions{
		RpcURL:         cfg.RpcURL,
		MaxConcurrency: maxConcurrency,
		MaxRetries:     100,
		RetryDelay:     100 * time.Millisecond,
		BatchSize:      fetchBatchSize,
		Cache:          cacheInstance,
	})
	defer fetcher.Close()

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

	// For cache mode, always cache up to the latest block
	endBlock := latestBlock

	if startBlock > endBlock {
		log.Printf("[Chain %d - %s] Already caught up to block %d, nothing to do",
			cfg.ChainID, cfg.Name, endBlock)
		select {} // Block forever
	}

	totalBlocks := endBlock - originalStartBlock + 1
	remainingBlocks := endBlock - startBlock + 1
	if checkpoint > 0 {
		log.Printf("[Chain %d - %s] Resuming: caching blocks %d to %d (%s blocks remaining, %s total)",
			cfg.ChainID, cfg.Name, startBlock, endBlock, humanize.Comma(remainingBlocks), humanize.Comma(totalBlocks))
	} else {
		log.Printf("[Chain %d - %s] Caching blocks %d to %d (%s total blocks)",
			cfg.ChainID, cfg.Name, startBlock, endBlock, humanize.Comma(totalBlocks))
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

				log.Printf("[Chain %d - %s] Progress: %s/%s blocks (%.1f%%) | Rate: %.1f blocks/sec | Elapsed: %s%s",
					cfg.ChainID, cfg.Name, humanize.Comma(totalCachedSoFar), humanize.Comma(totalBlocks), progress, rate, elapsed.Round(time.Second), etaStr)
			case <-done:
				return
			}
		}
	}()

	// Fetch in parallel chunks
	chunkSize := int64(fetchBatchSize)
	var fetchWg sync.WaitGroup

	// Limit concurrent fetch operations (each has internal concurrency via MaxConcurrency)
	semaphore := make(chan struct{}, 10)

	// Track highest block cached for checkpoint
	var highestBlockMu sync.Mutex
	highestBlock := startBlock - 1
	checkpointInterval := int64(100000) // Save checkpoint every 10k blocks
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
				log.Printf("[Chain %d - %s] Error fetching blocks %d-%d: %v", cfg.ChainID, cfg.Name, from, to, err)
				return
			}

			blocksCached.Add(int64(len(blocks)))

			// Update highest block and checkpoint if needed
			highestBlockMu.Lock()
			if to > highestBlock {
				highestBlock = to
			}

			// Save checkpoint every interval
			if highestBlock-lastCheckpoint >= checkpointInterval {
				if err := cacheInstance.SetCheckpoint(highestBlock); err != nil {
					log.Printf("[Chain %d - %s] Failed to save checkpoint at block %d: %v", cfg.ChainID, cfg.Name, highestBlock, err)
				} else {
					log.Printf("[Chain %d - %s] Checkpoint saved at block %s", cfg.ChainID, cfg.Name, humanize.Comma(int64(highestBlock)))
					lastCheckpoint = highestBlock
				}
			}
			highestBlockMu.Unlock()
		}(current, batchEnd)

		current = batchEnd + 1
	}

	fetchWg.Wait()
	close(done)

	// Save checkpoint after initial sync
	if err := cacheInstance.SetCheckpoint(endBlock); err != nil {
		log.Printf("[Chain %d - %s] Failed to save checkpoint: %v", cfg.ChainID, cfg.Name, err)
	} else {
		log.Printf("[Chain %d - %s] Checkpoint saved at block %d", cfg.ChainID, cfg.Name, endBlock)
	}

	elapsed := time.Since(startTime)
	finalCount := blocksCached.Load()
	avgRate := float64(finalCount) / elapsed.Seconds()

	log.Printf("[Chain %d - %s] ✓ Initial sync complete: cached %d blocks in %s (avg %.1f blocks/sec)",
		cfg.ChainID, cfg.Name, finalCount, elapsed.Round(time.Second), avgRate)

	// Show cache metrics
	log.Printf("[Chain %d - %s] Cache metrics:\n%s", cfg.ChainID, cfg.Name, cacheInstance.GetMetrics())

	// Done caching, block forever
	log.Printf("[Chain %d - %s] ✓ Cache complete, nothing more to do", cfg.ChainID, cfg.Name)
	select {} // Block forever
}

package indexer

import (
	"fmt"
	"time"
)

const (
	batchedMinInterval   = 5 * time.Minute
	immediateMinInterval = 900 * time.Millisecond
)

// processBatchedIncrementals checks and runs batched incremental indexers
func (r *IndexRunner) processBatchedIncrementals() {
	for _, indexerFile := range r.batchedIndexers {
		indexerName := fmt.Sprintf("incremental/batched/%s", indexerFile)

		// Check throttle
		if !r.shouldRun(indexerName, batchedMinInterval) {
			continue
		}

		watermark := r.getWatermark(indexerName)
		
		// Initialize to block 1 if never run
		if watermark.LastBlockNum == 0 {
			watermark.LastBlockNum = 1
		}

		// Check if we have new blocks
		if r.latestBlockNum <= watermark.LastBlockNum {
			continue
		}

		// Run indexer
		fmt.Printf("[Chain %d] Running %s - blocks %d to %d\n", 
			r.chainId, indexerName, watermark.LastBlockNum+1, r.latestBlockNum)
		
		if err := r.runIncrementalIndexer(indexerFile, "batched", watermark.LastBlockNum+1, r.latestBlockNum); err != nil {
			fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
			panic(err)
		}

		// Update watermark and last run time
		watermark.LastBlockNum = r.latestBlockNum
		if err := r.saveWatermark(indexerName, watermark); err != nil {
			fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
			panic(err)
		}
		r.lastRunTime[indexerName] = time.Now()
	}
}

// processImmediateIncrementals checks and runs immediate incremental indexers
func (r *IndexRunner) processImmediateIncrementals() {
	for _, indexerFile := range r.immediateIndexers {
		indexerName := fmt.Sprintf("incremental/immediate/%s", indexerFile)

		// Check throttle
		if !r.shouldRun(indexerName, immediateMinInterval) {
			continue
		}

		watermark := r.getWatermark(indexerName)
		
		// Initialize to block 1 if never run
		if watermark.LastBlockNum == 0 {
			watermark.LastBlockNum = 1
		}

		// Check if we have new blocks
		if r.latestBlockNum <= watermark.LastBlockNum {
			continue
		}

		// Run indexer
		fmt.Printf("[Chain %d] Running %s - blocks %d to %d\n", 
			r.chainId, indexerName, watermark.LastBlockNum+1, r.latestBlockNum)
		
		if err := r.runIncrementalIndexer(indexerFile, "immediate", watermark.LastBlockNum+1, r.latestBlockNum); err != nil {
			fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
			panic(err)
		}

		// Update watermark and last run time
		watermark.LastBlockNum = r.latestBlockNum
		if err := r.saveWatermark(indexerName, watermark); err != nil {
			fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
			panic(err)
		}
		r.lastRunTime[indexerName] = time.Now()
	}
}

// runIncrementalIndexer executes an incremental indexer for given block range
func (r *IndexRunner) runIncrementalIndexer(indexerFile string, indexerType string, firstBlock, lastBlock uint64) error {
	params := []struct{ key, value string }{
		{"{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId)},
		{"{first_block:UInt64}", fmt.Sprintf("%d", firstBlock)},
		{"{last_block:UInt64}", fmt.Sprintf("%d", lastBlock)},
	}

	filename := fmt.Sprintf("incremental/%s/%s.sql", indexerType, indexerFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, params)
}

// shouldRun checks if enough time has passed since last run
func (r *IndexRunner) shouldRun(indexerName string, minInterval time.Duration) bool {
	lastRun, exists := r.lastRunTime[indexerName]
	if !exists {
		return true
	}
	return time.Since(lastRun) >= minInterval
}


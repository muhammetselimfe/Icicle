package evmindexer

import (
	"fmt"
	"time"
)

// IncrementalBatchSize is the maximum number of blocks to process per batch
// This prevents memory exhaustion when processing large block ranges with lots of events
const IncrementalBatchSize = 20000

// processIncrementalBatch processes pending blocks for all incremental indexers in batches
// Processes up to IncrementalBatchSize blocks per indexer per call
// Returns true if any work was done
func (r *IndexRunner) processIncrementalBatch() bool {
	hasWork := false

	// Process each indexer independently
	for _, indexerFile := range r.incrementalIndexers {
		indexerName := fmt.Sprintf("incremental/%s", indexerFile)
		watermark := r.getWatermark(indexerName)

		// Initialize watermark to startBlock-1 if never run (so first processed block is startBlock)
		if watermark.LastBlockNum == 0 {
			watermark.LastBlockNum = r.startBlock - 1
		}

		// Check if there are blocks to process
		if watermark.LastBlockNum < r.latestBlockNum {
			fromBlock := watermark.LastBlockNum + 1
			toBlock := r.latestBlockNum

			// Limit batch size to prevent memory exhaustion
			if toBlock-fromBlock+1 > IncrementalBatchSize {
				toBlock = fromBlock + IncrementalBatchSize - 1
			}

			// Run indexer for the batch
			start := time.Now()
			if err := r.runIncrementalIndexer(indexerFile, fromBlock, toBlock); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}
			elapsed := time.Since(start)

			// Update watermark to the last processed block
			watermark.LastBlockNum = toBlock

			// Save watermark to DB
			if err := r.saveWatermark(indexerName, watermark); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}

			// Log the batch processing
			blockCount := toBlock - fromBlock + 1
			remainingBlocks := r.latestBlockNum - toBlock
			fmt.Printf("[Chain %d] %s - processed blocks %d to %d (%d blocks, %d remaining) - %s\n",
				r.chainId, indexerName, fromBlock, toBlock, blockCount, remainingBlocks, elapsed)

			hasWork = true
		}
	}

	return hasWork
}

// runIncrementalIndexer executes an incremental indexer for a block range
func (r *IndexRunner) runIncrementalIndexer(indexerFile string, fromBlock, toBlock uint64) error {
	// Template parameters (string replacement for SELECT clauses)
	templateParams := []struct{ key, value string }{
		{"{chain_id}", fmt.Sprintf("%d", r.chainId)},
	}

	// Bind parameters (native ClickHouse parameter binding for WHERE clauses)
	bindParams := map[string]interface{}{
		"chain_id":   r.chainId,
		"from_block": fromBlock,
		"to_block":   toBlock,
	}

	filename := fmt.Sprintf("evm_incremental/%s.sql", indexerFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, templateParams, bindParams)
}

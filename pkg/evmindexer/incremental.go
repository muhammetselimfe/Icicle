package evmindexer

import (
	"fmt"
	"time"
)

// processIncrementalBatch processes all pending blocks for all incremental indexers
// Processes block-by-block: block 1 for all indexers, then block 2 for all indexers, etc.
// Returns true if any work was done
func (r *IndexRunner) processIncrementalBatch() bool {
	hasWork := false

	// Find the minimum watermark across all indexers
	minWatermark := r.latestBlockNum
	for _, indexerFile := range r.incrementalIndexers {
		indexerName := fmt.Sprintf("incremental/%s", indexerFile)
		watermark := r.getWatermark(indexerName)

		// Initialize watermark to startBlock-1 if never run (so first processed block is startBlock)
		if watermark.LastBlockNum == 0 {
			watermark.LastBlockNum = r.startBlock - 1
		}

		if watermark.LastBlockNum < minWatermark {
			minWatermark = watermark.LastBlockNum
		}
	}

	// Process one block at a time across all indexers
	if minWatermark < r.latestBlockNum {
		blockNum := minWatermark + 1

		for _, indexerFile := range r.incrementalIndexers {
			indexerName := fmt.Sprintf("incremental/%s", indexerFile)
			watermark := r.getWatermark(indexerName)

			// Skip if this indexer is ahead
			if watermark.LastBlockNum >= blockNum {
				continue
			}

			// Run indexer for this block
			start := time.Now()
			if err := r.runIncrementalIndexer(indexerFile, blockNum); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}
			elapsed := time.Since(start)

			// Update watermark in memory
			watermark.LastBlockNum = blockNum

			// Save watermark to DB only if >1s since last save OR if caught up
			lastSave := r.lastWatermarkSave[indexerName]
			isCaughtUp := blockNum >= r.latestBlockNum
			shouldSave := time.Since(lastSave) > time.Second || isCaughtUp

			if shouldSave {
				if err := r.saveWatermark(indexerName, watermark); err != nil {
					fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
					panic(err)
				}
				r.lastWatermarkSave[indexerName] = time.Now()
			}

			// Only log if slow (>100ms) or every 1000 blocks
			if elapsed > 100*time.Millisecond || blockNum%1000 == 0 {
				fmt.Printf("[Chain %d] %s - block %d - %s\n",
					r.chainId, indexerName, blockNum, elapsed)
			}
		}

		hasWork = true
	}

	return hasWork
}

// runIncrementalIndexer executes an incremental indexer for a single block
func (r *IndexRunner) runIncrementalIndexer(indexerFile string, blockNum uint64) error {
	// Template parameters (string replacement for SELECT clauses)
	templateParams := []struct{ key, value string }{
		{"{chain_id}", fmt.Sprintf("%d", r.chainId)},
	}

	// Bind parameters (native ClickHouse parameter binding for WHERE clauses)
	bindParams := map[string]interface{}{
		"chain_id":     r.chainId,
		"block_number": blockNum,
	}

	filename := fmt.Sprintf("evm_incremental/%s.sql", indexerFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, templateParams, bindParams)
}

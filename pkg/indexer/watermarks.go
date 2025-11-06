package indexer

import (
	"context"
	"fmt"
	"time"
)

// Watermark holds progress for an indexer
type Watermark struct {
	// For granular metrics
	LastPeriod time.Time

	// For incremental indexers
	LastBlockNum uint64
}

// loadWatermarks loads all watermarks for this chain from DB into memory
func (r *IndexRunner) loadWatermarks() error {
	ctx := context.Background()
	query := `
	SELECT indexer_name, last_period, last_block_num
	FROM indexer_watermarks FINAL
	WHERE chain_id = ?`

	rows, err := r.conn.Query(ctx, query, r.chainId)
	if err != nil {
		return fmt.Errorf("failed to query watermarks: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		var period time.Time
		var blockNum uint64

		if err := rows.Scan(&name, &period, &blockNum); err != nil {
			return fmt.Errorf("failed to scan watermark: %w", err)
		}

		r.watermarks[name] = &Watermark{
			LastPeriod:   period,
			LastBlockNum: blockNum,
		}
		count++
	}

	if rows.Err() != nil {
		return fmt.Errorf("error iterating watermarks: %w", rows.Err())
	}

	fmt.Printf("[Chain %d] Loaded %d watermarks from DB\n", r.chainId, count)
	return nil
}

// getWatermark returns watermark from memory, creating empty if not exists
func (r *IndexRunner) getWatermark(indexerName string) *Watermark {
	if wm, exists := r.watermarks[indexerName]; exists {
		return wm
	}

	// Create new empty watermark
	wm := &Watermark{}
	r.watermarks[indexerName] = wm
	return wm
}

// saveWatermark saves watermark to DB
func (r *IndexRunner) saveWatermark(indexerName string, wm *Watermark) error {
	ctx := context.Background()
	query := `
	INSERT INTO indexer_watermarks (chain_id, indexer_name, last_period, last_block_num)
	VALUES (?, ?, ?, ?)`

	return r.conn.Exec(ctx, query, r.chainId, indexerName, wm.LastPeriod, wm.LastBlockNum)
}

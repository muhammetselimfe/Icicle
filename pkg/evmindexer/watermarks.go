package evmindexer

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

// watermarkKey creates a key for watermark storage
func watermarkKey(indexerName, granularity string) string {
	if granularity == "" {
		return indexerName
	}
	return fmt.Sprintf("%s:%s", indexerName, granularity)
}

// loadWatermarks loads all watermarks for this chain from DB into memory
func (r *IndexRunner) loadWatermarks() error {
	ctx := context.Background()
	query := `
	SELECT indexer_name, granularity, last_period, last_block_num
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
		var granularity string
		var period time.Time
		var blockNum uint64

		if err := rows.Scan(&name, &granularity, &period, &blockNum); err != nil {
			return fmt.Errorf("failed to scan watermark: %w", err)
		}

		key := watermarkKey(name, granularity)
		r.watermarks[key] = &Watermark{
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

// getWatermark returns watermark from memory, creating empty if not exists (for incrementals)
func (r *IndexRunner) getWatermark(indexerName string) *Watermark {
	if wm, exists := r.watermarks[indexerName]; exists {
		return wm
	}

	// Create new empty watermark
	wm := &Watermark{}
	r.watermarks[indexerName] = wm
	return wm
}

// getWatermarkWithGranularity returns watermark from memory for granular metrics
func (r *IndexRunner) getWatermarkWithGranularity(indexerName string, granularity string) *Watermark {
	key := watermarkKey(indexerName, granularity)
	if wm, exists := r.watermarks[key]; exists {
		return wm
	}

	// Create new empty watermark
	wm := &Watermark{}
	r.watermarks[key] = wm
	return wm
}

// saveWatermark saves watermark to DB (for incrementals)
func (r *IndexRunner) saveWatermark(indexerName string, wm *Watermark) error {
	ctx := context.Background()
	query := `
	INSERT INTO indexer_watermarks (chain_id, indexer_name, granularity, last_period, last_block_num)
	VALUES (?, ?, ?, ?, ?)`

	return r.conn.Exec(ctx, query, r.chainId, indexerName, "", wm.LastPeriod, wm.LastBlockNum)
}

// saveWatermarkWithGranularity saves watermark to DB for granular metrics
func (r *IndexRunner) saveWatermarkWithGranularity(indexerName string, granularity string, wm *Watermark) error {
	ctx := context.Background()
	query := `
	INSERT INTO indexer_watermarks (chain_id, indexer_name, granularity, last_period, last_block_num)
	VALUES (?, ?, ?, ?, ?)`

	return r.conn.Exec(ctx, query, r.chainId, indexerName, granularity, wm.LastPeriod, wm.LastBlockNum)
}

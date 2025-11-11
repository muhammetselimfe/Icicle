package cache

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

const (
	// blockKeyPrefix is the prefix for block keys
	blockKeyPrefix = "block:"
	// blockKeyPadding is the zero-padded length for block numbers (14 digits supports up to 100 trillion blocks)
	blockKeyPadding = 14
	// checkpointKey is the key for storing the last cached block checkpoint
	checkpointKey = "checkpoint:last_cached_block"
)

// Cache implements caching using PebbleDB
type Cache struct {
	db *pebble.DB
}

// New creates a new PebbleDB cache at the specified path for the given chain ID
func New(dbPath string, chainID uint32) (*Cache, error) {
	chainPath := filepath.Join(dbPath, fmt.Sprintf("%d", chainID))

	opts := &pebble.Options{}

	// Use zstd compression level 1 for all levels
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		return pebble.UniformDBCompressionSettings(block.BalancedCompression)
	})

	// Configure much larger target file sizes for better performance
	// Start at 32MB for L0 and double each level
	opts.TargetFileSizes[0] = 32 << 20  // L0: 32MB
	opts.TargetFileSizes[1] = 64 << 20  // L1: 64MB
	opts.TargetFileSizes[2] = 128 << 20 // L2: 128MB
	opts.TargetFileSizes[3] = 256 << 20 // L3: 256MB
	opts.TargetFileSizes[4] = 512 << 20 // L4: 512MB
	opts.TargetFileSizes[5] = 1 << 30   // L5: 1GB
	opts.TargetFileSizes[6] = 2 << 30   // L6: 2GB

	// Increase L0 compaction thresholds for better write throughput
	opts.L0CompactionThreshold = 8  // Default is 4
	opts.L0StopWritesThreshold = 24 // Default is 12

	// Increase base level size for less write amplification
	opts.LBaseMaxBytes = 512 << 20 // 512MB (default is 64MB)

	// Increase memtable size for larger writes
	opts.MemTableSize = 64 << 20 // 64MB (default is 4MB)

	// Allow more concurrent compactions
	opts.CompactionConcurrencyRange = func() (int, int) { return 4, 8 }

	db, err := pebble.Open(chainPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Cache{db: db}, nil
}

// formatBlockKey formats a block number as a zero-padded key
func formatBlockKey(blockNum int64) []byte {
	padded := fmt.Sprintf("%0*d", blockKeyPadding, blockNum)
	return []byte(blockKeyPrefix + padded)
}

// parseBlockKey extracts block number from a key, returns -1 if invalid
func parseBlockKey(key []byte) int64 {
	prefix := []byte(blockKeyPrefix)
	if len(key) < len(prefix)+blockKeyPadding {
		return -1
	}
	if string(key[:len(prefix)]) != blockKeyPrefix {
		return -1
	}
	blockStr := string(key[len(prefix) : len(prefix)+blockKeyPadding])
	blockNum, err := strconv.ParseInt(blockStr, 10, 64)
	if err != nil {
		return -1
	}
	return blockNum
}

// GetCompleteBlock retrieves or fetches a complete block as JSON bytes
func (c *Cache) GetCompleteBlock(blockNum int64, fetch func() ([]byte, error)) ([]byte, error) {
	key := formatBlockKey(blockNum)

	// Try to get from cache
	value, closer, err := c.db.Get(key)
	if err == nil {
		// Cache hit - copy and return
		defer closer.Close()
		result := make([]byte, len(value))
		copy(result, value)
		return result, nil
	}

	if err != pebble.ErrNotFound {
		// Unexpected error
		return nil, fmt.Errorf("cache get error for block %d: %w", blockNum, err)
	}

	// Cache miss - fetch the block
	data, err := fetch()
	if err != nil {
		return nil, err
	}

	// Store in cache
	if err := c.db.Set(key, data, pebble.NoSync); err != nil {
		// Log error but don't fail the request
		fmt.Printf("Warning: failed to cache block %d: %v\n", blockNum, err)
	}

	return data, nil
}

// GetBlockRange retrieves a range of blocks from the cache [from, to] inclusive
func (c *Cache) GetBlockRange(from, to int64) (map[int64][]byte, error) {
	if from > to {
		return nil, fmt.Errorf("invalid range: from %d > to %d", from, to)
	}

	result := make(map[int64][]byte)
	startKey := formatBlockKey(from)
	endKey := formatBlockKey(to + 1) // +1 to make it exclusive for iterator

	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		blockNum := parseBlockKey(key)
		if blockNum < 0 || blockNum < from || blockNum > to {
			continue // Skip invalid or out-of-range keys
		}

		value := iter.Value()
		// Copy the value since iterator might reuse the buffer
		result[blockNum] = make([]byte, len(value))
		copy(result[blockNum], value)
	}

	return result, nil
}

// Compact triggers a manual compaction of the entire database
func (c *Cache) Compact() error {
	// Compact the entire key range
	// Using nil for start means beginning of keyspace
	// Using a high value for end means end of keyspace
	ctx := context.Background()
	return c.db.Compact(ctx, nil, []byte("\xff\xff\xff\xff\xff"), false)
}

// GetMetrics returns database metrics for monitoring
func (c *Cache) GetMetrics() string {
	return c.db.Metrics().String()
}

// Close closes the PebbleDB database
func (c *Cache) Close() error {
	return c.db.Close()
}

// GetCheckpoint retrieves the last cached block number checkpoint
// Returns 0 if no checkpoint exists
func (c *Cache) GetCheckpoint() (int64, error) {
	value, closer, err := c.db.Get([]byte(checkpointKey))
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get checkpoint: %w", err)
	}
	defer closer.Close()

	blockNum, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse checkpoint: %w", err)
	}

	return blockNum, nil
}

// SetCheckpoint saves the last cached block number checkpoint
func (c *Cache) SetCheckpoint(blockNum int64) error {
	value := []byte(strconv.FormatInt(blockNum, 10))
	if err := c.db.Set([]byte(checkpointKey), value, pebble.Sync); err != nil {
		return fmt.Errorf("failed to set checkpoint: %w", err)
	}
	return nil
}

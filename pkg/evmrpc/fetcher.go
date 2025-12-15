package evmrpc

import (
	"bytes"
	"icicle/pkg/cache"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type FetcherOptions struct {
	RpcURL           string
	ChainID          uint32           // Chain ID for logging
	ChainName        string           // Chain name for logging
	MaxConcurrency   int              // Maximum concurrent RPC and debug requests
	BatchSize        int              // Number of requests per batch
	DebugBatchSize   int              // Number of debug requests per batch
	MaxRetries       int              // Maximum number of retries per request
	RetryDelay       time.Duration    // Initial retry delay
	ProgressCallback ProgressCallback // Optional progress callback
	Cache            *cache.Cache     // Optional cache for complete blocks
}

type Fetcher struct {
	rpcURL         string
	chainID        uint32
	chainName      string
	batchSize      int
	debugBatchSize int
	maxRetries     int
	retryDelay     time.Duration
	progressCb     ProgressCallback
	cache          *cache.Cache

	// Concurrency control
	rpcLimit   chan struct{}
	debugLimit chan struct{}

	// Cache writer
	cacheWriteCh chan cacheWrite
	cacheWg      sync.WaitGroup
	done         chan struct{}

	// HTTP client
	httpClient *http.Client
}

type cacheWrite struct {
	blockNum int64
	block    *NormalizedBlock
}

// txInfo holds information about a transaction and its location
type txInfo struct {
	hash     string
	blockNum int64
	blockIdx int
	txIdx    int
}

func NewFetcher(opts FetcherOptions) *Fetcher {
	if opts.MaxConcurrency == 0 {
		opts.MaxConcurrency = 100
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.DebugBatchSize == 0 {
		opts.DebugBatchSize = 10
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = 500 * time.Millisecond
	}

	// Create HTTP client with proper connection pooling
	// Node.js reuses connections aggressively, so we do the same
	transport := &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 10000, // Default is only 2!
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	f := &Fetcher{
		rpcURL:         opts.RpcURL,
		chainID:        opts.ChainID,
		chainName:      opts.ChainName,
		batchSize:      opts.BatchSize,
		debugBatchSize: opts.DebugBatchSize,
		maxRetries:     opts.MaxRetries,
		retryDelay:     opts.RetryDelay,
		progressCb:     opts.ProgressCallback,
		cache:          opts.Cache,
		rpcLimit:       make(chan struct{}, opts.MaxConcurrency),
		debugLimit:     make(chan struct{}, opts.MaxConcurrency),
		cacheWriteCh:   make(chan cacheWrite, 1000), // Buffered channel
		done:           make(chan struct{}),
		httpClient: &http.Client{
			Timeout:   5 * time.Minute,
			Transport: transport,
		},
	}

	// Start cache writer workers if cache is enabled
	if f.cache != nil {
		numWriters := 4 // Dedicated cache write workers
		for i := 0; i < numWriters; i++ {
			f.cacheWg.Add(1)
			go f.cacheWriter()
		}
	}

	return f
}

// cacheWriter runs in background goroutines to write blocks to cache
func (f *Fetcher) cacheWriter() {
	defer f.cacheWg.Done()
	for {
		select {
		case <-f.done:
			return
		case cw, ok := <-f.cacheWriteCh:
			if !ok {
				return
			}
			data, err := json.Marshal(cw.block)
			if err != nil {
				continue // Silent fail for cache writes
			}
			_, _ = f.cache.GetCompleteBlock(cw.blockNum, func() ([]byte, error) {
				return data, nil
			})
		}
	}
}

// batchRpcCall sends a batch of JSON-RPC requests with retry logic
func (f *Fetcher) batchRpcCall(requests []jsonRpcRequest) ([]jsonRpcResponse, error) {
	if len(requests) == 0 {
		return []jsonRpcResponse{}, nil
	}

	jsonData, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	var responses []jsonRpcResponse
	var lastErr error

	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			log.Printf("[Chain %d - %s] WARNING: Batch request failed: %v. Retrying (attempt %d/%d) after %v", f.chainID, f.chainName, lastErr, attempt, f.maxRetries, delay)
			time.Sleep(delay)
		}

		req, err := http.NewRequest("POST", f.rpcURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := f.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to make batch request: %w", err)
			continue
		}

		decoder := json.NewDecoder(resp.Body)
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&responses)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("failed to unmarshal batch response: %w", err)
			continue
		}

		// Validate responses
		if len(responses) != len(requests) {
			lastErr = fmt.Errorf("batch response count mismatch: sent %d, got %d", len(requests), len(responses))
			continue
		}

		// Sort responses by ID to match request order
		sort.Slice(responses, func(i, j int) bool {
			return responses[i].ID < responses[j].ID
		})

		// Validate all responses and check for errors
		validationErr := false
		for i, resp := range responses {
			if resp.ID != requests[i].ID {
				lastErr = fmt.Errorf("batch response ID mismatch at index %d: expected %d, got %d", i, requests[i].ID, resp.ID)
				validationErr = true
				break
			}
			if resp.Error != nil {
				return nil, fmt.Errorf("RPC error in batch at index %d (ID %d): %s", i, resp.ID, resp.Error.Message)
			}
			if len(resp.Result) == 0 {
				return nil, fmt.Errorf("empty result in batch response at index %d (ID %d)", i, resp.ID)
			}
		}

		if validationErr {
			continue
		}

		return responses, nil
	}

	return nil, fmt.Errorf("batch request failed after %d retries: %w", f.maxRetries, lastErr)
}

// batchRpcCallDebug is like batchRpcCall but uses debug concurrency limit
func (f *Fetcher) batchRpcCallDebug(requests []jsonRpcRequest) ([]jsonRpcResponse, error) {
	if len(requests) == 0 {
		return []jsonRpcResponse{}, nil
	}

	jsonData, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal debug batch request: %w", err)
	}

	var responses []jsonRpcResponse
	var lastErr error

	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			log.Printf("[Chain %d - %s] WARNING: Debug batch request failed: %v. Retrying (attempt %d/%d) after %v", f.chainID, f.chainName, lastErr, attempt, f.maxRetries, delay)
			time.Sleep(delay)
		}

		req, err := http.NewRequest("POST", f.rpcURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to create debug request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := f.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to make debug batch request: %w", err)
			continue
		}

		decoder := json.NewDecoder(resp.Body)
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&responses)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("failed to unmarshal debug batch response: %w", err)
			continue
		}

		// Sort responses by ID to match request order
		sort.Slice(responses, func(i, j int) bool {
			return responses[i].ID < responses[j].ID
		})

		// For debug calls, we allow some errors (like precompile errors) but still validate structure
		if len(responses) != len(requests) {
			lastErr = fmt.Errorf("debug batch response count mismatch: sent %d, got %d", len(requests), len(responses))
			continue
		}

		validationErr := false
		for i, resp := range responses {
			if resp.ID != requests[i].ID {
				lastErr = fmt.Errorf("debug batch response ID mismatch at index %d: expected %d, got %d", i, requests[i].ID, resp.ID)
				validationErr = true
				break
			}
		}

		if validationErr {
			continue
		}

		return responses, nil
	}

	return nil, fmt.Errorf("debug batch request failed after %d retries: %w", f.maxRetries, lastErr)
}

func (f *Fetcher) GetLatestBlock() (int64, error) {
	requests := []jsonRpcRequest{
		{
			Jsonrpc: "2.0",
			Method:  "eth_blockNumber",
			Params:  []interface{}{},
			ID:      1,
		},
	}

	f.rpcLimit <- struct{}{}
	responses, err := f.batchRpcCall(requests)
	<-f.rpcLimit

	if err != nil {
		return 0, err
	}

	var blockNumHex string
	decoder := json.NewDecoder(bytes.NewReader(responses[0].Result))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&blockNumHex); err != nil {
		return 0, fmt.Errorf("failed to unmarshal block number: %w", err)
	}

	var blockNum int64
	if _, err := fmt.Sscanf(blockNumHex, "0x%x", &blockNum); err != nil {
		return 0, fmt.Errorf("failed to parse block number: %w", err)
	}

	return blockNum, nil
}

// chunksOf splits a slice into chunks of specified size
func chunksOf[T any](items []T, size int) [][]T {
	if size <= 0 {
		panic("chunk size must be positive")
	}

	var chunks [][]T
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

// FetchBlockRange fetches all blocks in the range [from, to] inclusive using batch operations
func (f *Fetcher) FetchBlockRange(from, to int64) ([]*NormalizedBlock, error) {
	if from > to {
		return nil, fmt.Errorf("invalid range: from %d > to %d", from, to)
	}

	numBlocks := int(to - from + 1)
	result := make([]*NormalizedBlock, numBlocks)

	// If no cache, fetch everything as before
	if f.cache == nil {
		return f.fetchBlockRangeUncached(from, to)
	}

	// Step 1: Check cache for all blocks using efficient range query
	cachedData, err := f.cache.GetBlockRange(from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query cache range: %w", err)
	}

	// Step 2: Process cache hits and identify misses
	var missingBlocks []int64
	for i := int64(0); i < int64(numBlocks); i++ {
		blockNum := from + i
		if data, ok := cachedData[blockNum]; ok && data != nil {
			// Cache hit - deserialize
			var block NormalizedBlock
			if err := json.Unmarshal(data, &block); err != nil {
				log.Printf("[Chain %d - %s] Warning: failed to deserialize cached block %d: %v", f.chainID, f.chainName, blockNum, err)
				missingBlocks = append(missingBlocks, blockNum)
			} else {
				result[int(i)] = &block
			}
		} else {
			// Cache miss
			missingBlocks = append(missingBlocks, blockNum)
		}
	}

	// Step 3: If all cached, return
	if len(missingBlocks) == 0 {
		return result, nil
	}

	// Step 4: Fetch missing blocks
	sort.Slice(missingBlocks, func(i, j int) bool {
		return missingBlocks[i] < missingBlocks[j]
	})

	// Fetch each missing block range
	fetchedBlocks, err := f.fetchAndCacheMissingBlocks(missingBlocks)
	if err != nil {
		return nil, err
	}

	// Step 5: Fill in missing blocks in result
	for _, blockNum := range missingBlocks {
		idx := int(blockNum - from)
		if block, ok := fetchedBlocks[blockNum]; ok {
			result[idx] = block
		} else {
			return nil, fmt.Errorf("missing block %d after fetch", blockNum)
		}
	}

	return result, nil
}

// fetchBlockRangeUncached is the original implementation without caching
func (f *Fetcher) fetchBlockRangeUncached(from, to int64) ([]*NormalizedBlock, error) {
	// Batch fetch all blocks
	blocks, err := f.fetchBlocksBatch(from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blocks: %w", err)
	}

	// Collect all transaction hashes with their block numbers
	var allTxs []txInfo

	for blockIdx, block := range blocks {
		blockNum := from + int64(blockIdx)
		for txIdx, tx := range block.Transactions {
			allTxs = append(allTxs, txInfo{
				hash:     tx.Hash,
				blockNum: blockNum,
				blockIdx: blockIdx,
				txIdx:    txIdx,
			})
		}
	}

	// Batch fetch all receipts
	var receiptsMap map[string]Receipt
	if len(allTxs) > 0 {
		receiptsMap, err = f.fetchReceiptsBatch(allTxs)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch receipts: %w", err)
		}
	} else {
		receiptsMap = make(map[string]Receipt)
	}

	// Batch fetch all traces
	var tracesMap map[string]*TraceResultOptional
	if len(allTxs) > 0 {
		tracesMap, err = f.fetchTracesBatch(from, to, allTxs)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch traces: %w", err)
		}
	} else {
		tracesMap = make(map[string]*TraceResultOptional)
	}

	// Assemble normalized blocks
	result := make([]*NormalizedBlock, len(blocks))
	for i := range blocks {
		blockNum := from + int64(i)

		// Collect receipts for this block
		receipts := make([]Receipt, len(blocks[i].Transactions))
		traces := make([]TraceResultOptional, len(blocks[i].Transactions))

		for j, tx := range blocks[i].Transactions {
			receipt, ok := receiptsMap[tx.Hash]
			if !ok {
				return nil, fmt.Errorf("missing receipt for tx %s in block %d", tx.Hash, blockNum)
			}
			receipts[j] = receipt

			trace, ok := tracesMap[tx.Hash]
			if ok && trace != nil {
				traces[j] = *trace
			} else {
				traces[j] = TraceResultOptional{
					TxHash: tx.Hash,
					Result: nil,
				}
			}
		}

		result[i] = &NormalizedBlock{
			Block:    blocks[i],
			Receipts: receipts,
			Traces:   traces,
		}
	}

	return result, nil
}

// fetchAndCacheMissingBlocks fetches missing blocks in batch and caches them
func (f *Fetcher) fetchAndCacheMissingBlocks(missingBlocks []int64) (map[int64]*NormalizedBlock, error) {
	if len(missingBlocks) == 0 {
		return make(map[int64]*NormalizedBlock), nil
	}

	// Find contiguous ranges for efficient batch fetching
	ranges := f.findContiguousRanges(missingBlocks)

	result := make(map[int64]*NormalizedBlock)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErr error

	for _, r := range ranges {
		wg.Add(1)
		go func(from, to int64) {
			defer wg.Done()

			blocks, err := f.fetchBlockRangeUncached(from, to)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = err
				}
				mu.Unlock()
				return
			}

			// Cache each block
			for i, block := range blocks {
				blockNum := from + int64(i)

				mu.Lock()
				result[blockNum] = block
				mu.Unlock()

				// Fire-and-forget cache write via channel
				select {
				case f.cacheWriteCh <- cacheWrite{blockNum: blockNum, block: block}:
					// Sent to cache writer
				default:
					// Channel full, skip caching this block (non-blocking)
				}
			}
		}(r[0], r[1])
	}

	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}

	return result, nil
}

// findContiguousRanges finds contiguous block ranges from a sorted list
func (f *Fetcher) findContiguousRanges(blocks []int64) [][2]int64 {
	if len(blocks) == 0 {
		return nil
	}

	var ranges [][2]int64
	start := blocks[0]
	end := blocks[0]

	for i := 1; i < len(blocks); i++ {
		if blocks[i] == end+1 {
			end = blocks[i]
		} else {
			ranges = append(ranges, [2]int64{start, end})
			start = blocks[i]
			end = blocks[i]
		}
	}
	ranges = append(ranges, [2]int64{start, end})

	return ranges
}

func (f *Fetcher) fetchBlocksBatch(from, to int64) ([]Block, error) {
	numBlocks := int(to - from + 1)
	blocks := make([]Block, numBlocks)

	// Create all block requests
	var allRequests []jsonRpcRequest
	for i := int64(0); i < int64(numBlocks); i++ {
		blockNum := from + i
		allRequests = append(allRequests, jsonRpcRequest{
			Jsonrpc: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []interface{}{fmt.Sprintf("0x%x", blockNum), true}, // true for full transactions
			ID:      int(i),
		})
	}

	// Split into batches and execute concurrently
	batches := chunksOf(allRequests, f.batchSize)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var batchErr error
	var completedBlocks int64

	for batchIdx, batch := range batches {
		wg.Add(1)
		go func(idx int, requests []jsonRpcRequest) {
			defer wg.Done()

			f.rpcLimit <- struct{}{}
			responses, err := f.batchRpcCall(requests)
			<-f.rpcLimit

			if err != nil {
				mu.Lock()
				if batchErr == nil {
					batchErr = fmt.Errorf("batch %d failed: %w", idx, err)
				}
				mu.Unlock()
				return
			}

			// Parse block responses
			batchTxCount := 0
			for _, resp := range responses {
				var block Block
				decoder := json.NewDecoder(bytes.NewReader(resp.Result))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&block); err != nil {
					mu.Lock()
					if batchErr == nil {
						batchErr = fmt.Errorf("failed to unmarshal block at index %d: %w", resp.ID, err)
					}
					mu.Unlock()
					return
				}

				batchTxCount += len(block.Transactions)
				mu.Lock()
				blocks[resp.ID] = block
				mu.Unlock()
			}

			// Report progress
			mu.Lock()
			completedBlocks += int64(len(responses))
			if f.progressCb != nil {
				f.progressCb("blocks", completedBlocks, int64(numBlocks), batchTxCount)
			}
			mu.Unlock()
		}(batchIdx, batch)
	}

	wg.Wait()

	if batchErr != nil {
		return nil, batchErr
	}

	return blocks, nil
}

func (f *Fetcher) fetchReceiptsBatch(txInfos []txInfo) (map[string]Receipt, error) {
	receiptsMap := make(map[string]Receipt)
	var mu sync.Mutex

	// Create all receipt requests
	var allRequests []jsonRpcRequest
	txHashToIdx := make(map[int]string) // Map request ID to tx hash

	for i, tx := range txInfos {
		allRequests = append(allRequests, jsonRpcRequest{
			Jsonrpc: "2.0",
			Method:  "eth_getTransactionReceipt",
			Params:  []interface{}{tx.hash},
			ID:      i,
		})
		txHashToIdx[i] = tx.hash
	}

	// Split into batches and execute concurrently
	batches := chunksOf(allRequests, f.batchSize)
	var wg sync.WaitGroup
	var batchErr error
	var completedReceipts int64
	totalReceipts := int64(len(txInfos))

	for batchIdx, batch := range batches {
		wg.Add(1)
		go func(idx int, requests []jsonRpcRequest) {
			defer wg.Done()

			f.rpcLimit <- struct{}{}
			responses, err := f.batchRpcCall(requests)
			<-f.rpcLimit

			if err != nil {
				mu.Lock()
				if batchErr == nil {
					batchErr = fmt.Errorf("receipt batch %d failed: %w", idx, err)
				}
				mu.Unlock()
				return
			}

			// Parse and store receipt responses
			for _, resp := range responses {
				txHash := txHashToIdx[resp.ID]

				var receipt Receipt
				decoder := json.NewDecoder(bytes.NewReader(resp.Result))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&receipt); err != nil {
					mu.Lock()
					if batchErr == nil {
						batchErr = fmt.Errorf("failed to unmarshal receipt for tx %s: %w", txHash, err)
					}
					mu.Unlock()
					return
				}

				mu.Lock()
				receiptsMap[txHash] = receipt
				mu.Unlock()
			}

			// Report progress
			mu.Lock()
			completedReceipts += int64(len(responses))
			if f.progressCb != nil {
				f.progressCb("receipts", completedReceipts, totalReceipts, len(responses))
			}
			mu.Unlock()
		}(batchIdx, batch)
	}

	wg.Wait()

	if batchErr != nil {
		return nil, batchErr
	}

	return receiptsMap, nil
}

func (f *Fetcher) fetchTracesBatch(from, to int64, txInfos []txInfo) (map[string]*TraceResultOptional, error) {
	tracesMap := make(map[string]*TraceResultOptional)
	var mu sync.Mutex

	// First try block-level tracing

	numBlocks := int(to - from + 1)
	var blockRequests []jsonRpcRequest

	for i := 0; i < numBlocks; i++ {
		blockNum := from + int64(i)
		blockRequests = append(blockRequests, jsonRpcRequest{
			Jsonrpc: "2.0",
			Method:  "debug_traceBlockByNumber",
			Params:  []interface{}{fmt.Sprintf("0x%x", blockNum), map[string]string{"tracer": "callTracer"}},
			ID:      i,
		})
	}

	// Try block traces in batches
	blockBatches := chunksOf(blockRequests, f.debugBatchSize)
	var blockTraceSuccess = true
	blockTraces := make(map[int64][]TraceResultOptional)

	var wg sync.WaitGroup
	var blockErr error

	for batchIdx, batch := range blockBatches {
		wg.Add(1)
		go func(idx int, requests []jsonRpcRequest) {
			defer wg.Done()

			f.debugLimit <- struct{}{}
			responses, err := f.batchRpcCallDebug(requests)
			<-f.debugLimit

			if err != nil {
				mu.Lock()
				blockTraceSuccess = false
				if blockErr == nil {
					blockErr = fmt.Errorf("debug batch %d failed: %w", idx, err)
				}
				mu.Unlock()
				return
			}

			// Parse block trace responses
			for _, resp := range responses {
				if resp.Error != nil {
					mu.Lock()
					blockTraceSuccess = false
					mu.Unlock()
					return
				}

				var traces []TraceResultOptional
				decoder := json.NewDecoder(bytes.NewReader(resp.Result))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&traces); err != nil {
					mu.Lock()
					blockTraceSuccess = false
					mu.Unlock()
					return
				}

				blockNum := from + int64(resp.ID)
				mu.Lock()
				blockTraces[blockNum] = traces
				mu.Unlock()
			}
		}(batchIdx, batch)
	}

	wg.Wait()

	if blockTraceSuccess && blockErr == nil {
		// Map block traces to transaction hashes
		for _, txInfo := range txInfos {
			if traces, ok := blockTraces[txInfo.blockNum]; ok && txInfo.txIdx < len(traces) {
				tracesMap[txInfo.hash] = &traces[txInfo.txIdx]
			}
		}
		return tracesMap, nil
	}

	// Fall back to per-transaction tracing

	var txRequests []jsonRpcRequest
	txHashToIdx := make(map[int]string)

	for i, tx := range txInfos {
		txRequests = append(txRequests, jsonRpcRequest{
			Jsonrpc: "2.0",
			Method:  "debug_traceTransaction",
			Params:  []interface{}{tx.hash, map[string]string{"tracer": "callTracer"}},
			ID:      i,
		})
		txHashToIdx[i] = tx.hash
	}

	// Execute transaction traces in batches
	txBatches := chunksOf(txRequests, f.debugBatchSize)
	wg = sync.WaitGroup{}
	var txBatchErr error

	for batchIdx, batch := range txBatches {
		wg.Add(1)
		go func(idx int, requests []jsonRpcRequest) {
			defer wg.Done()

			var responses []jsonRpcResponse
			var err error

			// Retry logic for this batch
			for attempt := 0; attempt <= f.maxRetries; attempt++ {
				if attempt > 0 {
					delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
					if delay > 10*time.Second {
						delay = 10 * time.Second
					}
					log.Printf("[Chain %d - %s] Retrying trace batch %d (attempt %d/%d) after %v\n", f.chainID, f.chainName, idx, attempt, f.maxRetries, delay)
					time.Sleep(delay)
				}

				f.debugLimit <- struct{}{}
				responses, err = f.batchRpcCallDebug(requests)
				<-f.debugLimit

				if err != nil {
					continue // Network/batch error, retry
				}

				// Check if any non-precompile errors exist
				hasRetryableError := false
				for _, resp := range responses {
					if resp.Error != nil {
						if !isPrecompileError(fmt.Errorf("%s", resp.Error.Message)) {
							hasRetryableError = true
							break
						}
					}
				}

				if !hasRetryableError {
					break // Success or only precompile errors
				}
			}

			if err != nil {
				// Batch failed after retries
				mu.Lock()
				if txBatchErr == nil {
					txBatchErr = fmt.Errorf("trace batch %d failed after retries: %w", idx, err)
				}
				mu.Unlock()
				return
			}

			// Parse transaction trace responses
			for _, resp := range responses {
				txHash := txHashToIdx[resp.ID]

				if resp.Error != nil {
					// ONLY precompile errors are acceptable as nil traces
					if isPrecompileError(fmt.Errorf("%s", resp.Error.Message)) {
						log.Printf("[Chain %d - %s] Trace failed for tx %s (precompile), treating as nil trace", f.chainID, f.chainName, txHash)
						mu.Lock()
						tracesMap[txHash] = &TraceResultOptional{
							TxHash: txHash,
							Result: nil,
						}
						mu.Unlock()
						continue
					} else {
						// Non-precompile error after retries - fail the batch
						mu.Lock()
						if txBatchErr == nil {
							txBatchErr = fmt.Errorf("trace for tx %s failed after retries: %s", txHash, resp.Error.Message)
						}
						mu.Unlock()
						return
					}
				}

				var trace CallTrace
				decoder := json.NewDecoder(bytes.NewReader(resp.Result))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&trace); err != nil {
					mu.Lock()
					if txBatchErr == nil {
						txBatchErr = fmt.Errorf("failed to parse trace for tx %s: %w", txHash, err)
					}
					mu.Unlock()
					return
				}

				mu.Lock()
				tracesMap[txHash] = &TraceResultOptional{
					TxHash: txHash,
					Result: &trace,
				}
				mu.Unlock()
			}
		}(batchIdx, batch)
	}

	wg.Wait()

	if txBatchErr != nil {
		return nil, txBatchErr
	}

	return tracesMap, nil
}

func isPrecompileError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "incorrect number of top-level calls")
}

// Close stops all background goroutines and cleans up resources
func (f *Fetcher) Close() {
	if f.done != nil {
		close(f.done)
	}
	if f.cacheWriteCh != nil {
		close(f.cacheWriteCh)
	}
	f.cacheWg.Wait()
}

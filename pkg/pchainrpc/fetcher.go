package pchainrpc

import (
	"bytes"
	"clickhouse-metrics-poc/pkg/cache"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/rpc"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/platformvm/block"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
)

type FetcherOptions struct {
	RpcURL         string
	MaxConcurrency int           // Maximum concurrent RPC requests
	BatchSize      int           // Number of blocks per batch
	MaxRetries     int           // Maximum number of retries per request
	RetryDelay     time.Duration // Initial retry delay
	Cache          *cache.Cache  // Optional cache for complete blocks
}

// pooledRequester implements EndpointRequester with proper connection pooling
type pooledRequester struct {
	uri        string
	httpClient *http.Client
}

func newPooledRequester(uri string) *pooledRequester {
	transport := &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 10000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	return &pooledRequester{
		uri: uri,
		httpClient: &http.Client{
			Timeout:   5 * time.Minute,
			Transport: transport,
		},
	}
}

func (r *pooledRequester) SendRequest(ctx context.Context, method string, params interface{}, reply interface{}, options ...rpc.Option) error {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.uri+"/ext/P", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to issue request: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	if err := json.Unmarshal(rpcResp.Result, reply); err != nil {
		return fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return nil
}

type Fetcher struct {
	client     *platformvm.Client
	rpcURL     string
	batchSize  int
	maxRetries int
	retryDelay time.Duration
	cache      *cache.Cache

	// Concurrency control
	rpcLimit chan struct{}
}

func NewFetcher(opts FetcherOptions) *Fetcher {
	if opts.MaxConcurrency == 0 {
		opts.MaxConcurrency = 50
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = 500 * time.Millisecond
	}

	// Create client with custom HTTP connection pooling
	requester := newPooledRequester(opts.RpcURL)
	client := &platformvm.Client{
		Requester: requester,
	}

	f := &Fetcher{
		client:     client,
		rpcURL:     opts.RpcURL,
		batchSize:  opts.BatchSize,
		maxRetries: opts.MaxRetries,
		retryDelay: opts.RetryDelay,
		cache:      opts.Cache,
		rpcLimit:   make(chan struct{}, opts.MaxConcurrency),
	}

	return f
}

// GetLatestBlock returns the latest block height from the P-chain
func (f *Fetcher) GetLatestBlock() (int64, error) {
	var lastErr error
	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			log.Printf("WARNING: GetHeight failed: %v. Retrying (attempt %d/%d) after %v", lastErr, attempt, f.maxRetries, delay)
			time.Sleep(delay)
		}

		height, err := f.client.GetHeight(context.Background())
		if err != nil {
			lastErr = err
			continue
		}
		return int64(height), nil
	}

	return 0, fmt.Errorf("failed to get latest block after %d retries: %w", f.maxRetries, lastErr)
}

// FetchBlockRange fetches all blocks in the range [from, to] inclusive
func (f *Fetcher) FetchBlockRange(from, to int64) ([]*NormalizedBlock, error) {
	if from > to {
		return nil, fmt.Errorf("invalid range: from %d > to %d", from, to)
	}

	numBlocks := int(to - from + 1)
	result := make([]*NormalizedBlock, numBlocks)

	// If no cache, fetch everything uncached
	if f.cache == nil {
		return f.fetchBlockRangeUncached(from, to)
	}

	// Step 1: Check cache for all blocks (raw bytes)
	cachedData, err := f.cache.GetBlockRange(from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query cache range: %w", err)
	}

	// Step 2: Process cache hits and identify misses
	var missingBlocks []int64
	for i := int64(0); i < int64(numBlocks); i++ {
		blockNum := from + i
		if rawBytes, ok := cachedData[blockNum]; ok && rawBytes != nil {
			// Cache hit - parse and normalize raw bytes
			normalized, err := f.parseAndNormalize(rawBytes)
			if err != nil {
				log.Printf("Warning: failed to parse cached block %d: %v", blockNum, err)
				missingBlocks = append(missingBlocks, blockNum)
			} else {
				result[int(i)] = normalized
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

// fetchBlockRangeUncached fetches blocks without using cache
func (f *Fetcher) fetchBlockRangeUncached(from, to int64) ([]*NormalizedBlock, error) {
	numBlocks := int(to - from + 1)
	blocks := make([]*NormalizedBlock, numBlocks)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErr error

	// Fetch blocks concurrently
	for i := int64(0); i < int64(numBlocks); i++ {
		wg.Add(1)
		go func(idx int64) {
			defer wg.Done()

			blockHeight := from + idx

			f.rpcLimit <- struct{}{}
			defer func() { <-f.rpcLimit }()

			block, err := f.fetchSingleBlock(blockHeight)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("failed to fetch block %d: %w", blockHeight, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			blocks[idx] = block
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}

	return blocks, nil
}

// fetchAndCacheMissingBlocks fetches missing blocks, caches raw bytes, and returns normalized blocks
func (f *Fetcher) fetchAndCacheMissingBlocks(missingBlocks []int64) (map[int64]*NormalizedBlock, error) {
	if len(missingBlocks) == 0 {
		return make(map[int64]*NormalizedBlock), nil
	}

	result := make(map[int64]*NormalizedBlock)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErr error

	for _, blockNum := range missingBlocks {
		wg.Add(1)
		go func(height int64) {
			defer wg.Done()

			f.rpcLimit <- struct{}{}
			defer func() { <-f.rpcLimit }()

			// Fetch raw block bytes
			blockBytes, err := f.client.GetBlockByHeight(context.Background(), uint64(height))
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("GetBlockByHeight failed for block %d: %w", height, err)
				}
				mu.Unlock()
				return
			}

			// Cache raw bytes immediately
			if f.cache != nil {
				_, _ = f.cache.GetCompleteBlock(height, func() ([]byte, error) {
					return blockBytes, nil
				})
			}

			// Parse and normalize
			normalized, err := f.parseAndNormalize(blockBytes)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("parseAndNormalize failed for block %d: %w", height, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			result[height] = normalized
			mu.Unlock()
		}(blockNum)
	}

	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}

	return result, nil
}

// parseAndNormalize parses raw block bytes and normalizes them
func (f *Fetcher) parseAndNormalize(blockBytes []byte) (*NormalizedBlock, error) {
	blk, err := block.Parse(block.Codec, blockBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}
	return f.normalizeBlock(blk)
}

// fetchSingleBlock fetches a single block by height with retry logic
func (f *Fetcher) fetchSingleBlock(height int64) (*NormalizedBlock, error) {
	var lastErr error

	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			time.Sleep(delay)
		}

		// Fetch block bytes
		blockBytes, err := f.client.GetBlockByHeight(context.Background(), uint64(height))
		if err != nil {
			lastErr = fmt.Errorf("GetBlockByHeight failed: %w", err)
			continue
		}

		// Cache raw bytes immediately if cache is enabled
		if f.cache != nil {
			_, _ = f.cache.GetCompleteBlock(height, func() ([]byte, error) {
				return blockBytes, nil
			})
		}

		// Parse and normalize
		normalized, err := f.parseAndNormalize(blockBytes)
		if err != nil {
			lastErr = fmt.Errorf("parseAndNormalize failed: %w", err)
			continue
		}

		return normalized, nil
	}

	return nil, fmt.Errorf("failed to fetch block %d after %d retries: %w", height, f.maxRetries, lastErr)
}

// timestampExtractor implements block.Visitor to extract timestamps
type timestampExtractor struct {
	timestamp time.Time
}

func (te *timestampExtractor) BanffAbortBlock(b *block.BanffAbortBlock) error {
	te.timestamp = b.Timestamp()
	return nil
}

func (te *timestampExtractor) BanffCommitBlock(b *block.BanffCommitBlock) error {
	te.timestamp = b.Timestamp()
	return nil
}

func (te *timestampExtractor) BanffProposalBlock(b *block.BanffProposalBlock) error {
	te.timestamp = b.Timestamp()
	return nil
}

func (te *timestampExtractor) BanffStandardBlock(b *block.BanffStandardBlock) error {
	te.timestamp = b.Timestamp()
	return nil
}

func (te *timestampExtractor) ApricotAbortBlock(b *block.ApricotAbortBlock) error {
	// Apricot blocks don't have timestamps - use zero time
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotCommitBlock(b *block.ApricotCommitBlock) error {
	// Apricot blocks don't have timestamps - use zero time
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotProposalBlock(b *block.ApricotProposalBlock) error {
	// Apricot blocks don't have timestamps - use zero time
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotStandardBlock(b *block.ApricotStandardBlock) error {
	// Apricot blocks don't have timestamps - use zero time
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotAtomicBlock(b *block.ApricotAtomicBlock) error {
	// Apricot blocks don't have timestamps - use zero time
	te.timestamp = time.Time{}
	return nil
}

// normalizeBlock converts a platform block to normalized structure
func (f *Fetcher) normalizeBlock(blk block.Block) (*NormalizedBlock, error) {
	// Extract timestamp using visitor pattern
	extractor := &timestampExtractor{}
	if err := blk.Visit(extractor); err != nil {
		return nil, fmt.Errorf("failed to extract timestamp: %w", err)
	}
	blockTime := extractor.timestamp

	normalized := &NormalizedBlock{
		BlockID:      blk.ID(),
		Height:       blk.Height(),
		ParentID:     blk.Parent(),
		Timestamp:    blockTime,
		Transactions: make([]NormalizedTx, 0, len(blk.Txs())),
	}

	// Parse each transaction
	for _, tx := range blk.Txs() {
		normalizedTx, err := f.normalizeTx(tx, blk.Height(), blockTime)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize tx %s: %w", tx.ID(), err)
		}
		normalized.Transactions = append(normalized.Transactions, *normalizedTx)
	}

	return normalized, nil
}

// normalizeTx normalizes a transaction into storage format
func (f *Fetcher) normalizeTx(tx *txs.Tx, blockHeight uint64, blockTime time.Time) (*NormalizedTx, error) {
	if tx == nil || tx.Unsigned == nil {
		return nil, fmt.Errorf("nil transaction or unsigned tx")
	}

	normalized := &NormalizedTx{
		TxID:        tx.ID(),
		TxType:      TxTypeString(tx),
		BlockHeight: blockHeight,
		BlockTime:   blockTime,
	}

	// Type switch to extract type-specific fields
	switch utx := tx.Unsigned.(type) {
	case *txs.ConvertSubnetToL1Tx:
		normalized.SubnetID = &utx.Subnet
		normalized.ChainID = &utx.ChainID
		// Convert JSONByteSlice to []byte
		address := []byte(utx.Address)
		normalized.Address = &address
		// Encode validators as JSON
		validatorsJSON, _ := json.Marshal(utx.Validators)
		normalized.Validators = validatorsJSON

	case *txs.AddValidatorTx:
		normalized.NodeID = &utx.Validator.NodeID
		normalized.StartTime = &utx.Validator.Start
		normalized.EndTime = &utx.Validator.End
		normalized.Weight = &utx.Validator.Wght

	case *txs.AddDelegatorTx:
		normalized.NodeID = &utx.Validator.NodeID
		normalized.StartTime = &utx.Validator.Start
		normalized.EndTime = &utx.Validator.End
		normalized.Weight = &utx.Validator.Wght

	case *txs.CreateSubnetTx:
		ownerJSON, _ := json.Marshal(utx.Owner)
		normalized.Owner = ownerJSON

	case *txs.CreateChainTx:
		normalized.SubnetID = &utx.SubnetID
		normalized.ChainName = &utx.ChainName
		normalized.GenesisData = utx.GenesisData
		normalized.VMID = &utx.VMID
		normalized.FxIDs = utx.FxIDs
		subnetAuthJSON, _ := json.Marshal(utx.SubnetAuth)
		normalized.SubnetAuth = subnetAuthJSON

	case *txs.ImportTx:
		normalized.SourceChain = &utx.SourceChain

	case *txs.ExportTx:
		normalized.DestinationChain = &utx.DestinationChain

	case *txs.AddSubnetValidatorTx:
		normalized.SubnetID = &utx.SubnetValidator.Subnet
		normalized.NodeID = &utx.SubnetValidator.NodeID
		startTime := uint64(utx.SubnetValidator.StartTime().Unix())
		endTime := uint64(utx.SubnetValidator.EndTime().Unix())
		weight := utx.SubnetValidator.Weight()
		normalized.StartTime = &startTime
		normalized.EndTime = &endTime
		normalized.Weight = &weight

	case *txs.RemoveSubnetValidatorTx:
		normalized.SubnetID = &utx.Subnet
		normalized.NodeID = &utx.NodeID

	case *txs.TransformSubnetTx:
		normalized.SubnetID = &utx.Subnet
		normalized.AssetID = &utx.AssetID
		normalized.InitialSupply = &utx.InitialSupply
		normalized.MaxSupply = &utx.MaximumSupply
		normalized.MinConsumptionRate = &utx.MinConsumptionRate
		normalized.MaxConsumptionRate = &utx.MaxConsumptionRate
		normalized.MinValidatorStake = &utx.MinValidatorStake
		normalized.MaxValidatorStake = &utx.MaxValidatorStake
		normalized.MinStakeDuration = &utx.MinStakeDuration
		normalized.MaxStakeDuration = &utx.MaxStakeDuration
		normalized.MinDelegationFee = &utx.MinDelegationFee
		normalized.MinDelegatorStake = &utx.MinDelegatorStake
		normalized.MaxValidatorWeightFactor = &utx.MaxValidatorWeightFactor
		normalized.UptimeRequirement = &utx.UptimeRequirement

	case *txs.TransferSubnetOwnershipTx:
		normalized.SubnetID = &utx.Subnet
		ownerJSON, _ := json.Marshal(utx.Owner)
		normalized.Owner = ownerJSON

	case *txs.AddPermissionlessValidatorTx:
		normalized.SubnetID = &utx.Subnet
		normalized.NodeID = &utx.Validator.NodeID
		normalized.StartTime = &utx.Validator.Start
		normalized.EndTime = &utx.Validator.End
		normalized.Weight = &utx.Validator.Wght
		signerJSON, _ := json.Marshal(utx.Signer)
		normalized.Signer = signerJSON
		stakeOutsJSON, _ := json.Marshal(utx.StakeOuts)
		normalized.StakeOuts = stakeOutsJSON
		rewardsOwnerJSON, _ := json.Marshal(utx.ValidatorRewardsOwner)
		normalized.ValidatorRewardsOwner = rewardsOwnerJSON
		normalized.DelegationShares = &utx.DelegationShares

	case *txs.AddPermissionlessDelegatorTx:
		normalized.SubnetID = &utx.Subnet
		normalized.NodeID = &utx.Validator.NodeID
		normalized.StartTime = &utx.Validator.Start
		normalized.EndTime = &utx.Validator.End
		normalized.Weight = &utx.Validator.Wght
		stakeOutsJSON, _ := json.Marshal(utx.StakeOuts)
		normalized.StakeOuts = stakeOutsJSON
		rewardsOwnerJSON, _ := json.Marshal(utx.RewardsOwner)
		normalized.DelegatorRewardsOwner = rewardsOwnerJSON

	case *txs.RewardValidatorTx:
		normalized.RewardTxID = &utx.TxID

	case *txs.IncreaseL1ValidatorBalanceTx:
		normalized.ValidationID = &utx.ValidationID
		normalized.Balance = &utx.Balance

	case *txs.SetL1ValidatorWeightTx:
		normalized.Message = []byte(utx.Message)

	case *txs.AdvanceTimeTx:
		normalized.Time = &utx.Time

	case *txs.BaseTx:
		// Base transaction has no specific fields beyond common ones
	}

	return normalized, nil
}

// normalizeBlockToJSON converts a platform block to JSON-based structure
func (f *Fetcher) normalizeBlockToJSON(blk block.Block) (*JSONBlock, error) {
	// Extract timestamp using visitor pattern
	extractor := &timestampExtractor{}
	if err := blk.Visit(extractor); err != nil {
		return nil, fmt.Errorf("failed to extract timestamp: %w", err)
	}
	blockTime := extractor.timestamp

	jsonBlock := &JSONBlock{
		BlockID:      blk.ID(),
		Height:       blk.Height(),
		ParentID:     blk.Parent(),
		Timestamp:    blockTime,
		Transactions: make([]JSONTx, 0, len(blk.Txs())),
	}

	// Parse each transaction
	for _, tx := range blk.Txs() {
		jsonTx, err := f.normalizeTxToJSON(tx, blk.Height(), blockTime)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize tx %s: %w", tx.ID(), err)
		}
		jsonBlock.Transactions = append(jsonBlock.Transactions, *jsonTx)
	}

	return jsonBlock, nil
}

// normalizeTxToJSON normalizes a transaction into JSON storage format
func (f *Fetcher) normalizeTxToJSON(tx *txs.Tx, blockHeight uint64, blockTime time.Time) (*JSONTx, error) {
	if tx == nil || tx.Unsigned == nil {
		return nil, fmt.Errorf("nil transaction or unsigned tx")
	}

	// Serialize the entire tx.Unsigned to JSON
	txDataJSON, err := json.Marshal(tx.Unsigned)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tx.Unsigned to JSON: %w", err)
	}

	jsonTx := &JSONTx{
		TxID:        tx.ID(),
		TxType:      TxTypeString(tx),
		BlockHeight: blockHeight,
		BlockTime:   blockTime,
		TxData:      txDataJSON,
	}

	return jsonTx, nil
}

// FetchBlockRangeJSON fetches a range of blocks and returns them as JSON blocks
func (f *Fetcher) FetchBlockRangeJSON(from, to int64) ([]*JSONBlock, error) {
	if from > to {
		return nil, fmt.Errorf("invalid range: from (%d) > to (%d)", from, to)
	}

	numBlocks := to - from + 1
	if numBlocks <= 0 {
		return nil, nil
	}

	// Try to get blocks from cache first
	if f.cache != nil {
		cachedBlocks, missingBlocks := f.getCachedJSONBlocks(from, to)

		// If we have all blocks cached, return them
		if len(missingBlocks) == 0 {
			result := make([]*JSONBlock, numBlocks)
			for i := int64(0); i < numBlocks; i++ {
				result[i] = cachedBlocks[from+i]
			}
			return result, nil
		}

		// Fetch missing blocks
		fetchedBlocks, err := f.fetchAndCacheMissingJSONBlocks(missingBlocks)
		if err != nil {
			return nil, err
		}

		// Merge cached and fetched blocks
		for height, block := range fetchedBlocks {
			cachedBlocks[height] = block
		}

		// Build result array in order
		result := make([]*JSONBlock, numBlocks)
		for i := int64(0); i < numBlocks; i++ {
			result[i] = cachedBlocks[from+i]
		}
		return result, nil
	}

	// No cache - fetch all blocks
	blocks := make([]*JSONBlock, numBlocks)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErr error

	// Fetch blocks concurrently
	for i := int64(0); i < int64(numBlocks); i++ {
		wg.Add(1)
		go func(idx int64) {
			defer wg.Done()

			blockHeight := from + idx

			f.rpcLimit <- struct{}{}
			defer func() { <-f.rpcLimit }()

			block, err := f.fetchSingleJSONBlock(blockHeight)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("failed to fetch block %d: %w", blockHeight, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			blocks[idx] = block
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}

	return blocks, nil
}

// getCachedJSONBlocks attempts to get blocks from cache, returning cached blocks and list of missing block numbers
func (f *Fetcher) getCachedJSONBlocks(from, to int64) (map[int64]*JSONBlock, []int64) {
	cached := make(map[int64]*JSONBlock)
	var missing []int64

	// Get block range from cache
	cachedData, err := f.cache.GetBlockRange(from, to)
	if err != nil {
		// On error, mark all as missing
		for height := from; height <= to; height++ {
			missing = append(missing, height)
		}
		return cached, missing
	}

	// Process cached blocks and identify missing ones
	for height := from; height <= to; height++ {
		if rawBytes, ok := cachedData[height]; ok && rawBytes != nil {
			// Parse and convert to JSON
			jsonBlock, err := f.parseAndNormalizeToJSON(rawBytes)
			if err != nil {
				missing = append(missing, height)
				continue
			}
			cached[height] = jsonBlock
		} else {
			missing = append(missing, height)
		}
	}

	return cached, missing
}

// fetchAndCacheMissingJSONBlocks fetches missing blocks, caches raw bytes, and returns JSON blocks
func (f *Fetcher) fetchAndCacheMissingJSONBlocks(missingBlocks []int64) (map[int64]*JSONBlock, error) {
	if len(missingBlocks) == 0 {
		return make(map[int64]*JSONBlock), nil
	}

	result := make(map[int64]*JSONBlock)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErr error

	for _, blockNum := range missingBlocks {
		wg.Add(1)
		go func(height int64) {
			defer wg.Done()

			f.rpcLimit <- struct{}{}
			defer func() { <-f.rpcLimit }()

			// Fetch raw block bytes
			blockBytes, err := f.client.GetBlockByHeight(context.Background(), uint64(height))
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("GetBlockByHeight failed for block %d: %w", height, err)
				}
				mu.Unlock()
				return
			}

			// Cache raw bytes immediately
			if f.cache != nil {
				_, _ = f.cache.GetCompleteBlock(height, func() ([]byte, error) {
					return blockBytes, nil
				})
			}

			// Parse and normalize to JSON
			jsonBlock, err := f.parseAndNormalizeToJSON(blockBytes)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("parseAndNormalizeToJSON failed for block %d: %w", height, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			result[height] = jsonBlock
			mu.Unlock()
		}(blockNum)
	}

	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}

	return result, nil
}

// parseAndNormalizeToJSON parses raw block bytes and normalizes to JSON format
func (f *Fetcher) parseAndNormalizeToJSON(blockBytes []byte) (*JSONBlock, error) {
	blk, err := block.Parse(block.Codec, blockBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}
	return f.normalizeBlockToJSON(blk)
}

// fetchSingleJSONBlock fetches a single block by height with retry logic and returns JSON format
func (f *Fetcher) fetchSingleJSONBlock(height int64) (*JSONBlock, error) {
	var lastErr error

	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			time.Sleep(delay)
		}

		// Fetch block bytes
		blockBytes, err := f.client.GetBlockByHeight(context.Background(), uint64(height))
		if err != nil {
			lastErr = fmt.Errorf("GetBlockByHeight failed: %w", err)
			continue
		}

		// Cache raw bytes immediately if cache is enabled
		if f.cache != nil {
			_, _ = f.cache.GetCompleteBlock(height, func() ([]byte, error) {
				return blockBytes, nil
			})
		}

		// Parse and normalize to JSON
		jsonBlock, err := f.parseAndNormalizeToJSON(blockBytes)
		if err != nil {
			lastErr = fmt.Errorf("parseAndNormalizeToJSON failed: %w", err)
			continue
		}

		return jsonBlock, nil
	}

	return nil, fmt.Errorf("failed to fetch block %d after %d retries: %w", height, f.maxRetries, lastErr)
}

// GetCurrentValidators fetches current validators for a given subnet with retry logic
func (f *Fetcher) GetCurrentValidators(ctx context.Context, subnetID string) (*GetCurrentValidatorsResponse, error) {
	params := map[string]interface{}{
		"subnetID": subnetID,
	}

	var lastErr error
	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			log.Printf("WARNING: GetCurrentValidators failed for subnet %s: %v. Retrying (attempt %d/%d) after %v",
				subnetID, lastErr, attempt, f.maxRetries, delay)
			time.Sleep(delay)
		}

		var response GetCurrentValidatorsResponse
		err := f.client.Requester.SendRequest(
			ctx,
			"platform.getCurrentValidators",
			params,
			&response,
		)
		if err != nil {
			lastErr = err
			continue
		}
		return &response, nil
	}

	return nil, fmt.Errorf("failed to get current validators for subnet %s after %d retries: %w", subnetID, f.maxRetries, lastErr)
}

// ParseValidatorInfo converts RPC ValidatorInfo to normalized ValidatorState
func ParseValidatorInfo(info ValidatorInfo, subnetID ids.ID) (*ValidatorState, error) {
	// Parse NodeID
	nodeID, err := ids.NodeIDFromString(info.NodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node ID %s: %w", info.NodeID, err)
	}

	// Parse weight using strconv for better handling of large uint64 values
	weight, err := parseUint64Field(info.Weight, "weight")
	if err != nil {
		return nil, err
	}

	// Parse balance (for L1 validators)
	var balance uint64
	if info.Balance != "" {
		balance, err = parseUint64Field(info.Balance, "balance")
		if err != nil {
			return nil, err
		}
	}

	// Parse uptime
	var uptime float64
	if info.Uptime != "" {
		uptime, err = parseFloat64Field(info.Uptime, "uptime")
		if err != nil {
			return nil, err
		}
	}

	// Parse ValidationID (for L1 validators)
	var validationID ids.ID
	if info.ValidationID != "" {
		validationID, err = ids.FromString(info.ValidationID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse validation ID %s: %w", info.ValidationID, err)
		}
	} else {
		// For non-L1 validators, use TxID as ValidationID
		validationID, err = ids.FromString(info.TxID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tx ID %s: %w", info.TxID, err)
		}
	}

	state := &ValidatorState{
		ValidationID: validationID,
		NodeID:       nodeID,
		SubnetID:     subnetID,
		Weight:       weight,
		Balance:      balance,
		StartTime:    time.Unix(int64(info.StartTime), 0),
		EndTime:      time.Unix(int64(info.EndTime), 0),
		Uptime:       uptime,
		Active:       info.Connected,
	}

	return state, nil
}

// parseUint64Field safely parses a string to uint64
func parseUint64Field(value string, fieldName string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s '%s': %w", fieldName, value, err)
	}
	return parsed, nil
}

// parseFloat64Field safely parses a string to float64
func parseFloat64Field(value string, fieldName string) (float64, error) {
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s '%s': %w", fieldName, value, err)
	}
	return parsed, nil
}

// Close stops all background goroutines and cleans up resources
func (f *Fetcher) Close() {
	// P-Chain fetcher doesn't have background goroutines currently,
	// but this method is provided for consistency and future-proofing
}

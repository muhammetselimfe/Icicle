package pchainrpc

import (
	"bytes"
	"icicle/pkg/cache"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/cb58"
	"github.com/ava-labs/avalanchego/utils/formatting/address"
	"github.com/ava-labs/avalanchego/utils/rpc"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/platformvm/block"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
)

// ConvertCB58ToPChainAddress converts a short CB58 address to P-Chain bech32 format
// e.g., "AhRtxbQdas3HyjjTXjL49CgZpF7eaSYCp" -> "P-avax1..."
func ConvertCB58ToPChainAddress(shortAddr string) (string, error) {
	// If already in P-avax format, return as-is
	if strings.HasPrefix(shortAddr, "P-avax") || strings.HasPrefix(shortAddr, "P-fuji") || strings.HasPrefix(shortAddr, "P-local") {
		return shortAddr, nil
	}

	// Decode CB58 to bytes
	addrBytes, err := cb58.Decode(shortAddr)
	if err != nil {
		return "", fmt.Errorf("failed to decode CB58 address %s: %w", shortAddr, err)
	}

	// Format as bech32 with "avax" HRP (mainnet)
	bech32Addr, err := address.FormatBech32("avax", addrBytes)
	if err != nil {
		return "", fmt.Errorf("failed to format bech32 address: %w", err)
	}

	// Prepend P- for P-Chain
	return "P-" + bech32Addr, nil
}

// ConvertCB58ToPChainAddressFuji converts a short CB58 address to P-Chain bech32 format for Fuji testnet
func ConvertCB58ToPChainAddressFuji(shortAddr string) (string, error) {
	if strings.HasPrefix(shortAddr, "P-") {
		return shortAddr, nil
	}

	addrBytes, err := cb58.Decode(shortAddr)
	if err != nil {
		return "", fmt.Errorf("failed to decode CB58 address %s: %w", shortAddr, err)
	}

	bech32Addr, err := address.FormatBech32("fuji", addrBytes)
	if err != nil {
		return "", fmt.Errorf("failed to format bech32 address: %w", err)
	}

	return "P-" + bech32Addr, nil
}

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
	// Apricot blocks store timestamps differently - mark for parent timestamp lookup
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotCommitBlock(b *block.ApricotCommitBlock) error {
	// Apricot blocks store timestamps differently - mark for parent timestamp lookup
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotProposalBlock(b *block.ApricotProposalBlock) error {
	// Apricot blocks store timestamps differently - mark for parent timestamp lookup
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotStandardBlock(b *block.ApricotStandardBlock) error {
	// Apricot blocks store timestamps differently - mark for parent timestamp lookup
	te.timestamp = time.Time{}
	return nil
}

func (te *timestampExtractor) ApricotAtomicBlock(b *block.ApricotAtomicBlock) error {
	// Apricot blocks store timestamps differently - mark for parent timestamp lookup
	te.timestamp = time.Time{}
	return nil
}

// findTimestampForApricotBlock searches backwards to find the most recent AdvanceTimeTx
// that established the chain timestamp for this block
func (f *Fetcher) findTimestampForApricotBlock(currentHeight uint64) (time.Time, error) {
	// Search backwards up to 100 blocks or until we find an AdvanceTimeTx
	// AdvanceTimeTx typically appear every few blocks in Apricot era
	maxLookback := uint64(100)
	startHeight := uint64(0)
	if currentHeight > maxLookback {
		startHeight = currentHeight - maxLookback
	}

	for height := currentHeight; height >= startHeight && height > 0; height-- {
		blockBytes, err := f.client.GetBlockByHeight(context.Background(), height)
		if err != nil {
			continue // Skip errors and keep searching
		}

		blk, err := block.Parse(block.Codec, blockBytes)
		if err != nil {
			continue
		}

		// Check all transactions in this block for AdvanceTimeTx
		for _, tx := range blk.Txs() {
			if tx.Unsigned == nil {
				continue
			}
			if advTimeTx, ok := tx.Unsigned.(*txs.AdvanceTimeTx); ok {
				return advTimeTx.Timestamp(), nil
			}
		}
	}

	// If no AdvanceTimeTx found in lookback window, estimate based on height
	mainnetLaunch := time.Date(2020, 9, 21, 0, 0, 0, 0, time.UTC)
	estimatedSeconds := int64(currentHeight) * 2
	return mainnetLaunch.Add(time.Duration(estimatedSeconds) * time.Second), nil
}

// normalizeBlock converts a platform block to normalized structure
func (f *Fetcher) normalizeBlock(blk block.Block) (*NormalizedBlock, error) {
	// Extract timestamp using visitor pattern
	extractor := &timestampExtractor{}
	if err := blk.Visit(extractor); err != nil {
		return nil, fmt.Errorf("failed to extract timestamp: %w", err)
	}
	blockTime := extractor.timestamp

	// Debug logging for specific block
	if blk.Height() == 1570934 {
		log.Printf("[DEBUG] Block 1570934 - Block type: %T", blk)
		log.Printf("[DEBUG] Block 1570934 - Initial timestamp from visitor: %v (IsZero=%v)", blockTime, blockTime.IsZero())
	}

	// For Apricot blocks (pre-Banff), find exact timestamp from AdvanceTimeTx
	// The P-Chain maintains a chain-level timestamp that's updated via AdvanceTimeTx
	if blockTime.IsZero() {
		// First check if this block itself contains an AdvanceTimeTx
		for _, tx := range blk.Txs() {
			if tx.Unsigned == nil {
				continue
			}
			if advTimeTx, ok := tx.Unsigned.(*txs.AdvanceTimeTx); ok {
				blockTime = advTimeTx.Timestamp()
				if blk.Height() == 1570934 {
					log.Printf("[DEBUG] Block 1570934 - Found AdvanceTimeTx in block: %v", blockTime)
				}
				break
			}
		}

		// If not found, search backwards for the most recent AdvanceTimeTx
		if blockTime.IsZero() && blk.Height() > 0 {
			var err error
			blockTime, err = f.findTimestampForApricotBlock(blk.Height())
			if err != nil {
				return nil, fmt.Errorf("failed to find timestamp for Apricot block: %w", err)
			}
			if blk.Height() == 1570934 {
				log.Printf("[DEBUG] Block 1570934 - Timestamp from backward search: %v", blockTime)
			}
		}
	} else if blk.Height() == 1570934 {
		log.Printf("[DEBUG] Block 1570934 - Using Banff timestamp: %v", blockTime)
	}

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

	// Estimate timestamp for old Apricot blocks
	// Note: Apricot blocks use AdvanceTimeTx for timestamps, which requires complex tracking
	// For simplicity, we estimate based on block height (~2 sec/block average)
	if blockTime.IsZero() && blk.Height() > 0 {
		mainnetLaunch := time.Date(2020, 9, 21, 0, 0, 0, 0, time.UTC)
		estimatedSeconds := int64(blk.Height()) * 2
		blockTime = mainnetLaunch.Add(time.Duration(estimatedSeconds) * time.Second)
	}

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
	// Parse NodeID - handle both CB58 and hex formats
	// L1 validators may return node IDs in hex format (0x prefix or raw hex)
	var nodeID ids.NodeID
	var err error

	nodeIDStr := info.NodeID

	// Check if it's a hex-encoded node ID (common for L1 validators)
	if strings.HasPrefix(nodeIDStr, "0x") || strings.HasPrefix(nodeIDStr, "0X") {
		// Remove 0x prefix and decode hex
		hexStr := strings.TrimPrefix(strings.TrimPrefix(nodeIDStr, "0x"), "0X")
		nodeBytes, hexErr := hex.DecodeString(hexStr)
		if hexErr != nil {
			return nil, fmt.Errorf("failed to decode hex node ID %s: %w", nodeIDStr, hexErr)
		}
		if len(nodeBytes) != 20 {
			return nil, fmt.Errorf("invalid hex node ID length %d (expected 20): %s", len(nodeBytes), nodeIDStr)
		}
		copy(nodeID[:], nodeBytes)
		log.Printf("DEBUG: Converted hex NodeID %s to CB58 %s", nodeIDStr, nodeID.String())
	} else if strings.HasPrefix(nodeIDStr, "NodeID-") {
		// Check if the suffix looks like hex (40 hex chars = 20 bytes)
		suffix := strings.TrimPrefix(nodeIDStr, "NodeID-")
		if len(suffix) == 40 && isHexString(suffix) {
			// It's hex format disguised as NodeID- prefix
			nodeBytes, hexErr := hex.DecodeString(suffix)
			if hexErr == nil && len(nodeBytes) == 20 {
				copy(nodeID[:], nodeBytes)
				log.Printf("DEBUG: Converted hex NodeID %s to CB58 %s", nodeIDStr, nodeID.String())
			} else {
				// Fall back to standard parsing
				nodeID, err = ids.NodeIDFromString(nodeIDStr)
			}
		} else {
			// Standard CB58 format
			nodeID, err = ids.NodeIDFromString(nodeIDStr)
		}
	} else {
		// Try standard parsing
		nodeID, err = ids.NodeIDFromString(nodeIDStr)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to parse node ID %s: %w", nodeIDStr, err)
	}

	// Validate the resulting NodeID string has proper length
	nodeIDResult := nodeID.String()
	if len(nodeIDResult) < 35 {
		return nil, fmt.Errorf("parsed node ID too short (%d chars): input=%s output=%s", len(nodeIDResult), nodeIDStr, nodeIDResult)
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

	// Determine active status:
	// - For L1 validators (have ValidationID): active if balance > 0
	// - For non-L1 validators: active if returned by getCurrentValidators
	isL1Validator := info.ValidationID != ""
	active := true
	if isL1Validator && balance == 0 {
		active = false // L1 validators with 0 balance cannot validate
	}

	state := &ValidatorState{
		ValidationID: validationID,
		NodeID:       nodeID,
		SubnetID:     subnetID,
		Weight:       weight,
		Balance:      balance,
		Uptime:       uptime,
		Active:       active,
	}

	// Parse StartTime
	if info.StartTime != "" {
		startTime, err := parseUint64Field(info.StartTime, "startTime")
		if err != nil {
			return nil, err
		}
		state.StartTime = time.Unix(int64(startTime), 0)
	}

	// Parse EndTime
	if info.EndTime != "" {
		endTime, err := parseUint64Field(info.EndTime, "endTime")
		if err != nil {
			return nil, err
		}
		state.EndTime = time.Unix(int64(endTime), 0)
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

// isHexString checks if a string contains only hexadecimal characters
func isHexString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// Close stops all background goroutines and cleans up resources
func (f *Fetcher) Close() {
	// P-Chain fetcher doesn't have background goroutines currently,
	// but this method is provided for consistency and future-proofing
}

// GetUTXOsResponse represents the response from platform.getUTXOs
type GetUTXOsResponse struct {
	NumFetched string   `json:"numFetched"`
	UTXOs      []string `json:"utxos"`
	EndIndex   struct {
		Address string `json:"address"`
		UTXO    string `json:"utxo"`
	} `json:"endIndex"`
	Encoding string `json:"encoding"`
}

// GetUTXOs fetches UTXOs for the given addresses
func (f *Fetcher) GetUTXOs(ctx context.Context, addresses []string) (*GetUTXOsResponse, error) {
	params := map[string]interface{}{
		"addresses": addresses,
		"limit":     1024,
		"encoding":  "hex",
	}

	var lastErr error
	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			time.Sleep(delay)
		}

		var response GetUTXOsResponse
		err := f.client.Requester.SendRequest(
			ctx,
			"platform.getUTXOs",
			params,
			&response,
		)
		if err != nil {
			lastErr = err
			continue
		}
		return &response, nil
	}

	return nil, fmt.Errorf("failed to get UTXOs after %d retries: %w", f.maxRetries, lastErr)
}

// ParsedUTXO represents a parsed UTXO with its components
type ParsedUTXO struct {
	TxID        ids.ID
	OutputIndex uint32
	Amount      uint64
}

// ParseUTXOHex parses a hex-encoded UTXO and extracts txID, outputIndex, and amount
func ParseUTXOHex(utxoHex string) (*ParsedUTXO, error) {
	// Remove 0x prefix if present
	utxoHex = strings.TrimPrefix(utxoHex, "0x")

	utxoBytes, err := hex.DecodeString(utxoHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode UTXO hex: %w", err)
	}

	// UTXO format: codecID (2) + txID (32) + outputIndex (4) + output
	if len(utxoBytes) < 38 {
		return nil, fmt.Errorf("UTXO too short: %d bytes", len(utxoBytes))
	}

	// Extract txID (bytes 2-34)
	var txID ids.ID
	copy(txID[:], utxoBytes[2:34])

	// Extract outputIndex (bytes 34-38, big endian)
	outputIndex := uint32(utxoBytes[34])<<24 | uint32(utxoBytes[35])<<16 | uint32(utxoBytes[36])<<8 | uint32(utxoBytes[37])

	// Extract amount from output
	// Output format: assetID (32) + typeID (4) + amount (8) + ...
	// Amount is at offset 38 + 32 + 4 = 74
	if len(utxoBytes) < 82 {
		return nil, fmt.Errorf("UTXO too short for amount: %d bytes", len(utxoBytes))
	}

	amountBytes := utxoBytes[74:82]
	amount := uint64(amountBytes[0])<<56 | uint64(amountBytes[1])<<48 | uint64(amountBytes[2])<<40 | uint64(amountBytes[3])<<32 |
		uint64(amountBytes[4])<<24 | uint64(amountBytes[5])<<16 | uint64(amountBytes[6])<<8 | uint64(amountBytes[7])

	return &ParsedUTXO{
		TxID:        txID,
		OutputIndex: outputIndex,
		Amount:      amount,
	}, nil
}

// FindRefundUTXO searches UTXOs for a specific txID and outputIndex
func (f *Fetcher) FindRefundUTXO(ctx context.Context, addresses []string, targetTxID ids.ID, targetOutputIndex uint32) (*ParsedUTXO, error) {
	response, err := f.GetUTXOs(ctx, addresses)
	if err != nil {
		return nil, err
	}

	for _, utxoHex := range response.UTXOs {
		parsed, err := ParseUTXOHex(utxoHex)
		if err != nil {
			continue // Skip malformed UTXOs
		}

		if parsed.TxID == targetTxID && parsed.OutputIndex == targetOutputIndex {
			return parsed, nil
		}
	}

	return nil, fmt.Errorf("refund UTXO not found for txID %s outputIndex %d", targetTxID, targetOutputIndex)
}

// GetL1ValidatorResponse represents the response from platform.getL1Validator
type GetL1ValidatorResponse struct {
	NodeID               string `json:"nodeID"`
	Weight               string `json:"weight"`
	StartTime            string `json:"startTime"`
	ValidationID         string `json:"validationID"`
	PublicKey            string `json:"publicKey"`
	RemainingBalanceOwner struct {
		Locktime  string   `json:"locktime"`
		Threshold string   `json:"threshold"`
		Addresses []string `json:"addresses"`
	} `json:"remainingBalanceOwner"`
	DeactivationOwner struct {
		Locktime  string   `json:"locktime"`
		Threshold string   `json:"threshold"`
		Addresses []string `json:"addresses"`
	} `json:"deactivationOwner"`
	MinNonce  string `json:"minNonce"`
	Balance   string `json:"balance"`
	SubnetID  string `json:"subnetID"`
	Height    string `json:"height"`
}

// GetL1Validator fetches L1 validator info including remainingBalanceOwner
func (f *Fetcher) GetL1Validator(ctx context.Context, validationID string) (*GetL1ValidatorResponse, error) {
	params := map[string]interface{}{
		"validationID": validationID,
	}

	var lastErr error
	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			delay := f.retryDelay * time.Duration(1<<uint(attempt-1))
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			time.Sleep(delay)
		}

		var response GetL1ValidatorResponse
		err := f.client.Requester.SendRequest(
			ctx,
			"platform.getL1Validator",
			params,
			&response,
		)
		if err != nil {
			lastErr = err
			continue
		}
		return &response, nil
	}

	return nil, fmt.Errorf("failed to get L1 validator %s after %d retries: %w", validationID, f.maxRetries, lastErr)
}

package pchainrpc

import (
	"clickhouse-metrics-poc/pkg/cache"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

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

	f := &Fetcher{
		client:     platformvm.NewClient(opts.RpcURL),
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

package pchainsyncer

import (
	"clickhouse-metrics-poc/pkg/pchainrpc"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanchego/ids"
)

// ValidatorSyncerConfig configures the validator state syncer
type ValidatorSyncerConfig struct {
	PChainID      uint32
	SyncInterval  time.Duration // How often to sync validator state
	DiscoveryMode string        // "auto" or "manual"
}

// ValidatorSyncer periodically syncs L1 validator state
type ValidatorSyncer struct {
	config   ValidatorSyncerConfig
	fetcher  *pchainrpc.Fetcher
	conn     clickhouse.Conn
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewValidatorSyncer creates a new validator state syncer
func NewValidatorSyncer(config ValidatorSyncerConfig, fetcher *pchainrpc.Fetcher, conn clickhouse.Conn) *ValidatorSyncer {
	if config.SyncInterval == 0 {
		config.SyncInterval = 5 * time.Minute // Default: sync every 5 minutes
	}
	if config.DiscoveryMode == "" {
		config.DiscoveryMode = "auto"
	}

	return &ValidatorSyncer{
		config:  config,
		fetcher: fetcher,
		conn:    conn,
		stopCh:  make(chan struct{}),
	}
}

// Start begins the periodic sync process
func (vs *ValidatorSyncer) Start(ctx context.Context) {
	log.Printf("Starting L1 validator state syncer (interval: %v, discovery: %s)", vs.config.SyncInterval, vs.config.DiscoveryMode)

	// Do initial sync immediately
	if err := vs.syncOnce(ctx); err != nil {
		log.Printf("ERROR: Initial validator state sync failed: %v", err)
	}

	// Start periodic sync
	ticker := time.NewTicker(vs.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := vs.syncOnce(ctx); err != nil {
				log.Printf("ERROR: Validator state sync failed: %v", err)
			}
		case <-vs.stopCh:
			log.Println("Stopping L1 validator state syncer")
			return
		case <-ctx.Done():
			log.Println("Context cancelled, stopping validator state syncer")
			return
		}
	}
}

// Stop stops the syncer (safe to call multiple times)
func (vs *ValidatorSyncer) Stop() {
	vs.stopOnce.Do(func() {
		close(vs.stopCh)
	})
}

// syncOnce performs a single sync cycle
func (vs *ValidatorSyncer) syncOnce(ctx context.Context) error {
	startTime := time.Now()
	log.Println("Starting validator state sync cycle...")

	// Step 1: Discover L1 subnets
	subnets, err := vs.discoverL1Subnets(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover L1 subnets: %w", err)
	}

	if len(subnets) == 0 {
		log.Println("No L1 subnets found to sync")
		return nil
	}

	log.Printf("Found %d L1 subnet(s) to sync", len(subnets))

	// Step 2: For each subnet, fetch and update validator state
	totalValidators := 0
	for _, subnet := range subnets {
		validatorCount, err := vs.syncSubnetValidators(ctx, subnet)
		if err != nil {
			log.Printf("WARNING: Failed to sync validators for subnet %s: %v", subnet, err)
			continue
		}
		totalValidators += validatorCount
	}

	duration := time.Since(startTime)
	log.Printf("Validator state sync completed: %d validators across %d subnets in %v", totalValidators, len(subnets), duration)

	return nil
}

// discoverL1Subnets discovers L1 subnets based on the configured discovery mode
func (vs *ValidatorSyncer) discoverL1Subnets(ctx context.Context) ([]ids.ID, error) {
	switch vs.config.DiscoveryMode {
	case "auto":
		// Discover from transactions and update l1_subnets table
		subnets, err := DiscoverL1SubnetsFromTransactions(ctx, vs.conn, vs.config.PChainID)
		if err != nil {
			return nil, fmt.Errorf("failed to discover subnets from transactions: %w", err)
		}

		// Update l1_subnets table
		if len(subnets) > 0 {
			if err := InsertL1Subnets(ctx, vs.conn, subnets); err != nil {
				return nil, fmt.Errorf("failed to insert L1 subnets: %w", err)
			}
		}

		// Return subnet IDs
		subnetIDs := make([]ids.ID, len(subnets))
		for i, subnet := range subnets {
			subnetIDs[i] = subnet.SubnetID
		}
		return subnetIDs, nil

	case "manual":
		// Read from l1_subnets table (manually configured)
		return GetL1Subnets(ctx, vs.conn, vs.config.PChainID)

	default:
		return nil, fmt.Errorf("unknown discovery mode: %s", vs.config.DiscoveryMode)
	}
}

// syncSubnetValidators fetches and syncs validator state for a specific subnet
func (vs *ValidatorSyncer) syncSubnetValidators(ctx context.Context, subnetID ids.ID) (int, error) {
	// Fetch current validators from RPC
	response, err := vs.fetcher.GetCurrentValidators(ctx, subnetID.String())
	if err != nil {
		return 0, fmt.Errorf("failed to fetch validators: %w", err)
	}

	if len(response.Validators) == 0 {
		log.Printf("No validators found for subnet %s", subnetID)
		return 0, nil
	}

	// Parse validator info into ValidatorState
	states := make([]*pchainrpc.ValidatorState, 0, len(response.Validators))
	for _, validatorInfo := range response.Validators {
		state, err := pchainrpc.ParseValidatorInfo(validatorInfo, subnetID)
		if err != nil {
			log.Printf("WARNING: Failed to parse validator info for %s: %v", validatorInfo.NodeID, err)
			continue
		}
		states = append(states, state)
	}

	// Insert into database
	if len(states) > 0 {
		if err := InsertValidatorStates(ctx, vs.conn, vs.config.PChainID, states); err != nil {
			return 0, fmt.Errorf("failed to insert validator states: %w", err)
		}
	}

	log.Printf("Synced %d validators for subnet %s", len(states), subnetID)
	return len(states), nil
}

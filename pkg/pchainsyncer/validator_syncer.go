package pchainsyncer

import (
	"icicle/pkg/pchainrpc"
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

	// Step 0: Insert Primary Network (genesis subnet) if first run
	if err := InsertPrimaryNetwork(ctx, vs.conn, vs.config.PChainID); err != nil {
		// Ignore duplicate key errors (already exists)
		log.Printf("Primary Network already exists or error: %v", err)
	}
	if err := InsertPrimaryNetworkChains(ctx, vs.conn, vs.config.PChainID); err != nil {
		log.Printf("Primary Network chains already exist or error: %v", err)
	}

	// Step 1: Discover and populate all subnets
	allSubnets, err := DiscoverAllSubnets(ctx, vs.conn, vs.config.PChainID)
	if err != nil {
		return fmt.Errorf("failed to discover all subnets: %w", err)
	}

	if len(allSubnets) > 0 {
		if err := InsertSubnets(ctx, vs.conn, allSubnets); err != nil {
			return fmt.Errorf("failed to insert subnets: %w", err)
		}
		log.Printf("Discovered and updated %d total subnet(s)", len(allSubnets))
	}

	// Step 2: Discover and populate subnet chains
	chains, err := DiscoverSubnetChains(ctx, vs.conn, vs.config.PChainID)
	if err != nil {
		return fmt.Errorf("failed to discover subnet chains: %w", err)
	}

	if len(chains) > 0 {
		if err := InsertSubnetChains(ctx, vs.conn, chains); err != nil {
			return fmt.Errorf("failed to insert subnet chains: %w", err)
		}
		log.Printf("Discovered and updated %d subnet chain(s)", len(chains))
	}

	// Step 2.5: Discover and populate historical L1 validators from transactions
	historicalValidators, err := DiscoverL1ValidatorHistory(ctx, vs.conn, vs.config.PChainID)
	if err != nil {
		log.Printf("WARNING: Failed to discover historical L1 validators: %v", err)
	} else if len(historicalValidators) > 0 {
		if err := InsertL1ValidatorHistory(ctx, vs.conn, historicalValidators); err != nil {
			log.Printf("WARNING: Failed to insert historical L1 validators: %v", err)
		} else {
			log.Printf("Discovered %d historical L1 validators from transactions", len(historicalValidators))
		}
	}

	// Step 3: Sync Primary Network validators
	primarySubnetID, _ := ids.FromString("11111111111111111111111111111111LpoYY")
	primaryValidatorCount, err := vs.syncSubnetValidators(ctx, primarySubnetID)
	if err != nil {
		log.Printf("WARNING: Failed to sync Primary Network validators: %v", err)
	} else {
		log.Printf("Synced %d Primary Network validators", primaryValidatorCount)
	}

	// Step 4: Discover L1 subnets for validator syncing
	l1Subnets, err := vs.discoverL1Subnets(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover L1 subnets: %w", err)
	}

	log.Printf("Found %d L1 subnet(s) to sync validators", len(l1Subnets))

	// Step 5: Discover regular/elastic subnets for validator syncing
	regularSubnets, err := vs.discoverRegularSubnets(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover regular subnets: %w", err)
	}

	log.Printf("Found %d regular/elastic subnet(s) to sync validators", len(regularSubnets))

	// Step 6: For each L1 subnet, fetch and update validator state
	totalValidators := primaryValidatorCount
	l1ValidatorCount := 0
	for _, subnet := range l1Subnets {
		validatorCount, err := vs.syncSubnetValidators(ctx, subnet)
		if err != nil {
			log.Printf("WARNING: Failed to sync validators for subnet %s: %v", subnet, err)
			continue
		}
		l1ValidatorCount += validatorCount
		totalValidators += validatorCount
	}

	// Step 7: For each regular subnet, fetch and update validator state
	regularValidatorCount := 0
	for _, subnet := range regularSubnets {
		validatorCount, err := vs.syncSubnetValidators(ctx, subnet)
		if err != nil {
			log.Printf("WARNING: Failed to sync validators for subnet %s: %v", subnet, err)
			continue
		}
		regularValidatorCount += validatorCount
		totalValidators += validatorCount
	}

	// Step 8: Sync balance transactions for L1 validators
	if err := SyncL1ValidatorBalanceTxs(ctx, vs.conn, vs.config.PChainID); err != nil {
		log.Printf("WARNING: Failed to sync L1 validator balance transactions: %v", err)
	}

	// Step 9: Sync validator refunds (from DisableL1Validator transactions)
	if err := SyncL1ValidatorRefunds(ctx, vs.conn, vs.fetcher, vs.config.PChainID); err != nil {
		log.Printf("WARNING: Failed to sync L1 validator refunds: %v", err)
	}

	// Step 10: Calculate and update L1 fee statistics
	feeStats, err := CalculateL1FeeStats(ctx, vs.conn, vs.config.PChainID)
	if err != nil {
		log.Printf("WARNING: Failed to calculate L1 fee stats: %v", err)
	} else if len(feeStats) > 0 {
		if err := InsertL1FeeStats(ctx, vs.conn, feeStats); err != nil {
			log.Printf("WARNING: Failed to insert L1 fee stats: %v", err)
		} else {
			log.Printf("Updated fee stats for %d L1 subnet(s)", len(feeStats))
		}
	}

	// Step 11: Update per-validator fee statistics
	if err := UpdatePerValidatorFeeStats(ctx, vs.conn, vs.config.PChainID); err != nil {
		log.Printf("WARNING: Failed to update per-validator fee stats: %v", err)
	}

	duration := time.Since(startTime)
	log.Printf("Validator state sync completed: %d validators (%d Primary Network, %d across %d L1 subnets, %d across %d regular subnets) in %v",
		totalValidators, primaryValidatorCount, l1ValidatorCount, len(l1Subnets), regularValidatorCount, len(regularSubnets), duration)

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

// discoverRegularSubnets discovers regular and elastic subnets from the subnets table
func (vs *ValidatorSyncer) discoverRegularSubnets(ctx context.Context) ([]ids.ID, error) {
	query := `
		SELECT DISTINCT subnet_id
		FROM subnets FINAL
		WHERE p_chain_id = ? AND subnet_type IN ('regular', 'elastic')
		ORDER BY created_time ASC
	`

	rows, err := vs.conn.Query(ctx, query, vs.config.PChainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query regular subnets: %w", err)
	}
	defer rows.Close()

	var subnetIDs []ids.ID
	for rows.Next() {
		var subnetIDStr string
		if err := rows.Scan(&subnetIDStr); err != nil {
			return nil, fmt.Errorf("failed to scan subnet ID: %w", err)
		}

		subnetID, err := ids.FromString(subnetIDStr)
		if err != nil {
			log.Printf("WARNING: Failed to parse subnet ID %s: %v", subnetIDStr, err)
			continue
		}

		subnetIDs = append(subnetIDs, subnetID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return subnetIDs, nil
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
	activeValidationIDs := make([]string, 0, len(response.Validators))
	for _, validatorInfo := range response.Validators {
		state, err := pchainrpc.ParseValidatorInfo(validatorInfo, subnetID)
		if err != nil {
			log.Printf("WARNING: Failed to parse validator info for %s: %v", validatorInfo.NodeID, err)
			continue
		}
		states = append(states, state)
		activeValidationIDs = append(activeValidationIDs, state.ValidationID.String())
	}

	// Insert into database
	if len(states) > 0 {
		if err := InsertValidatorStates(ctx, vs.conn, vs.config.PChainID, states); err != nil {
			return 0, fmt.Errorf("failed to insert validator states: %w", err)
		}
	}

	// Mark validators that are no longer in the RPC response as inactive
	// This handles validators whose staking period has ended
	if err := MarkInactiveValidators(ctx, vs.conn, vs.config.PChainID, subnetID.String(), activeValidationIDs); err != nil {
		log.Printf("WARNING: Failed to mark inactive validators for subnet %s: %v", subnetID, err)
		// Don't return error - this is not critical, just log it
	}

	log.Printf("Synced %d validators for subnet %s", len(states), subnetID)
	return len(states), nil
}

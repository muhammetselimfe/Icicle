package cmd

import (
	"icicle/pkg/cache"
	"icicle/pkg/evmsyncer"
	"icicle/pkg/pchainsyncer"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"gopkg.in/yaml.v3"
)

// ChainConfig represents configuration for any blockchain VM type
type ChainConfig struct {
	ChainID        uint32 `yaml:"chainID"`
	VM             string `yaml:"vm"` // "evm", "p", "x", etc.
	RpcURL         string `yaml:"rpcURL"`
	StartBlock     int64  `yaml:"startBlock"`
	FetchBatchSize int    `yaml:"fetchBatchSize"`
	MaxConcurrency int    `yaml:"maxConcurrency"`
	Name           string `yaml:"name"`

	// EVM-specific config for RPC batching
	RpcBatchSize   int `yaml:"rpcBatchSize"`   // RPC calls per HTTP request (default: 100)
	DebugBatchSize int `yaml:"debugBatchSize"` // Debug/trace calls per HTTP request (default: 15)

	// P-chain specific config
	EnableValidatorSync   bool `yaml:"enableValidatorSync"`   // Enable L1 validator state syncing
	ValidatorSyncInterval int  `yaml:"validatorSyncInterval"` // Validator sync interval in minutes (default: 5)
}

// Syncer interface for all chain syncers
type Syncer interface {
	Start() error
	Wait()
	Stop()
}

// LoadConfig loads and parses the YAML configuration file
func LoadConfig(path string) ([]ChainConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var configs []ChainConfig
	if err := yaml.Unmarshal(data, &configs); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Validate configurations
	for i, cfg := range configs {
		if cfg.ChainID == 0 && cfg.VM != "p" {
			return nil, fmt.Errorf("chain at index %d: chainID cannot be 0 for non-P-chain VMs", i)
		}
		if cfg.VM == "" {
			return nil, fmt.Errorf("chain at index %d: VM type is required", i)
		}
		if cfg.RpcURL == "" {
			return nil, fmt.Errorf("chain at index %d: rpcURL is required", i)
		}
		if cfg.Name == "" {
			return nil, fmt.Errorf("chain at index %d: name is required", i)
		}
	}

	return configs, nil
}

// CreateSyncer creates the appropriate syncer based on VM type
func CreateSyncer(cfg ChainConfig, conn driver.Conn, cacheInstance *cache.Cache, fast bool) (Syncer, error) {
	switch cfg.VM {
	case "evm":
		return evmsyncer.NewChainSyncer(evmsyncer.Config{
			ChainID:        cfg.ChainID,
			RpcURL:         cfg.RpcURL,
			StartBlock:     cfg.StartBlock,
			MaxConcurrency: cfg.MaxConcurrency,
			CHConn:         conn,
			Cache:          cacheInstance,
			FetchBatchSize: cfg.FetchBatchSize,
			RpcBatchSize:   cfg.RpcBatchSize,
			DebugBatchSize: cfg.DebugBatchSize,
			Name:           cfg.Name,
			Fast:           fast,
		})

	case "p":
		// Convert validator sync interval from minutes to duration
		validatorSyncInterval := time.Duration(cfg.ValidatorSyncInterval) * time.Minute
		if cfg.ValidatorSyncInterval == 0 {
			validatorSyncInterval = 5 * time.Minute // Default: 5 minutes
		}

		return pchainsyncer.NewPChainSyncer(pchainsyncer.Config{
			RpcURL:                cfg.RpcURL,
			StartBlock:            cfg.StartBlock,
			MaxConcurrency:        cfg.MaxConcurrency,
			FetchBatchSize:        cfg.FetchBatchSize,
			CHConn:                conn,
			Cache:                 cacheInstance,
			ChainID:               cfg.ChainID,
			Name:                  cfg.Name,
			EnableValidatorSync:   cfg.EnableValidatorSync,
			ValidatorSyncInterval: validatorSyncInterval,
		})

	default:
		return nil, fmt.Errorf("unsupported VM type: %s", cfg.VM)
	}
}

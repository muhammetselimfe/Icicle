package cmd

import (
	"clickhouse-metrics-poc/pkg/cache"
	"clickhouse-metrics-poc/pkg/chwrapper"
	"clickhouse-metrics-poc/pkg/evmsyncer"
	"clickhouse-metrics-poc/pkg/pchainsyncer"
	"encoding/json"
	"log"
	"os"
	"sync"
)

type ChainConfig struct {
	ChainID        uint32 `json:"chainID"`
	RpcURL         string `json:"rpcURL"`
	StartBlock     int64  `json:"startBlock,omitempty"`
	MaxConcurrency int    `json:"maxConcurrency,omitempty"`
	FetchBatchSize int    `json:"fetchBatchSize,omitempty"`
	Name           string `json:"name"`
}

type Config struct {
	EvmChains        []ChainConfig `json:"evmChains,omitempty"`
	PChainRpcURL     string        `json:"pChainRpcURL,omitempty"`
	PChainStartBlock int64         `json:"pChainStartBlock,omitempty"`
}

func RunIngest() {
	log.Println("Starting ingest...")
	// Load configuration
	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read config.json: %v", err)
	}

	// Try to parse as new format first
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Failed to parse config.json: %v", err)
	}

	// Support legacy format (array of chain configs)
	var evmConfigs []ChainConfig
	if len(config.EvmChains) == 0 && config.PChainRpcURL == "" {
		// Try parsing as legacy array format
		if err := json.Unmarshal(configData, &evmConfigs); err != nil {
			log.Fatalf("Failed to parse config.json as legacy format: %v", err)
		}
		config.EvmChains = evmConfigs
	}

	if len(config.EvmChains) == 0 && config.PChainRpcURL == "" {
		log.Fatal("No chain configurations found in config.json")
	}

	// Validate EVM configuration
	for _, cfg := range config.EvmChains {
		if cfg.Name == "" {
			log.Fatalf("Chain %d has empty name - name is required", cfg.ChainID)
		}
	}

	// Connect to ClickHouse
	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	err = chwrapper.CreateTables(conn)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	var wg sync.WaitGroup

	// Start P-chain syncer if configured
	if config.PChainRpcURL != "" {
		log.Printf("Starting P-chain syncer with RPC URL: %s", config.PChainRpcURL)

		// Create cache for P-chain (using chain ID 0)
		pChainCache, err := cache.New("./rpc_cache", 0)
		if err != nil {
			log.Fatalf("Failed to create cache for P-chain: %v", err)
		}
		defer pChainCache.Close()

		// Create P-chain syncer
		pChainStartBlock := config.PChainStartBlock
		if pChainStartBlock == 0 {
			pChainStartBlock = 1 // Default to block 1 if not specified
		}

		pChainSyncer, err := pchainsyncer.NewPChainSyncer(pchainsyncer.Config{
			RpcURL:         config.PChainRpcURL,
			StartBlock:     pChainStartBlock,
			MaxConcurrency: 50,
			FetchBatchSize: 100,
			CHConn:         conn,
			Cache:          pChainCache,
		})
		if err != nil {
			log.Fatalf("Failed to create P-chain syncer: %v", err)
		}

		wg.Add(1)
		go func(ps *pchainsyncer.PChainSyncer) {
			defer wg.Done()
			if err := ps.Start(); err != nil {
				log.Printf("Failed to start P-chain syncer: %v", err)
			}
			ps.Wait()
		}(pChainSyncer)

		log.Printf("Started P-chain syncer")
	}

	// Start a syncer for each EVM chain
	for _, cfg := range config.EvmChains {
		// Create cache
		cacheInstance, err := cache.New("./rpc_cache", cfg.ChainID)
		if err != nil {
			log.Fatalf("Failed to create cache for chain %d: %v", cfg.ChainID, err)
		}
		defer cacheInstance.Close()

		// TODO: delete this after testing
		// go func(c *cache.Cache, chainID uint32) {
		// 	log.Printf("Starting background compaction for chain %d...", chainID)
		// 	if err := c.Compact(); err != nil {
		// 		log.Printf("Background compaction failed for chain %d: %v", chainID, err)
		// 	} else {
		// 		log.Printf("Background compaction completed for chain %d", chainID)
		// 	}
		// }(cacheInstance, cfg.ChainID)

		// Create syncer
		chainSyncer, err := evmsyncer.NewChainSyncer(evmsyncer.Config{
			ChainID:        cfg.ChainID,
			RpcURL:         cfg.RpcURL,
			StartBlock:     cfg.StartBlock,
			MaxConcurrency: cfg.MaxConcurrency,
			CHConn:         conn,
			Cache:          cacheInstance,
			FetchBatchSize: cfg.FetchBatchSize,
			Name:           cfg.Name,
		})
		if err != nil {
			log.Fatalf("Failed to create syncer for chain %d: %v", cfg.ChainID, err)
		}

		wg.Add(1)
		go func(cs *evmsyncer.ChainSyncer, chainID uint32) {
			defer wg.Done()
			if err := cs.Start(); err != nil {
				log.Printf("Failed to start syncer for chain %d: %v", chainID, err)
			}
			cs.Wait()
		}(chainSyncer, cfg.ChainID)

		log.Printf("Started syncer for chain %d", cfg.ChainID)
	}

	wg.Wait()
}

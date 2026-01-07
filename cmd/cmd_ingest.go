package cmd

import (
	"icicle/pkg/cache"
	"icicle/pkg/chwrapper"
	"icicle/pkg/registrysyncer"
	"context"
	"log"
	"sync"
)

func RunIngest(fast bool) {
	if fast {
		log.Println("Starting ingest in FAST mode (indexers disabled)...")
	} else {
		log.Println("Starting ingest...")
	}

	// Load configuration from YAML
	configs, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(configs) == 0 {
		log.Fatal("No chain configurations found in config.yaml")
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

	// Sync L1 Registry at startup (in background)
	go func() {
		if err := registrysyncer.SyncRegistry(context.Background(), conn); err != nil {
			log.Printf("Failed to sync L1 registry: %v", err)
		}
	}()

	var wg sync.WaitGroup

	// Start a syncer for each chain
	for _, cfg := range configs {
		// Create cache
		cacheInstance, err := cache.New("./rpc_cache", cfg.ChainID)
		if err != nil {
			log.Fatalf("Failed to create cache for chain %d: %v", cfg.ChainID, err)
		}
		defer cacheInstance.Close()

		// Create syncer based on VM type
		syncer, err := CreateSyncer(cfg, conn, cacheInstance, fast)
		if err != nil {
			log.Fatalf("Failed to create syncer for chain %d (%s): %v", cfg.ChainID, cfg.VM, err)
		}

		wg.Add(1)
		go func(s Syncer, chainID uint32, chainName string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Chain %d] PANIC RECOVERED: %v", chainID, r)
				}
				log.Printf("[Chain %d] Syncer goroutine exiting!", chainID)
				wg.Done()
			}()
			if err := s.Start(); err != nil {
				log.Printf("Failed to start syncer for chain %d (%s): %v", chainID, chainName, err)
			}
			s.Wait()
			log.Printf("[Chain %d] Wait() returned - syncer stopped", chainID)
		}(syncer, cfg.ChainID, cfg.Name)

		log.Printf("Started syncer for chain %d (%s - %s)", cfg.ChainID, cfg.Name, cfg.VM)
	}

	wg.Wait()
	log.Println("All syncers stopped - RunIngest() returning")
}

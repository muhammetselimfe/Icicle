package registrysyncer

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	NpmTarballURL = "https://registry.npmjs.org/l1beat-l1-registry/-/l1beat-l1-registry-latest.tgz"
	NpmMetaURL    = "https://registry.npmjs.org/l1beat-l1-registry"
)

// getLatestTarballURL fetches the npm registry metadata to get the latest tarball URL
func getLatestTarballURL(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", NpmMetaURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch npm metadata: %w", err)
	}
	defer resp.Body.Close()

	var meta struct {
		DistTags struct {
			Latest string `json:"latest"`
		} `json:"dist-tags"`
		Versions map[string]struct {
			Dist struct {
				Tarball string `json:"tarball"`
			} `json:"dist"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return "", fmt.Errorf("failed to parse npm metadata: %w", err)
	}

	latest := meta.DistTags.Latest
	if v, ok := meta.Versions[latest]; ok {
		return v.Dist.Tarball, nil
	}
	return "", fmt.Errorf("latest version %s not found in npm metadata", latest)
}

// SyncRegistry downloads the l1beat-l1-registry npm package and ingests metadata into ClickHouse
func SyncRegistry(ctx context.Context, conn clickhouse.Conn) error {
	slog.Info("Starting L1 registry sync from npm package")

	// Get latest tarball URL
	tarballURL, err := getLatestTarballURL(ctx)
	if err != nil {
		slog.Warn("Failed to get latest tarball URL, using fallback", "error", err)
		tarballURL = NpmTarballURL
	}
	slog.Info("Downloading registry tarball", "url", tarballURL)

	// Download tarball
	req, err := http.NewRequestWithContext(ctx, "GET", tarballURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download tarball: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d downloading tarball", resp.StatusCode)
	}

	// Parse tarball (gzip -> tar)
	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var chains []ChainRegistry

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar entry: %w", err)
		}

		// npm tarballs have a "package/" prefix; look for data/*/chain.json
		name := header.Name
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if filepath.Base(name) != "chain.json" || !strings.Contains(name, "/data/") {
			continue
		}
		// Skip template directory
		if strings.Contains(name, "/_template/") {
			continue
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			slog.Warn("Failed to read registry file", "path", name, "error", err)
			continue
		}

		var chain ChainRegistry
		if err := json.Unmarshal(content, &chain); err != nil {
			slog.Warn("Failed to parse registry file", "path", name, "error", err)
			continue
		}

		if chain.SubnetID != "" {
			chains = append(chains, chain)
		}
	}

	slog.Info("Found chains metadata", "count", len(chains))

	if len(chains) > 0 {
		if err := insertRegistryData(ctx, conn, chains); err != nil {
			return fmt.Errorf("failed to insert registry data: %w", err)
		}
	}

	slog.Info("Registry sync completed successfully")
	return nil
}

func insertRegistryData(ctx context.Context, conn clickhouse.Conn, registries []ChainRegistry) error {
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO l1_registry (
		blockchain_id, subnet_id, name, description, logo_url, website_url,
		network, is_l1, categories, socials,
		evm_chain_id, rpc_url, explorer_url, sybil_resistance_type,
		network_token_name, network_token_symbol, network_token_decimals, network_token_logo_uri,
		staking_weight_factor, staking_token_symbol,
		last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer func() { _ = batch.Abort() }()

	now := time.Now()
	for _, reg := range registries {
		socialsJSON, _ := json.Marshal(reg.Socials)

		// Insert one row per chain in the registry entry
		for _, c := range reg.Chains {
			if c.BlockchainID == "" {
				continue
			}

			var rpcURL string
			if len(c.RpcURLs) > 0 {
				rpcURL = c.RpcURLs[0]
			}

			tokenDecimals := c.NativeToken.Decimals
			if tokenDecimals == 0 {
				tokenDecimals = 18
			}

			err = batch.Append(
				c.BlockchainID,
				reg.SubnetID,
				reg.Name,
				reg.Description,
				reg.Logo,
				reg.Website,
				reg.Network,
				reg.IsL1,
				reg.Categories,
				string(socialsJSON),
				c.EvmChainID,
				rpcURL,
				c.ExplorerURL,
				c.SybilResistanceType,
				c.NativeToken.Name,
				c.NativeToken.Symbol,
				tokenDecimals,
				c.NativeToken.LogoURI,
				c.Staking.WeightFactor,
				c.Staking.TokenSymbol,
				now,
			)
			if err != nil {
				return fmt.Errorf("failed to append chain %s/%s: %w", reg.Name, c.BlockchainID, err)
			}
		}
	}

	return batch.Send()
}

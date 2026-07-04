package evmindexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"icicle/pkg/chwrapper"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TokenMetadataFetcher fetches ERC-20 token metadata via RPC
type TokenMetadataFetcher struct {
	chainID    uint32
	conn       driver.Conn
	rpcURL     string
	httpClient *http.Client
}

// TokenMetadata holds the metadata for an ERC-20 token
type TokenMetadata struct {
	Token    []byte
	Name     string
	Symbol   string
	Decimals uint8
}

// ERC-20 function selectors
const (
	nameSelector     = "0x06fdde03" // name()
	symbolSelector   = "0x95d89b41" // symbol()
	decimalsSelector = "0x313ce567" // decimals()
)

// NewTokenMetadataFetcher creates a new token metadata fetcher
func NewTokenMetadataFetcher(chainID uint32, conn driver.Conn, rpcURL string) *TokenMetadataFetcher {
	return &TokenMetadataFetcher{
		chainID: chainID,
		conn:    conn,
		rpcURL:  rpcURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchMissingMetadata finds tokens without metadata and fetches it
func (f *TokenMetadataFetcher) FetchMissingMetadata(limit int) (int, error) {
	// Bounded so the query-send can't block forever on a severed pooled
	// connection and drain the shared pool (2026-07-04 stall). This heavy
	// SELECT over erc20_balance_changes runs every 10s in the indexer loop.
	ctx, cancel := chwrapper.ReadContext(context.Background())
	defer cancel()

	slog.Debug("Token metadata: checking for missing tokens", "chain_id", f.chainID)

	// Find tokens in balance_changes that don't have metadata yet
	// Use NOT IN instead of LEFT JOIN for better handling of empty tables
	query := `
		SELECT DISTINCT token
		FROM erc20_balance_changes FINAL
		WHERE chain_id = ?
		  AND token NOT IN (
		      SELECT token FROM token_metadata FINAL WHERE chain_id = ?
		  )
		LIMIT ?
	`

	rows, err := f.conn.Query(ctx, query, f.chainID, f.chainID, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to query missing tokens: %w", err)
	}
	defer rows.Close()

	var tokens [][]byte
	for rows.Next() {
		var token []byte
		if err := rows.Scan(&token); err != nil {
			return 0, fmt.Errorf("failed to scan token: %w", err)
		}
		tokens = append(tokens, token)
	}

	slog.Info("Token metadata: found tokens without metadata", "chain_id", f.chainID, "count", len(tokens))

	if len(tokens) == 0 {
		return 0, nil
	}

	// Fetch metadata for each token
	var metadata []TokenMetadata
	for _, token := range tokens {
		m, err := f.fetchTokenMetadata(token)
		if err != nil {
			slog.Warn("Failed to fetch metadata for token", "chain_id", f.chainID, "token", hex.EncodeToString(token), "error", err)
			if m == nil {
				// Insert with empty values so we don't retry forever
				m = &TokenMetadata{
					Token:    token,
					Name:     "",
					Symbol:   "",
					Decimals: 18, // Default to 18 decimals
				}
			}
		}
		metadata = append(metadata, *m)
	}

	// Batch insert metadata
	if len(metadata) > 0 {
		if err := f.insertMetadata(metadata); err != nil {
			return 0, fmt.Errorf("failed to insert metadata: %w", err)
		}
	}

	return len(metadata), nil
}

// fetchTokenMetadata fetches name, symbol, decimals for a single token
func (f *TokenMetadataFetcher) fetchTokenMetadata(token []byte) (*TokenMetadata, error) {
	tokenAddr := "0x" + hex.EncodeToString(token)

	// Fetch all three in parallel
	type result[T any] struct {
		value T
		err   error
	}
	nameCh := make(chan result[string], 1)
	symbolCh := make(chan result[string], 1)
	decimalsCh := make(chan result[uint8], 1)

	go func() {
		name, err := f.callStringMethod(tokenAddr, nameSelector)
		if err != nil {
			err = fmt.Errorf("name: %w", err)
		}
		nameCh <- result[string]{value: name, err: err}
	}()

	go func() {
		symbol, err := f.callStringMethod(tokenAddr, symbolSelector)
		if err != nil {
			err = fmt.Errorf("symbol: %w", err)
		}
		symbolCh <- result[string]{value: symbol, err: err}
	}()

	go func() {
		decimals, err := f.callDecimals(tokenAddr)
		if err != nil {
			err = fmt.Errorf("decimals: %w", err)
		}
		decimalsCh <- result[uint8]{value: decimals, err: err}
	}()

	name := <-nameCh
	symbol := <-symbolCh
	decimals := <-decimalsCh

	var errs []error
	for _, err := range []error{name.err, symbol.err, decimals.err} {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return &TokenMetadata{
		Token:    token,
		Name:     name.value,
		Symbol:   symbol.value,
		Decimals: decimals.value,
	}, errors.Join(errs...)
}

// callStringMethod calls a method that returns a string (name or symbol)
func (f *TokenMetadataFetcher) callStringMethod(tokenAddr, selector string) (string, error) {
	result, err := f.ethCall(tokenAddr, selector)
	if err != nil {
		return "", err
	}

	return decodeString(result)
}

// callDecimals calls decimals() which returns uint8
func (f *TokenMetadataFetcher) callDecimals(tokenAddr string) (uint8, error) {
	result, err := f.ethCall(tokenAddr, decimalsSelector)
	if err != nil {
		return 18, err
	}

	// Decimals returns a uint8, encoded as 32 bytes
	if len(result) < 2 {
		return 18, fmt.Errorf("invalid decimals response")
	}

	// Remove 0x prefix
	result = strings.TrimPrefix(result, "0x")
	if len(result) < 64 {
		return 18, fmt.Errorf("invalid decimals response length")
	}

	// Parse last byte (or last 2 hex chars)
	var decimals uint8
	_, err = fmt.Sscanf(result[len(result)-2:], "%02x", &decimals)
	if err != nil {
		return 18, err
	}

	return decimals, nil
}

// ethCall makes an eth_call RPC request
func (f *TokenMetadataFetcher) ethCall(to, data string) (string, error) {
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_call",
		"params": []interface{}{
			map[string]string{
				"to":   to,
				"data": data,
			},
			"latest",
		},
		"id": 1,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", err
	}

	resp, err := f.httpClient.Post(f.rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("RPC HTTP status %d", resp.StatusCode)
	}

	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return "", err
	}

	if rpcResp.Error != nil {
		return "", fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

// decodeString decodes an ABI-encoded string
func decodeString(hexData string) (string, error) {
	hexData = strings.TrimPrefix(hexData, "0x")
	if len(hexData) < 128 {
		// Try to decode as a simple string (some tokens don't follow ABI encoding)
		data, err := hex.DecodeString(hexData)
		if err != nil {
			return "", err
		}
		// Trim null bytes
		return strings.TrimRight(string(data), "\x00"), nil
	}

	data, err := hex.DecodeString(hexData)
	if err != nil {
		return "", err
	}

	// ABI-encoded string: offset (32 bytes) + length (32 bytes) + data
	if len(data) < 64 {
		return "", fmt.Errorf("data too short for ABI string")
	}

	// Read length from bytes 32-64
	length := int(data[63]) // Simple case: length < 256
	if len(data) >= 32 {
		// Parse length from offset 32
		for i := 32; i < 64; i++ {
			if data[i] != 0 {
				// More complex length encoding
				length = 0
				for j := 32; j < 64; j++ {
					length = length*256 + int(data[j])
				}
				break
			}
		}
	}

	if length == 0 {
		return "", nil
	}

	// Read string data starting at byte 64
	if len(data) < 64+length {
		length = len(data) - 64
	}

	if length <= 0 {
		return "", nil
	}

	return string(data[64 : 64+length]), nil
}

// insertMetadata batch inserts token metadata
func (f *TokenMetadataFetcher) insertMetadata(metadata []TokenMetadata) error {
	ctx, cancel := chwrapper.WriteContext(context.Background())
	defer cancel()

	batch, err := f.conn.PrepareBatch(ctx, `
		INSERT INTO token_metadata (chain_id, token, name, symbol, decimals)
	`)
	if err != nil {
		return err
	}

	for _, m := range metadata {
		// Convert []byte to [20]byte for FixedString(20)
		var tokenFixed [20]byte
		copy(tokenFixed[:], m.Token)

		if err := batch.Append(f.chainID, tokenFixed, m.Name, m.Symbol, m.Decimals); err != nil {
			return err
		}
	}

	return batch.Send()
}

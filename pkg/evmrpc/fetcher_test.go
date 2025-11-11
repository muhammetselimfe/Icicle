package evmrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchBlockRangeVsRaw(t *testing.T) {
	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		t.Fatalf("RPC_URL environment variable not set, skipping integration test")
	}

	// Create fetcher
	fetcher := NewFetcher(FetcherOptions{
		RpcURL:           rpcURL,
		MaxConcurrency:   5,
		ProgressCallback: nil, // No progress reporting in tests
	})

	// Get latest block number
	latest, err := fetcher.GetLatestBlock()
	require.NoError(t, err, "failed to get latest block")
	require.Greater(t, latest, int64(2), "need at least 3 blocks to test")

	// Test ranges to validate
	testRanges := []struct {
		name string
		from int64
		to   int64
	}{
		{
			name: "LatestBlocks",
			from: latest - 2,
			to:   latest,
		},
	}

	// Add specific historical blocks if available
	if latest >= 19617327 {
		testRanges = append(testRanges, struct {
			name string
			from int64
			to   int64
		}{
			name: "HistoricalBlocks_19617325-19617327",
			from: 19617325,
			to:   19617327,
		})
	}

	for _, tc := range testRanges {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			testBlockRange(t, fetcher, rpcURL, tc.from, tc.to)
		})
	}
}

func testBlockRange(t *testing.T, fetcher *Fetcher, rpcURL string, from, to int64) {
	t.Logf("Testing blocks %d to %d", from, to)

	// Fetch using our batch API
	numBlocks := int(to - from + 1)
	fetchedBlocks, err := fetcher.FetchBlockRange(from, to)
	require.NoError(t, err, "failed to fetch block range")
	require.Len(t, fetchedBlocks, numBlocks, "should fetch %d blocks", numBlocks)

	// Make raw requests
	rawBlocks := makeRawBlockRequests(t, rpcURL, from, to)
	rawReceipts := makeRawReceiptRequests(t, rpcURL, fetchedBlocks)
	rawTraces := makeRawTraceRequests(t, rpcURL, from, to, fetchedBlocks)

	// Compare blocks
	t.Run("CompareBlocks", func(t *testing.T) {
		for i, fetchedBlock := range fetchedBlocks {
			blockNum := from + int64(i)
			rawBlock := rawBlocks[blockNum]

			// Marshal both to JSON for comparison
			fetchedJSON, err := json.Marshal(fetchedBlock.Block)
			require.NoError(t, err, "failed to marshal fetched block")

			rawJSON, err := json.Marshal(rawBlock)
			require.NoError(t, err, "failed to marshal raw block")

			// Compare JSON
			require.JSONEq(t, string(rawJSON), string(fetchedJSON),
				"block %d data mismatch", blockNum)

			t.Logf("Block %d: ✓ matched", blockNum)
		}
	})

	// Compare receipts
	t.Run("CompareReceipts", func(t *testing.T) {
		for i, fetchedBlock := range fetchedBlocks {
			blockNum := from + int64(i)

			// Compare each transaction's receipt
			for j, tx := range fetchedBlock.Block.Transactions {
				rawReceipt := rawReceipts[tx.Hash]
				require.NotNil(t, rawReceipt, "missing raw receipt for tx %s", tx.Hash)

				fetchedReceiptJSON, err := json.Marshal(fetchedBlock.Receipts[j])
				require.NoError(t, err, "failed to marshal fetched receipt")

				rawReceiptJSON, err := json.Marshal(rawReceipt)
				require.NoError(t, err, "failed to marshal raw receipt")

				require.JSONEq(t, string(rawReceiptJSON), string(fetchedReceiptJSON),
					"receipt mismatch for tx %s in block %d", tx.Hash, blockNum)
			}

			t.Logf("Block %d receipts: ✓ all %d matched", blockNum, len(fetchedBlock.Block.Transactions))
		}
	})

	// Compare traces
	t.Run("CompareTraces", func(t *testing.T) {
		for i, fetchedBlock := range fetchedBlocks {
			blockNum := from + int64(i)

			for j, tx := range fetchedBlock.Block.Transactions {
				rawTrace := rawTraces[tx.Hash]
				fetchedTrace := fetchedBlock.Traces[j]

				// Both should have the same tx hash
				require.Equal(t, tx.Hash, fetchedTrace.TxHash,
					"trace tx hash mismatch for block %d, tx index %d", blockNum, j)

				// If raw trace exists, compare
				if rawTrace != nil {
					if fetchedTrace.Result != nil {
						fetchedTraceJSON, err := json.Marshal(fetchedTrace.Result)
						require.NoError(t, err, "failed to marshal fetched trace")

						rawTraceJSON, err := json.Marshal(rawTrace)
						require.NoError(t, err, "failed to marshal raw trace")

						require.JSONEq(t, string(rawTraceJSON), string(fetchedTraceJSON),
							"trace mismatch for tx %s in block %d", tx.Hash, blockNum)
					} else {
						// If fetched trace is nil but raw exists, that's a problem
						require.Nil(t, rawTrace, "fetched trace is nil but raw trace exists for tx %s", tx.Hash)
					}
				} else {
					// If raw trace is nil, fetched should also be nil
					require.Nil(t, fetchedTrace.Result,
						"fetched trace exists but raw trace is nil for tx %s", tx.Hash)
				}
			}

			t.Logf("Block %d traces: ✓ all %d matched", blockNum, len(fetchedBlock.Block.Transactions))
		}
	})
}

// makeRawBlockRequests makes raw HTTP batch requests to get blocks
func makeRawBlockRequests(t *testing.T, rpcURL string, from, to int64) map[int64]map[string]interface{} {
	numBlocks := int(to - from + 1)

	// Create batch request
	var requests []map[string]interface{}
	for i := int64(0); i < int64(numBlocks); i++ {
		blockNum := from + i
		requests = append(requests, map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_getBlockByNumber",
			"params":  []interface{}{fmt.Sprintf("0x%x", blockNum), true},
			"id":      i,
		})
	}

	// Make request
	reqBody, err := json.Marshal(requests)
	require.NoError(t, err, "failed to marshal block requests")

	resp, err := http.Post(rpcURL, "application/json", bytes.NewBuffer(reqBody))
	require.NoError(t, err, "failed to make raw block request")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read response body")

	var responses []map[string]interface{}
	err = json.Unmarshal(body, &responses)
	require.NoError(t, err, "failed to unmarshal block responses")

	// Sort by ID
	sort.Slice(responses, func(i, j int) bool {
		return int(responses[i]["id"].(float64)) < int(responses[j]["id"].(float64))
	})

	// Map to block number
	blocks := make(map[int64]map[string]interface{})
	for i, resp := range responses {
		blockNum := from + int64(i)

		// Check for error
		if errObj, ok := resp["error"]; ok {
			t.Fatalf("RPC error for block %d: %v", blockNum, errObj)
		}

		result := resp["result"].(map[string]interface{})
		blocks[blockNum] = result
	}

	return blocks
}

// makeRawReceiptRequests makes raw HTTP batch requests to get receipts
func makeRawReceiptRequests(t *testing.T, rpcURL string, blocks []*NormalizedBlock) map[string]map[string]interface{} {
	// Collect all transaction hashes
	var txHashes []string
	for _, block := range blocks {
		for _, tx := range block.Block.Transactions {
			txHashes = append(txHashes, tx.Hash)
		}
	}

	if len(txHashes) == 0 {
		return make(map[string]map[string]interface{})
	}

	// Create batch request
	var requests []map[string]interface{}
	for i, txHash := range txHashes {
		requests = append(requests, map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_getTransactionReceipt",
			"params":  []interface{}{txHash},
			"id":      i,
		})
	}

	// Make request
	reqBody, err := json.Marshal(requests)
	require.NoError(t, err, "failed to marshal receipt requests")

	resp, err := http.Post(rpcURL, "application/json", bytes.NewBuffer(reqBody))
	require.NoError(t, err, "failed to make raw receipt request")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read response body")

	var responses []map[string]interface{}
	err = json.Unmarshal(body, &responses)
	require.NoError(t, err, "failed to unmarshal receipt responses")

	// Sort by ID
	sort.Slice(responses, func(i, j int) bool {
		return int(responses[i]["id"].(float64)) < int(responses[j]["id"].(float64))
	})

	// Map to tx hash
	receipts := make(map[string]map[string]interface{})
	for i, resp := range responses {
		txHash := txHashes[i]

		// Check for error
		if errObj, ok := resp["error"]; ok {
			t.Fatalf("RPC error for tx %s: %v", txHash, errObj)
		}

		result := resp["result"].(map[string]interface{})
		receipts[txHash] = result
	}

	return receipts
}

// makeRawTraceRequests makes raw HTTP batch requests to get traces
func makeRawTraceRequests(t *testing.T, rpcURL string, from, to int64, blocks []*NormalizedBlock) map[string]map[string]interface{} {
	traces := make(map[string]map[string]interface{})

	// Try block-level traces first
	numBlocks := int(to - from + 1)
	var blockRequests []map[string]interface{}
	for i := 0; i < numBlocks; i++ {
		blockNum := from + int64(i)
		blockRequests = append(blockRequests, map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "debug_traceBlockByNumber",
			"params":  []interface{}{fmt.Sprintf("0x%x", blockNum), map[string]string{"tracer": "callTracer"}},
			"id":      i,
		})
	}

	// Make block trace request
	reqBody, err := json.Marshal(blockRequests)
	require.NoError(t, err, "failed to marshal block trace requests")

	resp, err := http.Post(rpcURL, "application/json", bytes.NewBuffer(reqBody))
	require.NoError(t, err, "failed to make raw block trace request")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read trace response body")

	var blockResponses []map[string]interface{}
	err = json.Unmarshal(body, &blockResponses)
	require.NoError(t, err, "failed to unmarshal block trace responses")

	// Sort by ID
	sort.Slice(blockResponses, func(i, j int) bool {
		return int(blockResponses[i]["id"].(float64)) < int(blockResponses[j]["id"].(float64))
	})

	// Check if block-level traces worked
	blockTracesWork := true
	for _, resp := range blockResponses {
		if _, hasError := resp["error"]; hasError {
			blockTracesWork = false
			break
		}
	}

	if blockTracesWork {
		// Parse block traces
		for i, resp := range blockResponses {
			if resultArr, ok := resp["result"].([]interface{}); ok {
				block := blocks[i]
				for j, traceResult := range resultArr {
					if j >= len(block.Block.Transactions) {
						break
					}

					txHash := block.Block.Transactions[j].Hash
					traceObj := traceResult.(map[string]interface{})

					if result, ok := traceObj["result"].(map[string]interface{}); ok {
						traces[txHash] = result
					} else {
						traces[txHash] = nil
					}
				}
			}
		}
	} else {
		// Fall back to per-transaction traces
		t.Logf("Block-level traces failed, falling back to per-transaction traces")

		var txHashes []string
		for _, block := range blocks {
			for _, tx := range block.Block.Transactions {
				txHashes = append(txHashes, tx.Hash)
			}
		}

		if len(txHashes) == 0 {
			return traces
		}

		// Create tx trace requests
		var txRequests []map[string]interface{}
		for i, txHash := range txHashes {
			txRequests = append(txRequests, map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "debug_traceTransaction",
				"params":  []interface{}{txHash, map[string]string{"tracer": "callTracer"}},
				"id":      i,
			})
		}

		// Make tx trace request
		reqBody, err := json.Marshal(txRequests)
		require.NoError(t, err, "failed to marshal tx trace requests")

		resp, err := http.Post(rpcURL, "application/json", bytes.NewBuffer(reqBody))
		require.NoError(t, err, "failed to make raw tx trace request")
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "failed to read tx trace response body")

		var txResponses []map[string]interface{}
		err = json.Unmarshal(body, &txResponses)
		require.NoError(t, err, "failed to unmarshal tx trace responses")

		// Sort by ID
		sort.Slice(txResponses, func(i, j int) bool {
			return int(txResponses[i]["id"].(float64)) < int(txResponses[j]["id"].(float64))
		})

		// Parse tx traces
		for i, resp := range txResponses {
			txHash := txHashes[i]

			if errObj, ok := resp["error"]; ok {
				// Some traces might fail (e.g., precompiles)
				errMsg := errObj.(map[string]interface{})["message"].(string)
				if isPrecompileError(fmt.Errorf("%s", errMsg)) {
					t.Logf("Trace failed for tx %s (precompile), treating as nil", txHash)
					traces[txHash] = nil
				} else {
					t.Logf("Trace failed for tx %s: %v", txHash, errMsg)
					traces[txHash] = nil
				}
			} else if result, ok := resp["result"].(map[string]interface{}); ok {
				traces[txHash] = result
			} else {
				traces[txHash] = nil
			}
		}
	}

	return traces
}

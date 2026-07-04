package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "icicle/docs" // swagger docs
)

// @title Icicle API
// @version 1.0
// @description Avalanche P-Chain and EVM Indexer API
// @description
// @description ## Overview
// @description This API provides access to indexed Avalanche blockchain data including:
// @description - **Data API** (`/api/v1/data/*`): Blocks, transactions, subnets, validators, P-chain data
// @description - **Metrics API** (`/api/v1/metrics/*`): Fee statistics, chain metrics, time series data
// @description
// @description ## Rate Limiting
// @description All endpoints are rate limited to 60 requests/minute per IP (burst of 10) by default. Configurable via CLI flags.

// @host localhost:8080
// @BasePath /
// @schemes http https

// @tag.name Health
// @tag.description Health check endpoints

// @tag.name Data - EVM
// @tag.description EVM blockchain data (blocks, transactions)

// @tag.name Data - P-Chain
// @tag.description P-Chain transaction data

// @tag.name Data - Subnets
// @tag.description Subnet and L1 information

// @tag.name Data - Validators
// @tag.description L1 validator information

// @tag.name Metrics - Fees
// @tag.description L1 validation fee statistics

// @tag.name Metrics - Chain
// @tag.description EVM chain statistics and time series

// @tag.name Metrics - Indexer
// @tag.description Indexer sync status

// Config holds API server configuration
type Config struct {
	RateLimit RateLimitConfig
	Metrics   MetricsConfig
	WebSocket WebSocketConfig
}

type MetricsConfig struct {
	Token string
}

type WebSocketConfig struct {
	MaxConnections         int
	MaxConnectionsPerIP    int
	MaxConnectionsPerChain int
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		RateLimit: DefaultRateLimitConfig(),
		WebSocket: DefaultWebSocketConfig(),
	}
}

type Server struct {
	conn        driver.Conn
	router      *http.ServeMux
	rateLimiter *RateLimiter
	handler     http.Handler
	wsHub       *WSHub
}

type Response struct {
	Data interface{} `json:"data,omitempty"`
	Meta *Meta       `json:"meta,omitempty"`
}

type Meta struct {
	Total      int64  `json:"total,omitempty"`
	Limit      int    `json:"limit"`
	Offset     int    `json:"offset"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// Cursor holds decoded cursor position for keyset pagination
type Cursor struct {
	BlockNumber uint64
	TxIndex     uint16
	HasTxIndex  bool
}

func NewServer(conn driver.Conn, cfg Config) *Server {
	s := &Server{
		conn:        conn,
		router:      http.NewServeMux(),
		rateLimiter: NewRateLimiter(cfg.RateLimit),
		wsHub:       NewWSHub(conn, cfg.WebSocket, nil),
	}
	s.wsHub.clientIP = s.rateLimiter.ClientIP
	s.registerRoutes(cfg)

	// Start WebSocket hub
	go s.wsHub.Run()
	go s.wsHub.StartPoller()

	// Chain middlewares: Recovery -> CORS -> Metrics -> Timeout -> Logging -> RateLimit -> CacheControl -> Router
	s.handler = Chain(
		s.router,
		RecoveryMiddleware,
		CORSMiddleware("*"),
		MetricsMiddleware,
		TimeoutMiddleware(30*time.Second),
		LoggingMiddleware,
		s.rateLimiter.Middleware,
		CacheControlMiddleware,
	)

	return s
}

func (s *Server) registerRoutes(cfg Config) {
	// System endpoints (no versioning)
	s.router.HandleFunc("GET /health", s.handleHealth)
	if cfg.Metrics.Token != "" {
		s.router.Handle("GET /metrics", MetricsAuthMiddleware(cfg.Metrics.Token)(PrometheusHandler()))
	}

	// Swagger documentation
	s.router.HandleFunc("GET /api/docs/", httpSwagger.WrapHandler)

	// ==========================================
	// Data API - /api/v1/data/*
	// ==========================================

	// EVM Blocks
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/blocks", s.handleListBlocks)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/blocks/{number}", s.handleGetBlock)

	// EVM Transactions
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/txs", s.handleListTxs)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/txs/{hash}", s.handleGetTx)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/address/{address}/txs", s.handleAddressTxs)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/address/{address}/internal-txs", s.handleAddressInternalTxs)

	// EVM Address Balances
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/address/{address}/balances", s.handleAddressBalances)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/address/{address}/native", s.handleAddressNativeBalance)

	// EVM Stablecoins
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/stablecoins", s.handleListStablecoins)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/stablecoins/timeseries", s.handleStablecoinTimeseries)

	// EVM Lending liquidation-risk feed
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/positions", s.handleLendingPositions)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/positions/{account}", s.handleLendingAccount)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/stats", s.handleLendingStats)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/risk", s.handleLendingRisk)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/risk/timeseries", s.handleLendingRiskTimeseries)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/liquidations", s.handleLendingLiquidations)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/liquidations/daily", s.handleLendingLiquidationsDaily)
	s.router.HandleFunc("GET /api/v1/data/evm/{chainId}/lending/alerts", s.handleLendingAlerts)

	// P-Chain Transactions
	s.router.HandleFunc("GET /api/v1/data/pchain/txs", s.handleListPChainTxs)
	s.router.HandleFunc("GET /api/v1/data/pchain/txs/{txId}", s.handleGetPChainTx)
	s.router.HandleFunc("GET /api/v1/data/pchain/tx-types", s.handlePChainTxTypes)

	// P-Chain Overview, timeline & blocks
	s.router.HandleFunc("GET /api/v1/data/pchain/stats", s.handlePChainStats)
	s.router.HandleFunc("GET /api/v1/data/pchain/subnet-timeline", s.handleSubnetTimeline)
	s.router.HandleFunc("GET /api/v1/data/pchain/validators/timeseries", s.handleValidatorTimeseries)
	s.router.HandleFunc("GET /api/v1/data/pchain/blocks", s.handleListPChainBlocks)
	s.router.HandleFunc("GET /api/v1/data/pchain/blocks/{number}", s.handleGetPChainBlock)

	// Subnets
	s.router.HandleFunc("GET /api/v1/data/subnets/{subnetId}", s.handleGetSubnet)

	// Chains (unified endpoint: chains + subnets + L1 registry + validator stats)
	s.router.HandleFunc("GET /api/v1/data/chains", s.handleListChains)
	s.router.HandleFunc("GET /api/v1/data/chains/{chainId}/risk", s.handleChainRisk)

	// Validators
	s.router.HandleFunc("GET /api/v1/data/validators", s.handleListValidators)
	s.router.HandleFunc("GET /api/v1/data/validators/{id}", s.handleGetValidator)
	s.router.HandleFunc("GET /api/v1/data/validators/{id}/deposits", s.handleValidatorDeposits)

	// ==========================================
	// Metrics API - /api/v1/metrics/*
	// ==========================================

	// L1 Fee Metrics
	s.router.HandleFunc("GET /api/v1/metrics/fees", s.handleFeeMetrics)
	s.router.HandleFunc("GET /api/v1/metrics/fees/daily", s.handleDailyFeeBurn)

	// EVM Chain Stats & Time Series
	s.router.HandleFunc("GET /api/v1/metrics/evm/{chainId}/stats", s.handleChainMetrics)
	s.router.HandleFunc("GET /api/v1/metrics/evm/{chainId}/timeseries", s.handleListMetrics)
	s.router.HandleFunc("GET /api/v1/metrics/evm/{chainId}/timeseries/{metric}", s.handleGetMetric)
	s.router.HandleFunc("GET /api/v1/metrics/evm/{chainId}/fees/burned", s.handleFeeBurn)
	s.router.HandleFunc("GET /api/v1/metrics/burned/total", s.handleNetworkBurnedTotal)

	// Indexer Status
	s.router.HandleFunc("GET /api/v1/metrics/indexer/status", s.handleIndexerStatus)

	// Storage / ops stats
	// Storage stats expose internal table names / sizes / row counts — operator
	// metadata, not consumer data. Gate it behind the same bearer token as /metrics.
	s.router.Handle("GET /api/v1/metrics/storage", MetricsAuthMiddleware(cfg.Metrics.Token)(http.HandlerFunc(s.handleStorageStats)))

	// ==========================================
	// WebSocket - /ws/*
	// ==========================================
	s.router.HandleFunc("GET /ws/blocks/{chainId}", s.handleWSBlocks)
	s.router.HandleFunc("GET /ws/lending/{chainId}", s.handleWSLending)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Stop cleans up server resources
func (s *Server) Stop() {
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
}

// Helper functions

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func getPagination(r *http.Request) (limit, offset int) {
	return getPaginationWithCap(r, 100)
}

// getPaginationWithCap is like getPagination but allows a higher per-endpoint
// limit cap. Used for endpoints whose full result set is small and bounded
// (e.g. validators), so the client can fetch everything in one request instead
// of walking many pages.
func getPaginationWithCap(r *http.Request, maxLimit int) (limit, offset int) {
	limit = 20
	offset = 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
			if limit > maxLimit {
				limit = maxLimit
			}
		}
	}

	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
			if offset > 10000 {
				offset = 10000
			}
		}
	}

	return
}

// getCursor parses ?cursor=<block> or ?cursor=<block>:<txIdx>
func getCursor(r *http.Request) *Cursor {
	raw := r.URL.Query().Get("cursor")
	if raw == "" {
		return nil
	}
	parts := strings.SplitN(raw, ":", 2)
	bn, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil
	}
	c := &Cursor{BlockNumber: bn}
	if len(parts) == 2 {
		idx, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return nil
		}
		c.TxIndex = uint16(idx)
		c.HasTxIndex = true
	}
	return c
}

// getCountParam returns true if ?count=true
func getCountParam(r *http.Request) bool {
	return r.URL.Query().Get("count") == "true"
}

// trimResults trims results to limit, returning whether more exist
func trimResults[T any](results []T, limit int) ([]T, bool) {
	if len(results) > limit {
		return results[:limit], true
	}
	return results, false
}

// cursorBlock builds next_cursor from a block number
func cursorBlock(block uint64) string {
	return fmt.Sprintf("%d", block)
}

// cursorBlockTx builds next_cursor from block:txIndex
func cursorBlockTx(block uint32, txIndex uint16) string {
	return fmt.Sprintf("%d:%d", block, txIndex)
}

func getChainIDFromPath(r *http.Request) (uint32, error) {
	chainIDStr := r.PathValue("chainId")
	parsed, err := strconv.ParseUint(chainIDStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

// parseFlexibleTime parses time from various formats: 2025-01-01, 2025-01-01T00:00:00Z, or unix timestamp
func parseFlexibleTime(s string) time.Time {
	// Try date only: 2025-01-01
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t
	}
	// Try RFC3339: 2025-01-01T00:00:00Z
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	// Try unix timestamp
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(ts, 0).UTC()
	}
	return time.Time{}
}

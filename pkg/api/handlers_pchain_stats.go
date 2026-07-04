package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// PChainStats are the headline counters shown on the P-Chain overview page.
type PChainStats struct {
	ActiveL1Subnets     uint64 `json:"active_l1_subnets" example:"42"`
	ActiveLegacySubnets uint64 `json:"active_legacy_subnets" example:"15"`
	ActiveChains        uint64 `json:"active_chains" example:"58"`
	ActiveValidators    uint64 `json:"active_validators" example:"1200"`
	RecentTransactions  uint64 `json:"recent_transactions" example:"34000"`
	TotalL1FeesPaid     uint64 `json:"total_l1_fees_paid" example:"5000000000"`
}

// handlePChainStats returns aggregate P-Chain counters (active subnets, chains,
// validators, recent tx volume, and total L1 fees paid). All scoped to the
// primary P-Chain instance (p_chain_id = 0).
// @Summary Get P-Chain overview stats
// @Description Headline counters for the P-Chain: active L1/legacy subnets, active chains and validators, 7-day tx volume, and total L1 fees paid (nAVAX).
// @Tags Data - P-Chain
// @Produce json
// @Success 200 {object} Response{data=PChainStats}
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/data/pchain/stats [get]
func (s *Server) handlePChainStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var stats PChainStats

	// Active subnets / chains / validators, derived from validators that are
	// currently active joined to their subnet's type.
	err := s.conn.QueryRow(ctx, `
		SELECT
			uniqExactIf(s.subnet_id, s.subnet_type = 'l1')                              AS active_l1_subnets,
			uniqExactIf(s.subnet_id, s.subnet_type = 'legacy')                          AS active_legacy_subnets,
			uniqExactIf(s.subnet_id, s.subnet_type IN ('l1', 'legacy', 'primary'))      AS active_chains,
			uniqExactIf(v.node_id, s.subnet_type IN ('primary', 'l1'))                  AS active_validators
		FROM (SELECT subnet_id, node_id FROM l1_validator_state FINAL WHERE p_chain_id = 0 AND active = true) v
		INNER JOIN (SELECT subnet_id, subnet_type FROM subnets FINAL WHERE p_chain_id = 0) s
			ON v.subnet_id = s.subnet_id
	`).Scan(&stats.ActiveL1Subnets, &stats.ActiveLegacySubnets, &stats.ActiveChains, &stats.ActiveValidators)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}

	// Recent transaction volume (last 7 days). Matches the overview widget;
	// no FINAL since an approximate count is fine here and far cheaper.
	_ = s.conn.QueryRow(ctx, `
		SELECT count()
		FROM p_chain_txs
		WHERE p_chain_id = 0 AND block_time >= now() - toIntervalDay(7)
	`).Scan(&stats.RecentTransactions)

	// Total L1 validation fees paid across all subnets.
	_ = s.conn.QueryRow(ctx, `
		SELECT toUInt64(sum(total_fees_paid))
		FROM l1_fee_stats FINAL
		WHERE p_chain_id = 0
	`).Scan(&stats.TotalL1FeesPaid)

	writeJSON(w, http.StatusOK, Response{Data: stats})
}

// SubnetTimelinePoint is one month of L1-subnet conversions.
type SubnetTimelinePoint struct {
	Period time.Time `json:"period"`
	Value  uint64    `json:"value" example:"3"`
}

// handleSubnetTimeline returns the per-month count of subnets converted to L1.
// Callers typically render a cumulative line from these monthly counts.
// @Summary Get L1 subnet creation timeline
// @Description Monthly count of subnets converted to L1, oldest first.
// @Tags Data - P-Chain
// @Produce json
// @Success 200 {object} Response{data=[]SubnetTimelinePoint}
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/data/pchain/subnet-timeline [get]
func (s *Server) handleSubnetTimeline(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := s.conn.Query(ctx, `
		SELECT toStartOfMonth(converted_time) AS period, toUInt64(count()) AS value
		FROM (SELECT subnet_type, converted_time, converted_block FROM subnets FINAL WHERE p_chain_id = 0)
		WHERE subnet_type = 'l1' AND converted_block > 0
		GROUP BY period
		ORDER BY period ASC
	`)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	defer rows.Close()

	points := []SubnetTimelinePoint{}
	for rows.Next() {
		var p SubnetTimelinePoint
		if err := rows.Scan(&p.Period, &p.Value); err != nil {
			writeInternalError(w, err.Error())
			return
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		writeInternalError(w, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{Data: points})
}

// ValidatorCountPoint is one sampled point of the historical validator-count
// series. Counts are exact snapshots of the active validator sets (sampled
// on-chain via platform.getValidatorsAt), not reconstructions.
type ValidatorCountPoint struct {
	Period  time.Time `json:"period"`
	Primary uint64    `json:"primary" example:"1205"` // Primary Network validators
	L1      uint64    `json:"l1" example:"898"`       // Sum across all L1 subnets
	Total   uint64    `json:"total" example:"2103"`   // Primary + L1
}

// handleValidatorTimeseries serves the historical active-validator counts from
// validator_count_snapshots. History is backfilled monthly from mainnet launch
// (2020-10) and sampled daily going forward, so interval=day returns whatever
// sample density exists per period (no interpolation).
// @Summary Get historical validator counts
// @Description Exact active validator counts over time (Primary Network and L1s), sampled from on-chain validator sets. Monthly from 2020-10, daily from mid-2026. Oldest first.
// @Tags Data - P-Chain
// @Produce json
// @Param interval query string false "Aggregation interval: month (default) or day. Month returns the last sample of each month."
// @Param from query string false "Start date (YYYY-MM-DD)"
// @Param to query string false "End date (YYYY-MM-DD)"
// @Success 200 {object} Response{data=[]ValidatorCountPoint}
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/data/pchain/validators/timeseries [get]
func (s *Server) handleValidatorTimeseries(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	interval := r.URL.Query().Get("interval")
	var periodExpr string
	switch interval {
	case "", "month":
		periodExpr = "toStartOfMonth(snapshot_date)"
	case "day":
		periodExpr = "snapshot_date"
	default:
		writeAPIError(w, http.StatusBadRequest, ErrInvalidParameter, "interval must be 'day' or 'month'")
		return
	}

	where := "p_chain_id = 0"
	args := []interface{}{}
	if from := r.URL.Query().Get("from"); from != "" {
		t, err := time.Parse("2006-01-02", from)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, ErrInvalidParameter, "from must be YYYY-MM-DD")
			return
		}
		where += " AND snapshot_date >= ?"
		args = append(args, t)
	}
	if to := r.URL.Query().Get("to"); to != "" {
		t, err := time.Parse("2006-01-02", to)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, ErrInvalidParameter, "to must be YYYY-MM-DD")
			return
		}
		where += " AND snapshot_date <= ?"
		args = append(args, t)
	}

	// Inner query: last sample per (period, subnet) - with monthly history a
	// month has one sample, with daily sampling the month's freshest wins.
	// Outer: split Primary vs L1 per period.
	query := fmt.Sprintf(`
		SELECT
			period,
			toUInt64(sumIf(cnt, subnet_id = '%s'))  AS primary_count,
			toUInt64(sumIf(cnt, subnet_id != '%s')) AS l1_count
		FROM (
			SELECT %s AS period, subnet_id, argMax(validator_count, snapshot_date) AS cnt
			FROM validator_count_snapshots FINAL
			WHERE %s
			GROUP BY period, subnet_id
		)
		GROUP BY period
		ORDER BY period ASC
	`, primaryNetworkSubnetID, primaryNetworkSubnetID, periodExpr, where)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	defer rows.Close()

	points := []ValidatorCountPoint{}
	for rows.Next() {
		var p ValidatorCountPoint
		var period time.Time
		if err := rows.Scan(&period, &p.Primary, &p.L1); err != nil {
			writeInternalError(w, err.Error())
			return
		}
		p.Period = period
		p.Total = p.Primary + p.L1
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		writeInternalError(w, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{Data: points})
}

// PChainBlock is a P-Chain block summarized from its transactions. The P-Chain
// has no standalone block store here, so blocks are derived by grouping
// p_chain_txs on block_number.
type PChainBlock struct {
	BlockNumber uint64    `json:"block_number" example:"24160141"`
	TxCount     uint64    `json:"tx_count" example:"3"`
	BlockTime   time.Time `json:"block_time"`
	BlockHash   string    `json:"block_hash,omitempty" example:"2ZW6HUePB..."`

	// Proposer / parent info (only populated on the single-block endpoint).
	ParentID       string `json:"parent_id,omitempty"`
	ProposerID     string `json:"proposer_id,omitempty"`
	ProposerNodeID string `json:"proposer_node_id,omitempty"`
	BlockType      string `json:"block_type,omitempty"`
}

// handleListPChainBlocks returns recent P-Chain blocks (newest first).
// Blocks are grouped from p_chain_txs. Because block_number isn't the table's
// sort key, grouping the whole table would be prohibitively expensive, so the
// listing is bounded to a recent window (default 7 days, ?days=N to widen).
// @Summary List P-Chain blocks
// @Description Recent P-Chain blocks (grouped from transactions), newest first. Bounded to the last N days (default 7).
// @Tags Data - P-Chain
// @Produce json
// @Param days query int false "Look-back window in days" default(7)
// @Param limit query int false "Number of results (max 100)" default(20)
// @Param offset query int false "Pagination offset" default(0)
// @Success 200 {object} Response{data=[]PChainBlock,meta=Meta}
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/data/pchain/blocks [get]
func (s *Server) handleListPChainBlocks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := getPagination(r)
	fetchLimit := limit + 1

	days := 7
	if d := r.URL.Query().Get("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed > 0 {
			days = parsed
		}
	}

	// NB: don't alias max(block_time) as "block_time" — it collides with the
	// real block_time column referenced in WHERE (ClickHouse error 184).
	rows, err := s.conn.Query(ctx, `
		SELECT
			block_number,
			toUInt64(count()) AS tx_count,
			max(block_time)   AS last_block_time,
			any(tx_id)        AS block_hash
		FROM p_chain_txs
		WHERE p_chain_id = 0 AND block_time >= now() - toIntervalDay(?)
		GROUP BY block_number
		ORDER BY block_number DESC
		LIMIT ? OFFSET ?
	`, days, fetchLimit, offset)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	defer rows.Close()

	blocks := []PChainBlock{}
	for rows.Next() {
		var b PChainBlock
		if err := rows.Scan(&b.BlockNumber, &b.TxCount, &b.BlockTime, &b.BlockHash); err != nil {
			writeInternalError(w, err.Error())
			return
		}
		blocks = append(blocks, b)
	}
	if err := rows.Err(); err != nil {
		writeInternalError(w, err.Error())
		return
	}

	blocks, hasMore := trimResults(blocks, limit)
	meta := &Meta{Limit: limit, Offset: offset, HasMore: hasMore}

	writeJSON(w, http.StatusOK, Response{Data: blocks, Meta: meta})
}

// handleGetPChainBlock returns a single P-Chain block by number, including
// proposer/parent metadata pulled from the block's transactions.
// @Summary Get P-Chain block by number
// @Description A single P-Chain block (summarized from its transactions) including proposer and parent info.
// @Tags Data - P-Chain
// @Produce json
// @Param number path int true "Block number"
// @Success 200 {object} Response{data=PChainBlock}
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /api/v1/data/pchain/blocks/{number} [get]
func (s *Server) handleGetPChainBlock(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	blockNumber, err := strconv.ParseUint(r.PathValue("number"), 10, 64)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, ErrInvalidParameter, "Invalid block number")
		return
	}

	var b PChainBlock
	var txCount uint64
	err = s.conn.QueryRow(ctx, `
		SELECT
			block_number,
			toUInt64(count())                       AS tx_count,
			max(block_time)                         AS block_time,
			any(tx_id)                              AS block_hash,
			any(toString(tx_data.ParentID))         AS parent_id,
			any(toString(tx_data.ProposerID))       AS proposer_id,
			any(toString(tx_data.NodeID))           AS proposer_node_id,
			any(tx_type)                            AS block_type
		FROM p_chain_txs FINAL
		WHERE p_chain_id = 0 AND block_number = ?
		GROUP BY block_number
	`, blockNumber).Scan(
		&b.BlockNumber, &txCount, &b.BlockTime, &b.BlockHash,
		&b.ParentID, &b.ProposerID, &b.ProposerNodeID, &b.BlockType,
	)
	if err != nil {
		writeNotFoundError(w, "Block")
		return
	}
	b.TxCount = txCount

	writeJSON(w, http.StatusOK, Response{Data: b})
}

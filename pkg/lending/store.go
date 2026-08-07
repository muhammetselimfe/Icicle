package lending

import (
	"context"
	_ "embed"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"icicle/pkg/chwrapper"
)

//go:embed schema.sql
var schemaSQL string

// indexerWatermarksDDL mirrors the table the EVM indexer owns. The lending engine
// runs as its own service, so it creates the table if absent and shares it.
const indexerWatermarksDDL = `
CREATE TABLE IF NOT EXISTS indexer_watermarks (
    chain_id UInt32,
    indexer_name String,
    granularity LowCardinality(String),
    last_period DateTime64(3, 'UTC'),
    last_block_num UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, indexer_name, granularity)`

// Store is the ClickHouse access layer for the lending engine, scoped to one chain.
type Store struct {
	conn    driver.Conn
	chainID uint32
}

// NewStore builds a Store and creates the lending tables.
func NewStore(conn driver.Conn, chainID uint32) (*Store, error) {
	s := &Store{conn: conn, chainID: chainID}
	if err := s.initSchema(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) initSchema() error {
	stmts := append([]string{indexerWatermarksDDL}, splitStatements(schemaSQL)...)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := s.conn.Exec(context.Background(), stmt); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("lending: create table: %w", err)
			}
		}
	}
	return nil
}

// --- watermark ---

// GetWatermark returns the last processed block for a named discovery cursor.
func (s *Store) GetWatermark(ctx context.Context, indexerName string) (uint64, error) {
	var n uint64
	row := s.conn.QueryRow(ctx, `
		SELECT last_block_num FROM (
			SELECT * FROM indexer_watermarks FINAL
		) WHERE chain_id = ? AND indexer_name = ? AND granularity = 'block'
	`, s.chainID, indexerName)
	if err := row.Scan(&n); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, err
	}
	return n, nil
}

// SetWatermark persists the discovery cursor.
func (s *Store) SetWatermark(ctx context.Context, indexerName string, block uint64) error {
	return chwrapper.RetryableExec(ctx, s.conn, `
		INSERT INTO indexer_watermarks (chain_id, indexer_name, granularity, last_period, last_block_num, updated_at)
		VALUES (?, ?, 'block', ?, ?, ?)
	`, s.chainID, indexerName, time.Unix(0, 0).UTC(), block, time.Now().UTC())
}

// SafeBlock returns the highest fully ingested block (the raw sync watermark) so
// discovery never reads ahead of confirmed data, matching the indexer's
// finality-based, no-reorg posture.
func (s *Store) SafeBlock(ctx context.Context) (uint64, error) {
	var n uint32
	row := s.conn.QueryRow(ctx, `SELECT block_number FROM sync_watermark WHERE chain_id = ?`, s.chainID)
	if err := row.Scan(&n); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, err
	}
	return uint64(n), nil
}

// EarliestLogBlock returns the first raw_logs block for any of the given
// contract addresses, the self-correcting backfill floor (rule 8). Returns 0
// when none are present yet.
func (s *Store) EarliestLogBlock(ctx context.Context, addresses []string) (uint64, error) {
	if len(addresses) == 0 {
		return 0, nil
	}
	var n uint32
	q := fmt.Sprintf(`SELECT min(block_number) FROM raw_logs WHERE chain_id = ? AND address IN (%s)`, addrInList(addresses))
	row := s.conn.QueryRow(ctx, q, s.chainID)
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return uint64(n), nil
}

// --- discovery reads/writes ---

// ReadLogs reads raw_logs rows for the given contracts and topics within a block
// range, oldest first.
func (s *Store) ReadLogs(ctx context.Context, addresses, topics []string, fromBlock, toBlock uint64) ([]LogRow, error) {
	if len(addresses) == 0 || len(topics) == 0 {
		return nil, nil
	}
	// Coalesce nullable topics to a zero FixedString so they scan into a fixed
	// [32]byte. Scanning Nullable(FixedString) through a pointer-to-pointer is
	// unsupported by the driver, and an all-zero topic maps to the zero address,
	// which the adapters already treat as absent.
	q := fmt.Sprintf(`
		SELECT address, topic0,
			ifNull(topic1, toFixedString('', 32)) AS topic1,
			ifNull(topic2, toFixedString('', 32)) AS topic2,
			ifNull(topic3, toFixedString('', 32)) AS topic3,
			data, block_number
		FROM raw_logs
		WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
		  AND address IN (%s) AND topic0 IN (%s)
		ORDER BY block_number, log_index
	`, addrInList(addresses), topicInList(topics))

	rows, err := s.conn.Query(ctx, q, s.chainID, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LogRow
	for rows.Next() {
		var addr [20]byte
		var t0, t1, t2, t3 [32]byte
		var data []byte
		var block uint32
		if err := rows.Scan(&addr, &t0, &t1, &t2, &t3, &data, &block); err != nil {
			return nil, err
		}
		out = append(out, LogRow{
			Address: "0x" + hex.EncodeToString(addr[:]),
			Topic0:  "0x" + hex.EncodeToString(t0[:]),
			Topic1:  topicHex(t1),
			Topic2:  topicHex(t2),
			Topic3:  topicHex(t3),
			Data:    data,
			Block:   block,
		})
	}
	return out, rows.Err()
}

// UpsertAccounts records seen accounts. AggregatingMergeTree keeps the min
// first-seen and max last-event automatically.
func (s *Store) UpsertAccounts(ctx context.Context, protocol Protocol, accounts map[string]uint32) error {
	if len(accounts) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_accounts (chain_id, protocol, account, first_seen_block, last_event_block, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for acc, block := range accounts {
		if err := batch.Append(s.chainID, string(protocol), addrBytes(acc), block, block, now); err != nil {
			return err
		}
	}
	return chwrapper.RetryableBatchSend(batch)
}

// UpsertExposure records candidate (asset, side) memberships per account.
func (s *Store) UpsertExposure(ctx context.Context, protocol Protocol, exposures []Exposure) error {
	if len(exposures) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_exposure (chain_id, protocol, account, asset, side, last_block, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, e := range exposures {
		if e.Asset == "" || normalizeAddress(e.Asset) == ZeroAddress {
			continue
		}
		if err := batch.Append(s.chainID, string(protocol), addrBytes(e.Account), addrBytes(e.Asset), string(e.Side), e.Block, now); err != nil {
			return err
		}
	}
	return chwrapper.RetryableBatchSend(batch)
}

// --- account/exposure reads for the health engine ---

// ReadAccounts returns all distinct accounts tracked for a protocol.
func (s *Store) ReadAccounts(ctx context.Context, protocol Protocol) ([]string, error) {
	rows, err := s.conn.Query(ctx, `SELECT DISTINCT account FROM lending_accounts WHERE chain_id = ? AND protocol = ?`, s.chainID, string(protocol))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a [20]byte
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, "0x"+hex.EncodeToString(a[:]))
	}
	return out, rows.Err()
}

// ReadExposure returns candidate exposures grouped by account for a protocol.
func (s *Store) ReadExposure(ctx context.Context, protocol Protocol) (map[string][]Exposure, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT account, asset, side, max(last_block)
		FROM lending_exposure
		WHERE chain_id = ? AND protocol = ?
		GROUP BY account, asset, side
	`, s.chainID, string(protocol))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string][]Exposure{}
	for rows.Next() {
		var acc, asset [20]byte
		var side string
		var block uint32
		if err := rows.Scan(&acc, &asset, &side, &block); err != nil {
			return nil, err
		}
		a := "0x" + hex.EncodeToString(acc[:])
		out[a] = append(out[a], Exposure{
			Account: a, Asset: "0x" + hex.EncodeToString(asset[:]), Side: Side(side), Block: block,
		})
	}
	return out, rows.Err()
}

// ReadAccountsExposedTo returns accounts that hold the given asset as exposure,
// used to fan out a price move across tiers by asset (rule 1).
func (s *Store) ReadAccountsExposedTo(ctx context.Context, protocol Protocol, asset string) ([]string, error) {
	q := fmt.Sprintf(`
		SELECT DISTINCT account FROM lending_exposure
		WHERE chain_id = ? AND protocol = ? AND asset IN (%s)
	`, addrInList([]string{asset}))
	rows, err := s.conn.Query(ctx, q, s.chainID, string(protocol))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a [20]byte
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, "0x"+hex.EncodeToString(a[:]))
	}
	return out, rows.Err()
}

// --- position state ---

// PriorState is the last persisted liquidatable/near flag for crossing detection.
type PriorState struct {
	Liquidatable bool
	Near         bool
}

// ReadPriorStates returns the current liquidatable/near state per account for a
// protocol, deduped with argMax over updated_at so pre-merge duplicates never
// return stale health (rule 2). nearEdge is the near-liquidatable HF in WAD.
func (s *Store) ReadPriorStates(ctx context.Context, protocol Protocol, nearEdge *big.Int) (map[string]PriorState, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT account, liquidatable, health_factor
		FROM (
			SELECT account,
				argMax(liquidatable, updated_at) AS liquidatable,
				argMax(health_factor, updated_at) AS health_factor
			FROM lending_positions
			WHERE chain_id = ? AND protocol = ?
			GROUP BY account
		)
	`, s.chainID, string(protocol))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]PriorState{}
	for rows.Next() {
		var acc [20]byte
		var liq bool
		var hf *big.Int
		if err := rows.Scan(&acc, &liq, &hf); err != nil {
			return nil, err
		}
		a := "0x" + hex.EncodeToString(acc[:])
		near := hf != nil && nearEdge != nil && hf.Cmp(nearEdge) < 0
		out[a] = PriorState{Liquidatable: liq, Near: near}
	}
	return out, rows.Err()
}

// WriteHealth persists position snapshots, their per-asset legs, and any crossing
// alerts in one logical update.
func (s *Store) WriteHealth(ctx context.Context, healths []Health, tiers map[string]Tier, alerts []Alert) error {
	if len(healths) > 0 {
		if err := s.writePositions(ctx, healths, tiers); err != nil {
			return err
		}
		if err := s.writePositionAssets(ctx, healths); err != nil {
			return err
		}
	}
	if len(alerts) > 0 {
		if err := s.writeAlerts(ctx, alerts); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) writePositions(ctx context.Context, healths []Health, tiers map[string]Tier) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_positions
		(chain_id, protocol, account, health_factor, collateral_base, debt_base, shortfall_base, liquidity_base, liquidatable, tier, block_number, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, h := range healths {
		if !h.OK {
			continue
		}
		tier := tiers[h.Account.Address]
		if tier == "" {
			tier = TierCold
		}
		if err := batch.Append(
			s.chainID, string(h.Account.Protocol), addrBytes(h.Account.Address),
			orZero(h.HealthFactor), orZero(h.CollateralBase), orZero(h.DebtBase), orZero(h.ShortfallBase), orZero(h.LiquidityBase),
			h.Liquidatable, string(tier), uint32(h.BlockNumber), now,
		); err != nil {
			return err
		}
	}
	return chwrapper.RetryableBatchSend(batch)
}

func (s *Store) writePositionAssets(ctx context.Context, healths []Health) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_position_assets
		(chain_id, protocol, account, asset, side, amount, base_value, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	any := false
	for _, h := range healths {
		if !h.OK {
			continue
		}
		for _, a := range h.Assets {
			any = true
			if err := batch.Append(
				s.chainID, string(h.Account.Protocol), addrBytes(h.Account.Address),
				addrBytes(a.Asset), string(a.Side), orZero(a.Amount), orZero(a.BaseValue), now,
			); err != nil {
				return err
			}
		}
	}
	if !any {
		return batch.Abort()
	}
	return chwrapper.RetryableBatchSend(batch)
}

func (s *Store) writeAlerts(ctx context.Context, alerts []Alert) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_alerts
		(chain_id, protocol, account, kind, health_factor, collateral_base, debt_base, block_number, created_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, a := range alerts {
		if err := batch.Append(
			s.chainID, string(a.Protocol), addrBytes(a.Account), a.Kind,
			orZero(a.HealthFactor), orZero(a.CollateralBase), orZero(a.DebtBase), a.Block, now,
		); err != nil {
			return err
		}
	}
	return chwrapper.RetryableBatchSend(batch)
}

// --- params / addresses ---

// WriteAddresses persists resolved and verified addresses.
func (s *Store) WriteAddresses(ctx context.Context, protocol Protocol, notes []VerifyNote) error {
	if len(notes) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_protocol_addresses
		(chain_id, protocol, role, address, verified, note, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, n := range notes {
		detail := n.Detail
		if n.Expected != "" && !n.OK {
			detail = fmt.Sprintf("expected %s, resolved %s; %s", n.Expected, n.Resolved, n.Detail)
		}
		if err := batch.Append(s.chainID, string(protocol), n.Role, addrBytes(n.Resolved), n.OK, detail, now); err != nil {
			return err
		}
	}
	return chwrapper.RetryableBatchSend(batch)
}

// WriteParams persists per-asset and global parameters.
func (s *Store) WriteParams(ctx context.Context, protocol Protocol, params []AssetParam, globals GlobalParams) error {
	gb, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_protocol_globals
		(chain_id, protocol, close_factor_bps, liquidation_incentive_bps, small_position_base, base_currency_unit, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = gb.Abort() }()
	if err := gb.Append(s.chainID, string(protocol), globals.CloseFactorBps, globals.LiquidationIncentiveBps,
		orZero(globals.SmallPositionBase), orZero(globals.BaseCurrencyUnit), time.Now().UTC()); err != nil {
		return err
	}
	if err := chwrapper.RetryableBatchSend(gb); err != nil {
		return err
	}

	if len(params) == 0 {
		return nil
	}
	pb, err := s.conn.PrepareBatch(ctx, `INSERT INTO lending_protocol_params
		(chain_id, protocol, asset, market, symbol, decimals, liquidation_threshold_bps, liquidation_bonus_bps, ltv_bps, can_collateral, can_borrow, updated_at)`)
	if err != nil {
		return err
	}
	defer func() { _ = pb.Abort() }()
	now := time.Now().UTC()
	for _, p := range params {
		if err := pb.Append(
			s.chainID, string(protocol), addrBytes(p.Asset), addrBytes(p.Market), p.Symbol, p.Decimals,
			p.LiquidationThresholdBps, p.LiquidationBonusBps, p.LtvBps, p.CanCollateral, p.CanBorrow, now,
		); err != nil {
			return err
		}
	}
	return chwrapper.RetryableBatchSend(pb)
}

// --- helpers ---

func addrBytes(addr string) []byte {
	b := decodeHex(addr)
	out := make([]byte, 20)
	if len(b) >= 20 {
		copy(out, b[len(b)-20:])
	} else {
		copy(out[20-len(b):], b)
	}
	return out
}

func addrInList(addresses []string) string {
	parts := make([]string, 0, len(addresses))
	for _, a := range addresses {
		h := strings.TrimPrefix(strings.ToLower(a), "0x")
		if h == "" {
			continue
		}
		parts = append(parts, "unhex('"+h+"')")
	}
	if len(parts) == 0 {
		return "unhex('')"
	}
	return strings.Join(parts, ", ")
}

func topicInList(topics []string) string {
	parts := make([]string, 0, len(topics))
	for _, t := range topics {
		h := strings.TrimPrefix(strings.ToLower(t), "0x")
		parts = append(parts, "unhex('"+h+"')")
	}
	return strings.Join(parts, ", ")
}

// topicHex returns the 0x topic, or "" for an all-zero (absent) topic.
func topicHex(b [32]byte) string {
	zero := true
	for _, c := range b {
		if c != 0 {
			zero = false
			break
		}
	}
	if zero {
		return ""
	}
	return "0x" + hex.EncodeToString(b[:])
}

func orZero(n *big.Int) *big.Int {
	if n == nil {
		return big.NewInt(0)
	}
	return n
}

// splitStatements splits a SQL script on semicolons. Comments must not contain a
// semicolon (the same constraint the rest of the codebase follows).
func splitStatements(sql string) []string {
	return strings.Split(sql, ";")
}

// Alert is a crossing event emitted to lending_alerts.
type Alert struct {
	Protocol       Protocol
	Account        string
	Kind           string
	HealthFactor   *big.Int
	CollateralBase *big.Int
	DebtBase       *big.Int
	Block          uint32
}

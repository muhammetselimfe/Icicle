package lending

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"time"
)

// Executed-liquidation ingest. Scans raw_logs for Aave LiquidationCall and Benqi
// LiquidateBorrow events, values the repaid debt in USD at the event block, and
// persists them to lending_liquidations. Self-correcting via a watermark (rule 8)
// and idempotent via ReplacingMergeTree keyed by (block, tx, log_index), so a
// re-scan replaces rather than duplicates.

var (
	topicAaveLiquidation  = EventTopic("LiquidationCall(address,address,address,uint256,uint256,address,bool)")
	topicBenqiLiquidation = EventTopic("LiquidateBorrow(address,address,uint256,address,uint256)")
	mul1e10liq            = new(big.Int).Exp(big.NewInt(10), big.NewInt(10), nil)
)

const (
	liqWatermark  = "lending_liquidations"
	liqRangeBatch = 1_000_000 // blocks scanned per cycle while backfilling
)

// liqSource holds one protocol's liquidation event source and valuation inputs.
type liqSource struct {
	protocol  string
	logAddrs  []string         // contracts that emit the event (Aave pool / Benqi markets)
	oracle    string           // head oracle, fallback when block-pinned resolution fails
	anchor    string           // stable contract to resolve the oracle at a block
	oracleSig string           // getPriceOracle() (Aave) / oracle() (Benqi)
	decimals  map[string]uint8 // debt underlying -> decimals (Aave); nil for Benqi
	isBenqi   bool
}

// LiquidationRow is one decoded, valued liquidation ready to persist.
type LiquidationRow struct {
	Protocol                               string
	Block                                  uint32
	BlockTime                              time.Time
	TxHash                                 [32]byte
	LogIndex                               uint32
	Liquidator, Borrower, Collateral, Debt [20]byte
	RepayAmount, SeizeAmount, RepaidUSD    *big.Int
}

// LiquidationIngester scans, values, and persists executed liquidations.
type LiquidationIngester struct {
	store       *Store
	rpc         *Client
	chainID     uint32
	addrSet     []string
	byAddr      map[string]liqSource // log address -> its source (Benqi markets all map to the Benqi source)
	oracleCache map[string]string    // protocol|block-bucket -> oracle resolved at that block
}

// oracleStride buckets oracle resolution by ~1 day; oracles rotate rarely, so this
// bounds the resolution reads during backfill.
const oracleStride = 43200

// buildLiqSource derives a protocol's liquidation source from its resolved
// addresses and params: Aave events come from the pool with per-underlying
// decimals for valuation; Benqi events come from each qiToken market (the Compound
// price convention folds in decimals, so none are needed).
func buildLiqSource(protocol Protocol, addrs Addresses, params []AssetParam) liqSource {
	s := liqSource{protocol: string(protocol), oracle: NormalizeAddr(addrs.Oracle)}
	if protocol == ProtocolBenqi {
		s.isBenqi = true
		s.logAddrs = append([]string(nil), addrs.Markets...)
		s.anchor = NormalizeAddr(addrs.Comptroller)
		s.oracleSig = "oracle()"
		return s
	}
	s.logAddrs = []string{addrs.Pool}
	s.anchor = NormalizeAddr(addrs.Provider)
	s.oracleSig = "getPriceOracle()"
	s.decimals = map[string]uint8{}
	for _, p := range params {
		s.decimals[NormalizeAddr(p.Asset)] = p.Decimals
	}
	return s
}

// NewLiquidationIngester builds an ingester from the active protocols' sources.
func NewLiquidationIngester(store *Store, rpc *Client, chainID uint32, sources []liqSource) *LiquidationIngester {
	li := &LiquidationIngester{store: store, rpc: rpc, chainID: chainID, byAddr: map[string]liqSource{}, oracleCache: map[string]string{}}
	for _, s := range sources {
		for _, a := range s.logAddrs {
			la := NormalizeAddr(a)
			if la == ZeroAddress {
				continue
			}
			li.addrSet = append(li.addrSet, la)
			li.byAddr[la] = s
		}
	}
	return li
}

// Loop runs Sync until the context is cancelled, cycling fast while backfilling
// and idling once caught up.
func (li *LiquidationIngester) Loop(ctx context.Context) {
	if len(li.addrSet) == 0 {
		return
	}
	for {
		behind, err := li.Sync(ctx)
		if err != nil {
			slog.Warn("lending: liquidation sync failed", "error", err)
		}
		wait := 60 * time.Second
		if behind {
			wait = 1 * time.Second // catch up quickly on first backfill
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// Sync processes one block range from the watermark forward and returns whether
// more remains (so the caller can cycle without waiting).
func (li *LiquidationIngester) Sync(ctx context.Context) (bool, error) {
	wm, err := li.store.GetWatermark(ctx, liqWatermark)
	if err != nil {
		return false, err
	}
	safe, err := li.store.SafeBlock(ctx)
	if err != nil || safe == 0 {
		return false, err
	}
	from := wm + 1
	if wm == 0 {
		if e, eerr := li.store.EarliestLogBlock(ctx, li.addrSet); eerr == nil && e > 0 {
			from = e
		}
	}
	if from > safe {
		return false, nil
	}
	to := from + liqRangeBatch
	if to > safe {
		to = safe
	}

	rows, err := li.scan(ctx, from, to)
	if err != nil {
		return false, err
	}
	if err := li.persist(ctx, rows); err != nil {
		return false, err
	}
	if err := li.store.SetWatermark(ctx, liqWatermark, to); err != nil {
		return false, err
	}
	if len(rows) > 0 {
		slog.Info("lending: liquidations ingested", "count", len(rows), "from", from, "to", to)
	}
	return to < safe, nil
}

// scan reads liquidation events in [from, to], decodes them, and values the repaid
// debt block-pinned.
func (li *LiquidationIngester) scan(ctx context.Context, from, to uint64) ([]LiquidationRow, error) {
	q := fmt.Sprintf(`
		SELECT address, block_number, block_time, transaction_hash, log_index, topic0,
			ifNull(topic1, toFixedString('', 32)) AS t1,
			ifNull(topic2, toFixedString('', 32)) AS t2,
			ifNull(topic3, toFixedString('', 32)) AS t3,
			data
		FROM raw_logs
		WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
			AND address IN (%s) AND topic0 IN (%s)
		ORDER BY block_number, log_index
	`, addrInList(li.addrSet), topicInList([]string{topicAaveLiquidation, topicBenqiLiquidation}))

	rows, err := li.store.conn.Query(ctx, q, li.chainID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LiquidationRow
	for rows.Next() {
		var addr [20]byte
		var blk uint32
		var bt time.Time
		var tx, t0, t1, t2, t3 [32]byte
		var logIdx uint32
		var data []byte
		if err := rows.Scan(&addr, &blk, &bt, &tx, &logIdx, &t0, &t1, &t2, &t3, &data); err != nil {
			return nil, err
		}
		la := "0x" + hex.EncodeToString(addr[:])
		src, ok := li.byAddr[la]
		if !ok {
			continue
		}
		r := LiquidationRow{Block: blk, BlockTime: bt, TxHash: tx, LogIndex: logIdx}
		switch "0x" + hex.EncodeToString(t0[:]) {
		case topicAaveLiquidation:
			// indexed: collateral (t1), debt (t2), user (t3); data: debtToCover(0),
			// liquidatedCollateralAmount(1), liquidator(2), receiveAToken(3).
			r.Protocol = string(ProtocolAaveV3)
			r.Collateral = last20(t1)
			r.Debt = last20(t2)
			r.Borrower = last20(t3)
			r.Liquidator = dataAddr(data, 2)
			r.RepayAmount = Word(data, 0)
			r.SeizeAmount = Word(data, 1)
		case topicBenqiLiquidation:
			// unindexed: liquidator(0), borrower(1), repayAmount(2), qiTokenCollateral(3),
			// seizeTokens(4). The borrowed market is the emitting log address.
			r.Protocol = string(ProtocolBenqi)
			r.Liquidator = dataAddr(data, 0)
			r.Borrower = dataAddr(data, 1)
			r.RepayAmount = Word(data, 2)
			r.Collateral = dataAddr(data, 3)
			r.SeizeAmount = Word(data, 4)
			r.Debt = addr // emitting qiToken market
		default:
			continue
		}
		r.RepaidUSD = li.valueRepaid(ctx, src, r)
		out = append(out, r)
	}
	return out, rows.Err()
}

// valueRepaid prices the repaid debt in USD (1e18) using the oracle at the event
// block. Returns 0 when the price cannot be read (kept as an unpriced row rather
// than dropped, so counts stay complete).
func (li *LiquidationIngester) valueRepaid(ctx context.Context, src liqSource, r LiquidationRow) *big.Int {
	if r.RepayAmount == nil || r.RepayAmount.Sign() == 0 {
		return big.NewInt(0)
	}
	// Resolve the oracle as of the event block: Benqi rotates its oracle, so the
	// head address has no code / reverts at historical blocks. Aave's is stable but
	// block-pinned is correct for both.
	oracle := li.oracleAt(ctx, src, uint64(r.Block))
	if oracle == "" {
		return big.NewInt(0)
	}
	bh := "0x" + strconv.FormatUint(uint64(r.Block), 16)
	debt := "0x" + hex.EncodeToString(r.Debt[:])

	if src.isBenqi {
		// getUnderlyingPrice(market) is scaled 1e(36-dec); price * amount / 1e18 = USD 1e18.
		price := li.callPrice(ctx, oracle, "getUnderlyingPrice(address)", debt, bh)
		if price == nil || price.Sign() == 0 {
			return big.NewInt(0)
		}
		return new(big.Int).Div(new(big.Int).Mul(price, r.RepayAmount), WAD)
	}

	// Aave getAssetPrice(asset) is 1e8 USD. value = amount * price / 10^dec, then 1e8 -> 1e18.
	price := li.callPrice(ctx, oracle, "getAssetPrice(address)", debt, bh)
	if price == nil || price.Sign() == 0 {
		return big.NewInt(0)
	}
	dec := uint8(18)
	if d, ok := src.decimals[debt]; ok && d > 0 {
		dec = d
	}
	v := new(big.Int).Mul(r.RepayAmount, price)
	v.Mul(v, mul1e10liq)
	return v.Div(v, pow10liq(dec))
}

// oracleAt resolves the price oracle as of a block via the protocol's stable anchor
// (Aave PoolAddressesProvider.getPriceOracle / Benqi Comptroller.oracle), cached by
// a coarse block bucket. Falls back to the head oracle if the read fails.
func (li *LiquidationIngester) oracleAt(ctx context.Context, src liqSource, block uint64) string {
	key := src.protocol + "|" + strconv.FormatUint(block/oracleStride, 10)
	if v, ok := li.oracleCache[key]; ok {
		return v
	}
	o := src.oracle
	if src.anchor != "" && src.oracleSig != "" {
		bh := "0x" + strconv.FormatUint(block, 16)
		if res, err := li.rpc.EthCall(ctx, src.anchor, EncodeCall0(src.oracleSig), bh); err == nil {
			if a := Addr(DecodeHexBytes(res), 0); a != ZeroAddress {
				o = a
			}
		}
	}
	li.oracleCache[key] = o
	return o
}

func (li *LiquidationIngester) callPrice(ctx context.Context, oracle, sig, arg, blockHex string) *big.Int {
	res, err := li.rpc.EthCall(ctx, oracle, EncodeCall1Addr(sig, arg), blockHex)
	if err != nil {
		return nil
	}
	return Word(DecodeHexBytes(res), 0)
}

func (li *LiquidationIngester) persist(ctx context.Context, rows []LiquidationRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := li.store.conn.PrepareBatch(ctx, `INSERT INTO lending_liquidations (
		chain_id, protocol, block_number, block_time, tx_hash, log_index,
		liquidator, borrower, collateral_asset, debt_asset,
		repay_amount, seize_amount, repaid_usd
	)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, r := range rows {
		if err := batch.Append(
			li.chainID, r.Protocol, r.Block, r.BlockTime, r.TxHash[:], r.LogIndex,
			r.Liquidator[:], r.Borrower[:], r.Collateral[:], r.Debt[:],
			orZero(r.RepayAmount), orZero(r.SeizeAmount), orZero(r.RepaidUSD),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// --- helpers ---

func last20(b [32]byte) [20]byte {
	var a [20]byte
	copy(a[:], b[12:])
	return a
}

func dataAddr(data []byte, i int) [20]byte {
	var a [20]byte
	off := i * 32
	if off+32 <= len(data) {
		copy(a[:], data[off+12:off+32])
	}
	return a
}

func pow10liq(dec uint8) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dec)), nil)
}

package stealtime

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ava-labs/libevm/common"

	"icicle/pkg/kmeasure"
	"icicle/pkg/lending"
	"icicle/pkg/lending/aave"
	"icicle/pkg/lending/benqi"
	"icicle/pkg/prefilter"
)

// ReplayConfig configures the crash-day capture replay.
type ReplayConfig struct {
	ChainID          uint32
	ArchiveRPC       string
	FallbackRPC      string
	TopDays          int      // crash days to take, by sized-liquidation count
	MinSizeUSD1e18   *big.Int // sized threshold (repaid debt), default $1000
	GasUnits         uint64
	MinProfitUSD1e18 *big.Int
	Label            string // tags this run's rows in replay_results so runs never overwrite
	ProbeVenues      bool   // if set, run the venue self-test at ProbeBlock and return
	ProbeBlock       uint64
}

type replayLiq struct {
	liq       Liquidation
	crossing  uint64
	stealTime uint64
	day       string
}

type replayResult struct {
	day                          string
	protocol                     string
	account, liquidator          common.Address
	collateral, debt             common.Address
	takenBlock, crossingBlock    uint64
	stealTime                    uint64
	profitableV2, profitableReal bool
	netV2, netReal               *big.Int // V2-only vs real concentrated-liquidity venue set
	winVenue                     string   // venue that won the chosen route on the real set
	sizeBucket                   string   // by real net
}

var reactionBudgets = []uint64{1, 2, 3, 5}

// Replay re-evaluates crash-day sized liquidations with the actual crash-day base
// fee, computing capture under both a V2-only venue set (the conservative
// executability baseline) and the real concentrated-liquidity venue set (LFJ
// Liquidity Book, Uniswap V3, Pharaoh CL) that actually holds depth. It records
// which venue wins each route, so V2's true contribution to the large-liquidation
// prize is measured rather than assumed. It reads the liquidation set from
// stealtime_results (no re-enumeration) but re-quotes and re-costs block-pinned and
// fresh. Read-only: no keys, no contract, no submission.
func Replay(ctx context.Context, conn driver.Conn, cfg ReplayConfig) error {
	rpc := lending.NewClient(cfg.ArchiveRPC, cfg.FallbackRPC)

	if cfg.ProbeVenues {
		block := cfg.ProbeBlock
		if block == 0 {
			head, err := rpc.BlockNumber(ctx)
			if err != nil {
				return fmt.Errorf("probe: read head: %w", err)
			}
			block = head - 16 // a few blocks back to be safely canonical
		}
		fmt.Print(VenueProbe(ctx, rpc, block))
		return nil
	}

	resolver := kmeasure.NewChainResolver(rpc)
	aaveAd := aave.New("")
	benqiAd := benqi.New("")
	aaveAddrs, aaveParams, aaveGlobals, err := bootstrap(ctx, rpc, aaveAd)
	if err != nil {
		return fmt.Errorf("bootstrap aave: %w", err)
	}
	benqiAddrs, benqiParams, benqiGlobals, err := bootstrap(ctx, rpc, benqiAd)
	if err != nil {
		return fmt.Errorf("bootstrap benqi: %w", err)
	}
	aavePool := common.HexToAddress(aaveAddrs.Pool)
	benqiComptroller := common.HexToAddress(benqiAddrs.Comptroller)
	addrRes := newAddrResolver(rpc, benqiComptroller, 0)

	liqs, windowDays, err := loadCrashDayLiquidations(ctx, conn, cfg)
	if err != nil {
		return fmt.Errorf("load crash-day liquidations: %w", err)
	}
	slog.Info("stealtime replay: loaded crash-day sized liquidations", "count", len(liqs), "window_days", int(windowDays), "label", cfg.Label)

	// Confirm the gas basis is the real crash-day base fee, not a default, by
	// sampling the first liquidation's crossing block.
	gasNote := "base fee unavailable, cost model used its 25 gwei default"
	if len(liqs) > 0 {
		if bf, err := rpc.BlockBaseFee(ctx, liqs[0].crossing); err == nil && bf.Sign() > 0 {
			gasNote = fmt.Sprintf("crash-day base fee (sample block %d = %.1f gwei)", liqs[0].crossing, gweiFloat(bf))
		}
	}

	var results []replayResult
	for i, rl := range liqs {
		if i%200 == 0 && i > 0 {
			slog.Info("stealtime replay: progress", "done", i, "of", len(liqs))
		}

		blockAddrs := addrRes.at(ctx, rl.liq.Protocol, rl.crossing)
		aaveDataProvider := common.HexToAddress(blockAddrs.DataProvider)
		adapter := lending.Adapter(aaveAd)
		if rl.liq.Protocol == "benqi" {
			adapter = benqiAd
			benqiAd.Configure(blockAddrs, benqiParams, benqiGlobals)
		} else {
			aaveAd.Configure(blockAddrs, aaveParams, aaveGlobals)
		}

		pos, ok := assemblePosition(ctx, rpc, adapter, resolver, rl.liq.Protocol, rl.liq, rl.crossing)
		if !ok {
			continue
		}
		params := newBlockParams(ctx, rpc, rl.crossing, aaveDataProvider, benqiComptroller)
		// Crash-day gas: base fee at the opportunity block (cost.go reads BlockBaseFee).
		cost := buildBlockCost(ctx, rpc, rl.crossing, aavePool, cfg.GasUnits, cfg.MinProfitUSD1e18)

		resV2, err := prefilter.EvaluatePosition(ctx, pos, params, newBlockQuoterV2(rpc, rl.crossing), cost)
		if err != nil {
			continue
		}
		realQ := newRealVenueQuoter(rpc, rl.crossing)
		resReal, err := prefilter.EvaluatePosition(ctx, pos, params, realQ, cost)
		winVenue := "none"
		if err != nil {
			resReal = resV2
		} else {
			winVenue = realQ.WinningVenue(resReal.CollateralAsset, resReal.DebtAsset)
		}

		results = append(results, replayResult{
			day: rl.day, protocol: rl.liq.Protocol,
			account: rl.liq.Account, liquidator: rl.liq.Liquidator,
			collateral: rl.liq.CollateralAsset, debt: rl.liq.DebtAsset,
			takenBlock: rl.liq.TakenBlock, crossingBlock: rl.crossing, stealTime: rl.stealTime,
			profitableV2: resV2.Profitable, profitableReal: resReal.Profitable,
			netV2: clampNonNeg(resV2.NetProfitUSD), netReal: clampNonNeg(resReal.NetProfitUSD),
			winVenue: winVenue, sizeBucket: SizeBucketFor(resReal.NetProfitUSD),
		})
	}

	if cfg.Label != "" {
		if err := persistReplay(ctx, conn, cfg, results); err != nil {
			slog.Warn("stealtime replay: persist failed", "error", err)
		}
	}

	replayReport(results, windowDays, cfg, gasNote)
	return nil
}

func loadCrashDayLiquidations(ctx context.Context, conn driver.Conn, cfg ReplayConfig) ([]replayLiq, float64, error) {
	var minB, maxB uint64
	if err := conn.QueryRow(ctx, `SELECT min(taken_block), max(taken_block) FROM stealtime_results WHERE chain_id = ?`, cfg.ChainID).Scan(&minB, &maxB); err != nil {
		return nil, 0, err
	}
	windowDays := float64(maxB-minB) * 2 / 86400

	size := cfg.MinSizeUSD1e18.String()
	// Restrict raw_blocks to the backtest block range so the join builds a small
	// right side (a few million rows) instead of the whole chain (88M, OOM).
	q := fmt.Sprintf(`
		WITH blk AS (
			SELECT block_number, block_time FROM raw_blocks
			WHERE chain_id = %d AND block_number >= %d AND block_number <= %d
		),
		crash AS (
			SELECT toDate(b.block_time) AS day
			FROM stealtime_results s
			INNER JOIN blk b ON b.block_number = toUInt32(s.taken_block)
			WHERE s.chain_id = %d AND s.repaid_usd > %s
			GROUP BY day ORDER BY count() DESC LIMIT %d
		)
		SELECT s.protocol, s.account, s.liquidator, s.collateral_asset, s.debt_asset,
			s.taken_block, s.crossing_block, s.steal_time, toString(toDate(b.block_time))
		FROM stealtime_results s
		INNER JOIN blk b ON b.block_number = toUInt32(s.taken_block)
		WHERE s.chain_id = %d AND s.evaluated AND s.repaid_usd > %s
			AND toDate(b.block_time) IN (SELECT day FROM crash)
	`, cfg.ChainID, minB, maxB, cfg.ChainID, size, cfg.TopDays, cfg.ChainID, size)

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var out []replayLiq
	for rows.Next() {
		var protocol, day string
		var account, liquidator, collateral, debt [20]byte
		var taken, crossing, steal uint64
		if err := rows.Scan(&protocol, &account, &liquidator, &collateral, &debt, &taken, &crossing, &steal, &day); err != nil {
			return nil, 0, err
		}
		out = append(out, replayLiq{
			liq: Liquidation{
				Protocol: protocol, Account: common.BytesToAddress(account[:]), Liquidator: common.BytesToAddress(liquidator[:]),
				CollateralAsset: common.BytesToAddress(collateral[:]), DebtAsset: common.BytesToAddress(debt[:]), TakenBlock: taken,
			},
			crossing: crossing, stealTime: steal, day: day,
		})
	}
	return out, windowDays, rows.Err()
}

func ensureReplaySchema(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS replay_results (
		run_at DateTime64(3, 'UTC') DEFAULT now64(3),
		label LowCardinality(String),
		chain_id UInt32,
		protocol LowCardinality(String),
		account FixedString(20),
		liquidator FixedString(20),
		collateral_asset FixedString(20),
		debt_asset FixedString(20),
		taken_block UInt64,
		crossing_block UInt64,
		steal_time UInt64,
		size_bucket LowCardinality(String),
		profitable_v2 Bool,
		profitable_real Bool,
		net_v2 UInt256,
		net_real UInt256,
		win_venue LowCardinality(String)
	) ENGINE = MergeTree ORDER BY (label, chain_id, taken_block)`)
}

func persistReplay(ctx context.Context, conn driver.Conn, cfg ReplayConfig, results []replayResult) error {
	if err := ensureReplaySchema(ctx, conn); err != nil {
		return err
	}
	// Replace any prior rows for this label so a re-run is idempotent, not additive.
	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE replay_results DELETE WHERE label = '%s' AND chain_id = %d", sqlEscape(cfg.Label), cfg.ChainID)); err != nil {
		return err
	}
	if len(results) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO replay_results (
		run_at, label, chain_id, protocol, account, liquidator, collateral_asset, debt_asset,
		taken_block, crossing_block, steal_time, size_bucket,
		profitable_v2, profitable_real, net_v2, net_real, win_venue
	)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, r := range results {
		if err := batch.Append(
			now, cfg.Label, cfg.ChainID, r.protocol, r.account.Bytes(), r.liquidator.Bytes(),
			r.collateral.Bytes(), r.debt.Bytes(),
			r.takenBlock, r.crossingBlock, r.stealTime, r.sizeBucket,
			r.profitableV2, r.profitableReal, r.netV2, r.netReal, r.winVenue,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func replayReport(results []replayResult, windowDays float64, cfg ReplayConfig, gasNote string) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== crash-day capture replay (REAL concentrated-liquidity venues) ===\n")
	fmt.Fprintf(&b, "label=%q  evaluated=%d  window_days=%d  min_profit=$%.0f  gas_units=%d\n",
		cfg.Label, len(results), int(windowDays), usdFloat(cfg.MinProfitUSD1e18), cfg.GasUnits)
	fmt.Fprintf(&b, "venues: LFJ Liquidity Book + Uniswap V3 + Pharaoh CL (V2 routers attributed fallback)\n")
	fmt.Fprintf(&b, "gas basis: %s\n", gasNote)

	profV2, profReal := 0, 0
	for _, r := range results {
		if r.profitableV2 {
			profV2++
		}
		if r.profitableReal {
			profReal++
		}
	}
	fmt.Fprintf(&b, "profitable (sized, crash-day gas): V2-only=%d  real-venues=%d  of %d\n\n", profV2, profReal, len(results))

	fmt.Fprintf(&b, "Step 3: real-venue capture by reaction budget R (UPPER BOUND: assumes ready-first wins)\n")
	for _, R := range reactionBudgets {
		win, sum := 0, big.NewInt(0)
		byProto := map[string]int{}
		bySize := map[string]int{}
		for _, r := range results {
			if r.profitableReal && r.stealTime > R {
				win++
				sum = new(big.Int).Add(sum, r.netReal)
				byProto[r.protocol]++
				bySize[r.sizeBucket]++
			}
		}
		rate := 0.0
		if profReal > 0 {
			rate = float64(win) / float64(profReal)
		}
		fmt.Fprintf(&b, "  R=%d: winnable=%d  capture=$%.0f  rate=%.1f%%  (aave=%d benqi=%d | small=%d medium=%d large=%d)\n",
			R, win, usdFloat(sum), rate*100, byProto["aave-v3"], byProto["benqi"], bySize["small"], bySize["medium"], bySize["large"])
	}

	// V2-only vs real on the R=2 winnable set: does "V2 is sufficient" survive?
	v2Sum, realSum := big.NewInt(0), big.NewInt(0)
	v2Win, realWin := 0, 0
	for _, r := range results {
		if r.stealTime <= 2 {
			continue
		}
		if r.profitableV2 {
			v2Win++
			v2Sum = new(big.Int).Add(v2Sum, r.netV2)
		}
		if r.profitableReal {
			realWin++
			realSum = new(big.Int).Add(realSum, r.netReal)
		}
	}
	uplift := new(big.Int).Sub(realSum, v2Sum)
	fmt.Fprintf(&b, "\nStep 6: V2-only vs real venues, R=2 winnable set\n")
	fmt.Fprintf(&b, "  V2-only:      winnable=%d  capture=$%.0f\n", v2Win, usdFloat(v2Sum))
	fmt.Fprintf(&b, "  real venues:  winnable=%d  capture=$%.0f\n", realWin, usdFloat(realSum))
	fmt.Fprintf(&b, "  real over V2: +$%.0f (%.0f%%)\n", usdFloat(uplift), pct(uplift, v2Sum))

	// Venue attribution: which venue wins the route, overall and for large unwinds.
	fmt.Fprintf(&b, "\nVenue wins among real-venue profitable liquidations (which venue actually fills):\n")
	fmt.Fprintf(&b, "  all sizes: %s\n", venueTally(results, ""))
	fmt.Fprintf(&b, "  large only: %s\n", venueTally(results, "large"))

	// Profit concentration: how much of the R=2 capture sits in the top few.
	fmt.Fprintf(&b, "\nProfit concentration (real-venue, R=2 winnable set):\n%s", concentration(results))

	fmt.Print(b.String())
}

// venueTally counts winning venues among real-venue profitable results, optionally
// restricted to one size bucket.
func venueTally(results []replayResult, sizeBucket string) string {
	counts := map[string]int{}
	total := 0
	for _, r := range results {
		if !r.profitableReal {
			continue
		}
		if sizeBucket != "" && r.sizeBucket != sizeBucket {
			continue
		}
		counts[r.winVenue]++
		total++
	}
	if total == 0 {
		return "(none)"
	}
	type kv struct {
		k string
		v int
	}
	list := make([]kv, 0, len(counts))
	for k, v := range counts {
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].v > list[j].v })
	parts := make([]string, 0, len(list))
	for _, it := range list {
		parts = append(parts, fmt.Sprintf("%s=%d (%.0f%%)", it.k, it.v, float64(it.v)/float64(total)*100))
	}
	return strings.Join(parts, "  ")
}

func concentration(results []replayResult) string {
	nets := make([]*big.Int, 0)
	total := big.NewInt(0)
	for _, r := range results {
		if r.profitableReal && r.stealTime > 2 {
			nets = append(nets, r.netReal)
			total = new(big.Int).Add(total, r.netReal)
		}
	}
	if len(nets) == 0 || total.Sign() == 0 {
		return "  (no winnable set)\n"
	}
	sort.Slice(nets, func(i, j int) bool { return nets[i].Cmp(nets[j]) > 0 })
	var b strings.Builder
	for _, topN := range []int{1, 3, 5, 10} {
		if topN > len(nets) {
			break
		}
		sum := big.NewInt(0)
		for i := 0; i < topN; i++ {
			sum = new(big.Int).Add(sum, nets[i])
		}
		fmt.Fprintf(&b, "  top-%d: $%.0f of $%.0f (%.0f%%)\n", topN, usdFloat(sum), usdFloat(total), pct(sum, total))
	}
	return b.String()
}

// clampNonNeg returns max(n, 0). net_v2 / net_real are UInt256 columns, so a
// negative net (an unprofitable liquidation) would wrap to ~2^256 on insert;
// clamp to 0 to match the stealtime_results convention. Reports only ever sum
// profitable (positive) rows, so this never changes a capture number.
func clampNonNeg(n *big.Int) *big.Int {
	if n == nil || n.Sign() < 0 {
		return big.NewInt(0)
	}
	return n
}

func pct(part, whole *big.Int) float64 {
	if whole == nil || whole.Sign() == 0 {
		return 0
	}
	p := new(big.Float).Quo(new(big.Float).SetInt(part), new(big.Float).SetInt(whole))
	v, _ := p.Float64()
	return v * 100
}

func gweiFloat(wei *big.Int) float64 {
	if wei == nil {
		return 0
	}
	f := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e9))
	v, _ := f.Float64()
	return v
}

func sqlEscape(s string) string { return strings.ReplaceAll(s, "'", "''") }

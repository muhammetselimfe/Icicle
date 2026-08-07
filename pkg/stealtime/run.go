package stealtime

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
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

// Config configures the steal-time backtest.
type Config struct {
	ChainID           uint32
	ArchiveRPC        string
	FallbackRPC       string
	FromBlock         uint64
	ToBlock           uint64
	MaxLookbackBlocks uint64
	SampleStride      uint64
	MinDebtUSD1e18    *big.Int // skip liquidations whose repaid debt is below this, before the heavy work
	MinProfitUSD1e18  *big.Int
	GasUnits          uint64
	TopN              int
	Persist           bool
	Debug             bool // log per-position legs and result for the first DebugN evaluated
	DebugN            int
}

// Run executes the backtest over the configured block range.
func Run(ctx context.Context, conn driver.Conn, cfg Config) error {
	start := time.Now()
	rpc := lending.NewClient(cfg.ArchiveRPC, cfg.FallbackRPC)
	store, err := lending.NewStore(conn, cfg.ChainID)
	if err != nil {
		return err
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

	if err := ensureSchema(ctx, conn); err != nil {
		return err
	}

	liqs, err := Enumerate(ctx, store, aaveAddrs.Pool, benqiAddrs.Markets, cfg.FromBlock, cfg.ToBlock)
	if err != nil {
		return fmt.Errorf("enumerate: %w", err)
	}
	slog.Info("stealtime: enumerated liquidations", "count", len(liqs), "from", cfg.FromBlock, "to", cfg.ToBlock)

	reasons := map[prefilter.Reason]int{}
	var obs []Observation
	var rows []resultRow
	var scanned, profitable, quoterCalls, quoterFails, debugged, dustSkipped int

	for _, liq := range liqs {
		scanned++
		if scanned%2000 == 0 {
			slog.Info("stealtime: progress", "scanned", scanned, "of", len(liqs),
				"dust_prefiltered", dustSkipped, "evaluated", scanned-dustSkipped, "profitable", profitable)
		}
		adapter := lending.Adapter(aaveAd)
		poolOrComp := aavePool
		if liq.Protocol == "benqi" {
			adapter = benqiAd
			poolOrComp = benqiComptroller
		}

		// Cheap dust pre-filter: value the repaid debt at the liquidation block and
		// skip sub-threshold liquidations before any crossing or quote work. Every
		// liquidation is still persisted (with an evaluated flag) so the full field
		// is visible for taker-composition and in-window presence analysis.
		repaid := big.NewInt(0)
		if cfg.MinDebtUSD1e18 != nil && cfg.MinDebtUSD1e18.Sign() > 0 {
			oracleAtTaken := common.HexToAddress(addrRes.at(ctx, liq.Protocol, liq.TakenBlock).Oracle)
			debtInfo, _ := resolver.Resolve(ctx, liq.Protocol, liq.DebtAsset)
			repaid = valueRepaidUSD(ctx, rpc, oracleAtTaken, liq.Protocol, liq, debtInfo.Decimals, liq.TakenBlock)
			if repaid.Cmp(cfg.MinDebtUSD1e18) < 0 {
				dustSkipped++
				rows = append(rows, newResultRow(liq, repaid, false, 0, 0, false, false, "dust_prefiltered", nil))
				continue
			}
		}

		floor := uint64(0)
		if liq.TakenBlock > cfg.MaxLookbackBlocks {
			floor = liq.TakenBlock - cfg.MaxLookbackBlocks
		}

		crossing, err := FindCrossingByScan(liq.TakenBlock, floor, cfg.SampleStride, func(b uint64) (bool, error) {
			return liquidatableAtBlock(ctx, rpc, liq.Protocol, liq.Account, poolOrComp, b)
		})
		if err != nil {
			slog.Warn("stealtime: find crossing failed", "account", liq.Account.Hex(), "error", err)
			rows = append(rows, newResultRow(liq, repaid, false, 0, 0, false, false, "crossing_failed", nil))
			continue
		}
		steal := StealTime(liq.TakenBlock, crossing)

		// Reconfigure the adapter to the addresses as of the crossing block, so the
		// oracle and market list match the historical state we read against.
		blockAddrs := addrRes.at(ctx, liq.Protocol, crossing.CrossingBlock)
		aaveDataProvider := common.HexToAddress(blockAddrs.DataProvider)
		if liq.Protocol == "benqi" {
			benqiAd.Configure(blockAddrs, benqiParams, benqiGlobals)
		} else {
			aaveAd.Configure(blockAddrs, aaveParams, aaveGlobals)
		}

		pos, ok := assemblePosition(ctx, rpc, adapter, resolver, liq.Protocol, liq, crossing.CrossingBlock)
		if !ok {
			rows = append(rows, newResultRow(liq, repaid, false, crossing.CrossingBlock, steal, crossing.Censored, false, "assemble_failed", nil))
			continue
		}
		params := newBlockParams(ctx, rpc, crossing.CrossingBlock, aaveDataProvider, benqiComptroller)
		quoter := newBlockQuoter(rpc, crossing.CrossingBlock)
		cost := buildBlockCost(ctx, rpc, crossing.CrossingBlock, aavePool, cfg.GasUnits, cfg.MinProfitUSD1e18)

		res, err := prefilter.EvaluatePosition(ctx, pos, params, quoter, cost)
		qc, qf := quoter.Stats()
		quoterCalls += qc
		quoterFails += qf
		if err != nil {
			slog.Warn("stealtime: evaluate failed", "account", liq.Account.Hex(), "error", err)
			rows = append(rows, newResultRow(liq, repaid, false, crossing.CrossingBlock, steal, crossing.Censored, false, "evaluate_failed", nil))
			continue
		}
		reasons[res.Reason]++

		if cfg.Debug && debugged < cfg.DebugN {
			debugged++
			slog.Info("stealtime: debug eval",
				"account", liq.Account.Hex(), "protocol", liq.Protocol, "reason", string(res.Reason),
				"crossing", crossing.CrossingBlock, "taken", liq.TakenBlock,
				"net_usd", usdFloat(res.NetProfitUSD), "repay_usd", usdFloat(res.RepayUSD),
				"hf", usdFloat(pos.HealthFactor), "legs", len(pos.Legs))
			for _, l := range pos.Legs {
				slog.Info("stealtime: debug leg",
					"side", sideName(l.Side), "asset", l.Asset.Hex(), "decimals", l.Decimals,
					"amount", l.Amount.String(), "base_usd", usdFloat(l.BaseValue))
			}
		}

		if res.Profitable {
			profitable++
			obs = append(obs, Observation{
				Account: liq.Account, Liquidator: liq.Liquidator, Protocol: liq.Protocol,
				StealTime: steal, Censored: crossing.Censored, NetProfitUSD: res.NetProfitUSD,
				SizeBucket: SizeBucketFor(res.NetProfitUSD), TakenBlock: liq.TakenBlock,
			})
		}
		rows = append(rows, newResultRow(liq, repaid, true, crossing.CrossingBlock, steal, crossing.Censored, res.Profitable, string(res.Reason), res.NetProfitUSD))
	}

	if cfg.Persist {
		if err := persistAll(ctx, conn, cfg.ChainID, rows); err != nil {
			slog.Warn("stealtime: persist failed", "error", err)
		}
	}

	dist := Aggregate(obs, cfg.TopN)
	report(dist, reasons, scanned, dustSkipped, profitable, quoterCalls, quoterFails, time.Since(start))

	slog.Info("stealtime: run complete",
		"liquidations_scanned", scanned, "dust_prefiltered", dustSkipped, "profitable", profitable,
		"quoter_calls", quoterCalls, "quoter_failures", quoterFails, "duration", time.Since(start))
	return nil
}

// bootstrap resolves an adapter at head and returns its addresses and parameters.
// Decimals and risk params are effectively static, so head values are reused for
// historical blocks; only the addresses are re-resolved block-pinned per position.
func bootstrap(ctx context.Context, rpc *lending.Client, a lending.Adapter) (lending.Addresses, []lending.AssetParam, lending.GlobalParams, error) {
	addrs, _, err := a.Resolve(ctx, rpc)
	if err != nil {
		return lending.Addresses{}, nil, lending.GlobalParams{}, err
	}
	a.Configure(addrs, nil, lending.GlobalParams{})
	params, globals, err := a.RefreshParams(ctx, rpc)
	if err != nil {
		return lending.Addresses{}, nil, lending.GlobalParams{}, err
	}
	a.Configure(addrs, params, globals)
	return addrs, params, globals, nil
}

func ensureSchema(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS stealtime_results (
		run_at DateTime64(3, 'UTC') DEFAULT now64(3),
		chain_id UInt32,
		protocol LowCardinality(String),
		account FixedString(20),
		liquidator FixedString(20),
		collateral_asset FixedString(20),
		debt_asset FixedString(20),
		taken_block UInt64,
		crossing_block UInt64,
		steal_time UInt64,
		censored Bool,
		evaluated Bool,
		profitable Bool,
		reason LowCardinality(String),
		net_profit_usd UInt256,
		repaid_usd UInt256,
		size_bucket LowCardinality(String)
	) ENGINE = MergeTree ORDER BY (chain_id, taken_block)`)
}

// resultRow is one persisted liquidation: every enumerated liquidation, evaluated
// or dust-skipped, so the full field is visible for analysis.
type resultRow struct {
	protocol                                        string
	account, liquidator, collateralAsset, debtAsset common.Address
	takenBlock, crossingBlock, stealTime            uint64
	censored, evaluated, profitable                 bool
	reason                                          string
	netProfitUSD, repaidUSD                         *big.Int
	sizeBucket                                      string
}

func newResultRow(liq Liquidation, repaid *big.Int, evaluated bool, crossingBlock, steal uint64, censored, profitable bool, reason string, net *big.Int) resultRow {
	if net == nil || net.Sign() < 0 {
		net = big.NewInt(0) // net_profit_usd is UInt256; rejected rows can be negative
	}
	if repaid == nil || repaid.Sign() < 0 {
		repaid = big.NewInt(0)
	}
	return resultRow{
		protocol: liq.Protocol, account: liq.Account, liquidator: liq.Liquidator,
		collateralAsset: liq.CollateralAsset, debtAsset: liq.DebtAsset,
		takenBlock: liq.TakenBlock, crossingBlock: crossingBlock, stealTime: steal,
		censored: censored, evaluated: evaluated, profitable: profitable, reason: reason,
		netProfitUSD: net, repaidUSD: repaid, sizeBucket: SizeBucketFor(net),
	}
}

func persistAll(ctx context.Context, conn driver.Conn, chainID uint32, rows []resultRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO stealtime_results (
		run_at, chain_id, protocol, account, liquidator, collateral_asset, debt_asset,
		taken_block, crossing_block, steal_time, censored, evaluated, profitable, reason,
		net_profit_usd, repaid_usd, size_bucket
	)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, r := range rows {
		if err := batch.Append(
			now, chainID, r.protocol, r.account.Bytes(), r.liquidator.Bytes(),
			r.collateralAsset.Bytes(), r.debtAsset.Bytes(),
			r.takenBlock, r.crossingBlock, r.stealTime, r.censored, r.evaluated, r.profitable, r.reason,
			r.netProfitUSD, r.repaidUSD, r.sizeBucket,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func report(d Distribution, reasons map[prefilter.Reason]int, scanned, dustSkipped, profitable, qCalls, qFails int, dur time.Duration) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== steal-time backtest ===\n")
	fmt.Fprintf(&b, "scanned=%d dust_prefiltered=%d evaluated=%d profitable=%d quoter_calls=%d quoter_failures=%d duration=%s\n",
		scanned, dustSkipped, scanned-dustSkipped, profitable, qCalls, qFails, dur.Round(time.Millisecond))
	fmt.Fprintf(&b, "reasons: profitable=%d dust=%d illiquid=%d bad_debt=%d no_pair=%d unprofitable=%d\n",
		reasons[prefilter.ReasonProfitable], reasons[prefilter.ReasonDust], reasons[prefilter.ReasonIlliquid],
		reasons[prefilter.ReasonBadDebt], reasons[prefilter.ReasonNoPair], reasons[prefilter.ReasonUnprofit])

	fmt.Fprintf(&b, "\nprofitable liquidations: %d (censored: %d)\n", d.Total, d.Censored)
	fmt.Fprintf(&b, "steal_time blocks: median=%d p90=%d  within_0to2=%.0f%%  beyond_10=%.0f%%\n",
		d.MedianBlocks, d.P90Blocks, d.WithinTwo*100, d.BeyondTen*100)
	fmt.Fprintf(&b, "histogram: 0=%d 1=%d 2=%d 3to5=%d 6to10=%d 11to20=%d 21plus=%d censored=%d\n",
		d.Overall.B0, d.Overall.B1, d.Overall.B2, d.Overall.B3to5, d.Overall.B6to10, d.Overall.B11to20, d.Overall.B21plus, d.Overall.Censored)

	for proto, h := range d.ByProtocol {
		fmt.Fprintf(&b, "  %-8s 0=%d 1=%d 2=%d 3to5=%d 6to10=%d 11to20=%d 21plus=%d censored=%d\n",
			proto, h.B0, h.B1, h.B2, h.B3to5, h.B6to10, h.B11to20, h.B21plus, h.Censored)
	}
	for size, h := range d.BySize {
		fmt.Fprintf(&b, "  size=%-6s 0=%d 1=%d 2=%d 3to5=%d 6to10=%d 11to20=%d 21plus=%d censored=%d\n",
			size, h.B0, h.B1, h.B2, h.B3to5, h.B6to10, h.B11to20, h.B21plus, h.Censored)
	}

	fmt.Fprintf(&b, "total realized profit: $%.2f\n", usdFloat(d.TotalProfit))
	fmt.Fprintf(&b, "incumbent concentration: top-%d share=%.0f%%\n", len(d.TopLiquidators), d.TopNShare*100)
	for _, it := range d.TopLiquidators {
		fmt.Fprintf(&b, "  %s  %d liquidations\n", it.Liquidator.Hex(), it.Count)
	}
	fmt.Print(b.String())
}

func sideName(s prefilter.Side) string {
	if s == prefilter.SideDebt {
		return "debt"
	}
	return "collateral"
}

func usdFloat(n *big.Int) float64 {
	if n == nil {
		return 0
	}
	f := new(big.Float).Quo(new(big.Float).SetInt(n), new(big.Float).SetInt(wad))
	v, _ := f.Float64()
	return v
}

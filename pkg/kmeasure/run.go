package kmeasure

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

	"icicle/pkg/lending"
	"icicle/pkg/prefilter"
)

// backfillLagBlocks is how far behind the raw sync watermark discovery may be and
// still count as caught up.
const backfillLagBlocks = 10000

// Config configures the K-measurement runner.
type Config struct {
	ChainID          uint32
	FeedBaseURL      string
	ArchiveRPC       string
	FallbackRPC      string
	IntervalMinutes  int // 0 = one-shot
	GasUnits         uint64
	MinProfitUSD1e18 *big.Int
	FlashFeeBps      uint64
	MinDebtBase      string // feed pre-cut, default "" so the pre-filter classifies dust
	Routers          []common.Address
	Persist          bool
}

// Runner holds the wired components for repeated runs.
type Runner struct {
	cfg      Config
	conn     driver.Conn
	rpc      *lending.Client
	feed     *FeedClient
	resolver Resolver
	params   *Params
	rich     RichQuoter
}

// NewRunner wires the runner. It loads params once; cost and positions refresh per run.
func NewRunner(ctx context.Context, conn driver.Conn, cfg Config) (*Runner, error) {
	rpc := lending.NewClient(cfg.ArchiveRPC, cfg.FallbackRPC)
	params, err := LoadParams(ctx, conn, cfg.ChainID)
	if err != nil {
		return nil, fmt.Errorf("load params: %w", err)
	}
	if err := ensureSchema(ctx, conn); err != nil {
		return nil, err
	}
	return &Runner{
		cfg:      cfg,
		conn:     conn,
		rpc:      rpc,
		feed:     NewFeedClient(cfg.FeedBaseURL),
		resolver: NewChainResolver(rpc),
		params:   params,
		rich:     NewDexQuoter(rpc, cfg.Routers),
	}, nil
}

// Run executes one-shot, or repeats every IntervalMinutes until the context ends.
func (r *Runner) Run(ctx context.Context) error {
	registerMetrics()
	for {
		if err := r.runOnce(ctx); err != nil {
			slog.Error("kmeasure: run failed", "error", err)
		}
		if r.cfg.IntervalMinutes <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Duration(r.cfg.IntervalMinutes) * time.Minute):
		}
	}
}

func (r *Runner) runOnce(ctx context.Context) error {
	complete, head, disc := r.backfillStatus(ctx)
	if !complete {
		slog.Warn("kmeasure: lending backfill NOT complete, K undercounts the denominator",
			"discovery_block", disc, "head_block", head, "behind", int64(head)-int64(disc))
	} else {
		slog.Info("kmeasure: backfill complete", "discovery_block", disc, "head_block", head)
	}

	if s, err := r.feed.Stats(ctx); err == nil {
		for _, st := range s.Data {
			slog.Info("kmeasure: feed stats", "protocol", st.Protocol, "open", st.OpenPositions, "liquidatable", st.Liquidatable)
		}
	}

	raws, err := r.feed.FetchLiquidatable(ctx, r.cfg.MinDebtBase)
	if err != nil {
		return fmt.Errorf("fetch liquidatable: %w", err)
	}
	positions, err := BuildPositions(ctx, raws, r.resolver)
	if err != nil {
		return fmt.Errorf("build positions: %w", err)
	}

	cost := BuildCostModel(ctx, r.rpc, r.conn, r.cfg.ChainID, CostOpts{
		GasUnits:          r.cfg.GasUnits,
		MinProfitUSD1e18:  r.cfg.MinProfitUSD1e18,
		FlashFeeBps:       r.cfg.FlashFeeBps,
		GasPriceWeiFallbk: big.NewInt(25_000_000_000),
		NativeUSDFallback: new(big.Int).Mul(big.NewInt(20), lending.WAD),
	})

	adapter := NewQuoterAdapter(r.rich)
	summary, err := prefilter.ComputeK(ctx, positions, r.params, adapter, cost)
	if err != nil {
		return fmt.Errorf("compute K: %w", err)
	}

	calls, failures, avg := adapter.Stats()
	metricEvaluated.Set(float64(summary.Total))
	metricK.Set(float64(summary.Profitable))
	metricQuoterCalls.Add(float64(calls))
	metricQuoterFailures.Add(float64(failures))

	report(summary, cost, calls, failures, avg, complete)

	if r.cfg.Persist {
		if err := r.persist(ctx, summary, cost, calls, failures, complete); err != nil {
			slog.Warn("kmeasure: persist failed", "error", err)
		}
	}
	return nil
}

// backfillStatus compares the lending discovery watermark to the raw sync head.
func (r *Runner) backfillStatus(ctx context.Context) (complete bool, head, disc uint64) {
	var h uint32
	_ = r.conn.QueryRow(ctx, `SELECT block_number FROM sync_watermark WHERE chain_id = ?`, r.cfg.ChainID).Scan(&h)
	head = uint64(h)

	var d uint64
	_ = r.conn.QueryRow(ctx, `
		SELECT min(last_block_num) FROM (SELECT * FROM indexer_watermarks FINAL)
		WHERE chain_id = ? AND indexer_name LIKE 'lending_discovery:%'
	`, r.cfg.ChainID).Scan(&d)
	disc = d

	complete = disc > 0 && head > 0 && disc+backfillLagBlocks >= head
	return complete, head, disc
}

func (r *Runner) persist(ctx context.Context, s prefilter.Summary, cost prefilter.CostModel, calls, failures int, complete bool) error {
	batch, err := r.conn.PrepareBatch(ctx, `INSERT INTO kmeasure_runs (
		run_at, chain_id, backfill_complete, evaluated, k,
		r_profitable, r_dust, r_illiquid, r_bad_debt, r_no_pair, r_unprofitable,
		quoter_calls, quoter_failures,
		gas_price_wei, native_usd_1e18, flash_fee_bps, min_profit_usd_1e18
	)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	if err := batch.Append(
		time.Now().UTC(), r.cfg.ChainID, complete, uint32(s.Total), uint32(s.Profitable),
		uint32(s.ByReason[prefilter.ReasonProfitable]), uint32(s.ByReason[prefilter.ReasonDust]),
		uint32(s.ByReason[prefilter.ReasonIlliquid]), uint32(s.ByReason[prefilter.ReasonBadDebt]),
		uint32(s.ByReason[prefilter.ReasonNoPair]), uint32(s.ByReason[prefilter.ReasonUnprofit]),
		uint32(calls), uint32(failures),
		cost.GasPriceWei.Uint64(), orZero(cost.NativeUSD1e18), uint16(cost.FlashFeeBps), orZero(cost.MinProfitUSD1e18),
	); err != nil {
		return err
	}
	return batch.Send()
}

// report prints a human-readable summary plus the profitable shortlist.
func report(s prefilter.Summary, cost prefilter.CostModel, calls, failures int, avg time.Duration, complete bool) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== K-measurement ===\n")
	fmt.Fprintf(&b, "backfill_complete=%v  flash_fee_bps=%d  gas_price_gwei=%.1f  avax_usd=$%.2f  min_profit=$%.2f\n",
		complete, cost.FlashFeeBps, gwei(cost.GasPriceWei), usdFloat(cost.NativeUSD1e18), usdFloat(cost.MinProfitUSD1e18))
	fmt.Fprintf(&b, "evaluated=%d  K(profitable)=%d  quoter_calls=%d quoter_failures=%d avg_quote=%s\n",
		s.Total, s.Profitable, calls, failures, avg.Round(time.Millisecond))
	fmt.Fprintf(&b, "byReason: profitable=%d dust=%d illiquid=%d bad_debt=%d no_pair=%d unprofitable=%d\n",
		s.ByReason[prefilter.ReasonProfitable], s.ByReason[prefilter.ReasonDust], s.ByReason[prefilter.ReasonIlliquid],
		s.ByReason[prefilter.ReasonBadDebt], s.ByReason[prefilter.ReasonNoPair], s.ByReason[prefilter.ReasonUnprofit])

	profitable := make([]prefilter.Result, 0)
	for _, res := range s.Results {
		if res.Profitable {
			profitable = append(profitable, res)
		}
	}
	sort.Slice(profitable, func(i, j int) bool {
		return profitable[i].NetProfitUSD.Cmp(profitable[j].NetProfitUSD) > 0
	})
	if len(profitable) > 0 {
		fmt.Fprintf(&b, "profitable shortlist (account, protocol, debt -> collateral, net USD):\n")
		for _, res := range profitable {
			fmt.Fprintf(&b, "  %s %-8s %s -> %s  $%.2f\n",
				res.Account.Hex(), res.Protocol, short(res.DebtAsset), short(res.CollateralAsset), usdFloat(res.NetProfitUSD))
		}
	}
	fmt.Print(b.String())
}

func ensureSchema(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS kmeasure_runs (
		run_at DateTime64(3, 'UTC') DEFAULT now64(3),
		chain_id UInt32,
		backfill_complete Bool,
		evaluated UInt32,
		k UInt32,
		r_profitable UInt32, r_dust UInt32, r_illiquid UInt32, r_bad_debt UInt32, r_no_pair UInt32, r_unprofitable UInt32,
		quoter_calls UInt32, quoter_failures UInt32,
		gas_price_wei UInt64, native_usd_1e18 UInt256, flash_fee_bps UInt16, min_profit_usd_1e18 UInt256
	) ENGINE = MergeTree ORDER BY (chain_id, run_at)`)
}

func orZero(n *big.Int) *big.Int {
	if n == nil {
		return big.NewInt(0)
	}
	return n
}

func usdFloat(n *big.Int) float64 {
	if n == nil {
		return 0
	}
	f := new(big.Float).Quo(new(big.Float).SetInt(n), new(big.Float).SetInt(lending.WAD))
	v, _ := f.Float64()
	return v
}

func gwei(wei *big.Int) float64 {
	if wei == nil {
		return 0
	}
	f := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e9))
	v, _ := f.Float64()
	return v
}

func short(a common.Address) string {
	h := a.Hex()
	if len(h) < 10 {
		return h
	}
	return h[:10]
}

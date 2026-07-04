package cmd

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"icicle/pkg/chwrapper"
	"icicle/pkg/pchainrpc"
	"icicle/pkg/pchainsyncer"
)

// ValidatorBackfillOptions configures the one-time historical backfill of
// validator_count_snapshots.
type ValidatorBackfillOptions struct {
	From     string // start date, YYYY-MM-DD (default: mainnet launch month)
	To       string // end date, YYYY-MM-DD (default: today)
	Interval string // day | week | month
}

// RunValidatorBackfill samples exact historical validator counts (Primary
// Network + all L1 subnets) via platform.getValidatorsAt and stores them in
// validator_count_snapshots. Idempotent - re-running replaces existing rows,
// and a finer interval can be used later to densify the series.
func RunValidatorBackfill(ctx context.Context, opts ValidatorBackfillOptions) {
	configs, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	var pcfg *ChainConfig
	for i := range configs {
		if configs[i].VM == "p" {
			pcfg = &configs[i]
			break
		}
	}
	if pcfg == nil {
		log.Fatal("No P-Chain (vm: p) entry found in config.yaml")
	}

	dates, err := backfillDates(opts)
	if err != nil {
		log.Fatalf("Invalid backfill options: %v", err)
	}
	if len(dates) == 0 {
		log.Fatal("Empty date range")
	}

	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	fetcher := pchainrpc.NewFetcher(pchainrpc.FetcherOptions{RpcURL: pcfg.RpcURL})
	defer fetcher.Close()

	slog.Info("Starting validator count backfill",
		"from", dates[0].Format("2006-01-02"),
		"to", dates[len(dates)-1].Format("2006-01-02"),
		"interval", opts.Interval,
		"samples", len(dates),
		"rpc", pcfg.RpcURL)

	start := time.Now()
	if err := pchainsyncer.SampleValidatorCounts(ctx, conn, fetcher, pcfg.ChainID, dates); err != nil {
		log.Fatalf("Backfill failed: %v", err)
	}
	slog.Info("Validator count backfill complete", "elapsed", time.Since(start))
}

// backfillDates expands the options into concrete sample dates (UTC).
func backfillDates(opts ValidatorBackfillOptions) ([]time.Time, error) {
	from := time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC) // first full month after mainnet launch
	if opts.From != "" {
		t, err := time.Parse("2006-01-02", opts.From)
		if err != nil {
			return nil, fmt.Errorf("bad --from %q: %w", opts.From, err)
		}
		from = t.UTC()
	}

	to := time.Now().UTC().Truncate(24 * time.Hour)
	if opts.To != "" {
		t, err := time.Parse("2006-01-02", opts.To)
		if err != nil {
			return nil, fmt.Errorf("bad --to %q: %w", opts.To, err)
		}
		to = t.UTC()
	}
	if to.Before(from) {
		return nil, fmt.Errorf("--to %s is before --from %s", to.Format("2006-01-02"), from.Format("2006-01-02"))
	}

	var step func(time.Time) time.Time
	switch opts.Interval {
	case "day":
		step = func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }
	case "week":
		step = func(t time.Time) time.Time { return t.AddDate(0, 0, 7) }
	case "month", "":
		// Snap to month starts so samples align with chart period boundaries.
		from = time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
		step = func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }
	default:
		return nil, fmt.Errorf("bad --interval %q (day|week|month)", opts.Interval)
	}

	var dates []time.Time
	for d := from; !d.After(to); d = step(d) {
		dates = append(dates, d)
	}
	// Always include the end date itself so the series reaches "now".
	if len(dates) > 0 && !dates[len(dates)-1].Equal(to) {
		dates = append(dates, to)
	}
	return dates, nil
}

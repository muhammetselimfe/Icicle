package pchainsyncer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"icicle/pkg/chwrapper"
	"icicle/pkg/pchainrpc"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// PrimaryNetworkSubnetID is the Primary Network's subnet ID on mainnet.
const PrimaryNetworkSubnetID = "11111111111111111111111111111111LpoYY"

// DefaultSampleConcurrency caps parallel getValidatorsAt calls against the
// node. Deep-history heights (2020-2022, ~20M+ blocks behind tip) force the
// node to rebuild ancient validator sets from its diffs, which is expensive -
// 8 of those in parallel made the node shed all connections and fail health
// checks (2026-07-04). Keep this low: samples at or near tip are cheap anyway.
const DefaultSampleConcurrency = 2

// subnetWindow is an L1 subnet and the time it was converted, used to skip
// getValidatorsAt calls at heights before the subnet existed.
type subnetWindow struct {
	subnetID       string
	conversionTime time.Time
}

// countRow is one sampled (date, subnet) validator count ready for insert.
type countRow struct {
	date     time.Time
	subnetID string
	height   uint64
	count    uint32
}

// SampleValidatorCounts samples the exact active validator count for the
// Primary Network and every known L1 subnet at each of the given dates, via
// platform.getValidatorsAt, and stores the results in
// validator_count_snapshots. The date-to-height mapping comes from
// p_chain_txs (max block at or before end of the date, UTC). Idempotent:
// re-running replaces existing rows (ReplacingMergeTree).
//
// Per-subnet RPC failures are logged and skipped (absent row, not a fake
// zero) so a rerun can fill gaps. Returns an error only when nothing could be
// sampled at all. concurrency <= 0 falls back to DefaultSampleConcurrency.
//
// Dates are processed one at a time and each date's rows are inserted as soon
// as that date completes, so an interrupted run keeps everything it finished
// (the first backfill attempt batched all inserts to the end and a Ctrl-C
// threw away 25 minutes of successful samples). Sequential dates also keep
// the load on the node to one date's fan-out at a time.
func SampleValidatorCounts(ctx context.Context, conn clickhouse.Conn, fetcher *pchainrpc.Fetcher, pchainID uint32, dates []time.Time, concurrency int) error {
	if len(dates) == 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = DefaultSampleConcurrency
	}

	subnets, err := loadL1SubnetWindows(ctx, conn, pchainID)
	if err != nil {
		return fmt.Errorf("failed to load L1 subnets: %w", err)
	}
	slog.Info("Sampling validator counts", "dates", len(dates), "l1_subnets", len(subnets))

	var sampled, failed, datesDone int

	for _, date := range dates {
		if ctx.Err() != nil {
			break
		}

		height, err := heightAtDate(ctx, conn, pchainID, date)
		if err != nil {
			slog.Warn("Failed to resolve P-Chain height for date, skipping", "date", date.Format("2006-01-02"), "error", err)
			continue
		}
		if height == 0 {
			// Date predates the chain - nothing to sample.
			continue
		}

		// Primary Network plus every L1 subnet already converted by this date.
		targets := []string{PrimaryNetworkSubnetID}
		for _, s := range subnets {
			if s.conversionTime.IsZero() || !s.conversionTime.After(endOfDayUTC(date)) {
				targets = append(targets, s.subnetID)
			}
		}

		var (
			mu   sync.Mutex
			rows []countRow
		)
		sem := make(chan struct{}, concurrency)
		var wg sync.WaitGroup

		for _, subnetID := range targets {
			wg.Add(1)
			sem <- struct{}{}
			go func(subnetID string) {
				defer wg.Done()
				defer func() { <-sem }()

				validators, err := fetcher.GetValidatorsAt(ctx, subnetID, height)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					failed++
					slog.Warn("getValidatorsAt failed, skipping sample", "date", date.Format("2006-01-02"), "subnet_id", subnetID, "height", height, "error", err)
					return
				}
				rows = append(rows, countRow{date: date, subnetID: subnetID, height: height, count: uint32(len(validators))})
			}(subnetID)
		}
		wg.Wait()

		if len(rows) == 0 {
			continue
		}
		if err := insertValidatorCounts(ctx, conn, pchainID, rows); err != nil {
			return fmt.Errorf("failed to insert validator counts for %s: %w", date.Format("2006-01-02"), err)
		}
		sampled += len(rows)
		datesDone++
		slog.Info("Sampled validator counts for date", "date", date.Format("2006-01-02"), "subnets", len(rows), "progress", fmt.Sprintf("%d/%d", datesDone, len(dates)))
	}

	if sampled == 0 {
		return fmt.Errorf("no validator counts sampled (%d failures)", failed)
	}

	slog.Info("Validator count sampling complete", "sampled", sampled, "failed", failed, "dates_done", datesDone, "dates", len(dates))
	return nil
}

// SampleValidatorCountsToday takes today's snapshot if it hasn't been taken
// yet. Called from the validator syncer's periodic cycle - the existence
// check makes repeat calls within a day a cheap no-op.
func SampleValidatorCountsToday(ctx context.Context, conn clickhouse.Conn, fetcher *pchainrpc.Fetcher, pchainID uint32) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)

	rctx, cancel := chwrapper.ReadContext(ctx)
	defer cancel()
	var existing uint64
	err := conn.QueryRow(rctx, `
		SELECT count() FROM validator_count_snapshots
		WHERE p_chain_id = ? AND subnet_id = ? AND snapshot_date = ?
	`, pchainID, PrimaryNetworkSubnetID, today).Scan(&existing)
	if err != nil {
		return fmt.Errorf("failed to check existing snapshot: %w", err)
	}
	if existing > 0 {
		return nil
	}

	// Today's sample reads at (near) tip, which is cheap for the node - but a
	// higher fan-out buys little for 278 calls, so reuse the safe default.
	return SampleValidatorCounts(ctx, conn, fetcher, pchainID, []time.Time{today}, DefaultSampleConcurrency)
}

// loadL1SubnetWindows returns all known L1 subnets with their conversion time.
func loadL1SubnetWindows(ctx context.Context, conn clickhouse.Conn, pchainID uint32) ([]subnetWindow, error) {
	ctx, cancel := chwrapper.ReadContext(ctx)
	defer cancel()

	rows, err := conn.Query(ctx, `
		SELECT subnet_id, max(conversion_time)
		FROM l1_subnets FINAL
		WHERE p_chain_id = ?
		GROUP BY subnet_id
	`, pchainID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []subnetWindow
	for rows.Next() {
		var s subnetWindow
		if err := rows.Scan(&s.subnetID, &s.conversionTime); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// heightAtDate resolves the P-Chain height as of end of the given date (UTC)
// from indexed transactions. Returns 0 when the date predates the chain.
func heightAtDate(ctx context.Context, conn clickhouse.Conn, pchainID uint32, date time.Time) (uint64, error) {
	ctx, cancel := chwrapper.ReadContext(ctx)
	defer cancel()

	var height uint64
	err := conn.QueryRow(ctx, `
		SELECT max(block_number) FROM p_chain_txs
		WHERE p_chain_id = ? AND block_time <= ?
	`, pchainID, endOfDayUTC(date)).Scan(&height)
	return height, err
}

func endOfDayUTC(date time.Time) time.Time {
	d := date.UTC().Truncate(24 * time.Hour)
	return d.Add(24*time.Hour - time.Millisecond)
}

// insertValidatorCounts batch-inserts sampled counts.
func insertValidatorCounts(ctx context.Context, conn clickhouse.Conn, pchainID uint32, rows []countRow) error {
	ctx, cancel := chwrapper.WriteContext(ctx)
	defer cancel()

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO validator_count_snapshots (
		snapshot_date, subnet_id, p_chain_height, validator_count, p_chain_id
	)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r.date, r.subnetID, r.height, r.count, pchainID); err != nil {
			return err
		}
	}
	return batch.Send()
}

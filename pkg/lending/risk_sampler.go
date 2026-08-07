package lending

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"
)

// RiskSampler periodically snapshots aggregate risk metrics per protocol into
// lending_risk_snapshots, so the dashboard can chart whether risk is building or
// easing over time. It samples the same dust-free aggregates the /lending/risk
// endpoint serves on demand, at a fixed debt floor and near-liquidation band so the
// series is comparable across time.
type RiskSampler struct {
	store    *Store
	chainID  uint32
	interval time.Duration
	floor    *big.Int // min_debt_base
	nearMax  *big.Int // near-liquidation HF ceiling (1e18-scaled)
}

// NewRiskSampler builds a sampler. A nil floor defaults to $1 (1e18) and a nil
// nearMax to 1.1 (1.1e18).
func NewRiskSampler(store *Store, chainID uint32, interval time.Duration, floor, nearMax *big.Int) *RiskSampler {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	if floor == nil {
		floor = new(big.Int).Set(WAD)
	}
	if nearMax == nil {
		nearMax = new(big.Int).Div(new(big.Int).Mul(big.NewInt(11), WAD), big.NewInt(10))
	}
	return &RiskSampler{store: store, chainID: chainID, interval: interval, floor: floor, nearMax: nearMax}
}

// Loop snapshots once on start (so a fresh deploy has a point) and then every
// interval until the context is cancelled.
func (rs *RiskSampler) Loop(ctx context.Context) {
	ticker := time.NewTicker(rs.interval)
	defer ticker.Stop()
	rs.sampleSafe(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rs.sampleSafe(ctx)
		}
	}
}

func (rs *RiskSampler) sampleSafe(ctx context.Context) {
	if err := rs.Sample(ctx); err != nil {
		slog.Warn("lending: risk snapshot failed", "error", err)
	}
}

// Sample computes the per-protocol aggregates over the deduped current positions
// and appends one snapshot row per protocol.
func (rs *RiskSampler) Sample(ctx context.Context) error {
	floor := rs.floor.String()
	nearMax := rs.nearMax.String()
	wad := WAD.String()

	q := fmt.Sprintf(`
		SELECT protocol,
			countIf(debt > %[1]s) AS open_positions,
			countIf(liq AND debt > %[1]s) AS liquidatable,
			countIf(coll < debt AND debt > %[1]s) AS bad_debt_count,
			sumIf(debt - coll, coll < debt AND debt > %[1]s) AS bad_debt_total,
			countIf(NOT liq AND hf >= %[2]s AND hf < %[3]s AND debt > %[1]s) AS near_count,
			sumIf(coll, (liq OR hf < %[3]s) AND debt > %[1]s) AS var_coll,
			sumIf(debt, (liq OR hf < %[3]s) AND debt > %[1]s) AS var_debt
		FROM (
			SELECT account, protocol,
				argMax(health_factor, updated_at) AS hf,
				argMax(collateral_base, updated_at) AS coll,
				argMax(debt_base, updated_at) AS debt,
				argMax(liquidatable, updated_at) AS liq
			FROM lending_positions
			WHERE chain_id = ?
			GROUP BY account, protocol
		)
		GROUP BY protocol
	`, floor, wad, nearMax)

	rows, err := rs.store.conn.Query(ctx, q, rs.chainID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type snap struct {
		protocol                               string
		open, liquidatable, badDebtCount, near uint64
		badDebtTotal, varColl, varDebt         *big.Int
	}
	var snaps []snap
	for rows.Next() {
		var s snap
		if err := rows.Scan(&s.protocol, &s.open, &s.liquidatable, &s.badDebtCount, &s.badDebtTotal, &s.near, &s.varColl, &s.varDebt); err != nil {
			return err
		}
		snaps = append(snaps, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(snaps) == 0 {
		return nil
	}

	batch, err := rs.store.conn.PrepareBatch(ctx, `INSERT INTO lending_risk_snapshots (
		chain_id, protocol, snapshot_at, min_debt_base,
		open_positions, liquidatable, bad_debt_count, bad_debt_total,
		near_count, var_collateral, var_debt
	)`)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	now := time.Now().UTC()
	for _, s := range snaps {
		if err := batch.Append(
			rs.chainID, s.protocol, now, rs.floor,
			uint32(s.open), uint32(s.liquidatable), uint32(s.badDebtCount), orZero(s.badDebtTotal),
			uint32(s.near), orZero(s.varColl), orZero(s.varDebt),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

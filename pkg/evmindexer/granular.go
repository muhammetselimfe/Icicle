package evmindexer

import (
	"fmt"
	"time"
)

var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// processGranularMetrics checks and runs all granular metrics
func (r *IndexRunner) processGranularMetrics() {
	for _, metricFile := range r.granularMetrics {
		for _, granularity := range []string{"hour", "day", "week", "month"} {
			// Use just the metric filename for indexer name, granularity tracked separately
			indexerName := fmt.Sprintf("evm_metrics/%s", metricFile)

			watermark := r.getWatermarkWithGranularity(indexerName, granularity)

			// Initialize to epoch if never run
			if watermark.LastPeriod.IsZero() {
				watermark.LastPeriod = epoch
			}

			// Calculate periods to process
			periods := getPeriodsToProcess(watermark.LastPeriod, r.latestBlockTime, granularity)
			if len(periods) == 0 {
				continue
			}

			// Run metric
			start := time.Now()
			if err := r.runGranularMetric(metricFile, granularity, periods); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to run %s (%s): %v\n", r.chainId, indexerName, granularity, err)
				panic(err)
			}
			elapsed := time.Since(start)
			fmt.Printf("[Chain %d] %s (%s) - processed %d periods - time taken: %s\n",
				r.chainId, indexerName, granularity, len(periods), elapsed)

			// Update watermark
			watermark.LastPeriod = periods[len(periods)-1]
			if err := r.saveWatermarkWithGranularity(indexerName, granularity, watermark); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s (%s): %v\n", r.chainId, indexerName, granularity, err)
				panic(err)
			}
		}
	}
}

// runGranularMetric executes a single granular metric for given periods
func (r *IndexRunner) runGranularMetric(metricFile string, granularity string, periods []time.Time) error {
	firstPeriod := periods[0]
	lastPeriod := nextPeriod(periods[len(periods)-1], granularity) // exclusive end

	// Template parameters (string replacement)
	templateParams := []struct{ key, value string }{
		{"{chain_id}", fmt.Sprintf("%d", r.chainId)},
		{"{granularity}", granularity},
		{"{granularityCamelCase}", capitalize(granularity)},
	}

	// Bind parameters (native ClickHouse parameter binding for WHERE clauses)
	bindParams := map[string]interface{}{
		"chain_id":     r.chainId,
		"first_period": firstPeriod,
		"last_period":  lastPeriod,
	}

	filename := fmt.Sprintf("evm_metrics/%s.sql", metricFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, templateParams, bindParams)
}

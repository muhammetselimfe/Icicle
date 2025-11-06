package indexer

import (
	"fmt"
	"time"
)

var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// processGranularMetrics checks and runs all granular metrics
func (r *IndexRunner) processGranularMetrics() {
	for _, metricFile := range r.granularMetrics {
		for _, granularity := range []string{"hour", "day", "week", "month"} {
			indexerName := fmt.Sprintf("metrics/%s_%s", metricFile, granularity)

			watermark := r.getWatermark(indexerName)
			
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
			fmt.Printf("[Chain %d] Running %s - processing %d periods\n", r.chainId, indexerName, len(periods))
			
			if err := r.runGranularMetric(metricFile, granularity, periods); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}

			// Update watermark
			watermark.LastPeriod = periods[len(periods)-1]
			if err := r.saveWatermark(indexerName, watermark); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}
		}
	}
}

// runGranularMetric executes a single granular metric for given periods
func (r *IndexRunner) runGranularMetric(metricFile string, granularity string, periods []time.Time) error {
	firstPeriod := periods[0]
	lastPeriod := nextPeriod(periods[len(periods)-1], granularity) // exclusive end

	// Order matters! Specific patterns first, then generic
	params := []struct{ key, value string }{
		{"toStartOf{granularity}", fmt.Sprintf("toStartOf%s", capitalize(granularity))},
		{"_{granularity}", fmt.Sprintf("_%s", granularity)},
		{"{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId)},
		{"{first_period:DateTime}", fmt.Sprintf("toDateTime64('%s', 3)", firstPeriod.Format("2006-01-02 15:04:05.000"))},
		{"{last_period:DateTime}", fmt.Sprintf("toDateTime64('%s', 3)", lastPeriod.Format("2006-01-02 15:04:05.000"))},
		{"{granularity}", granularity}, // Last - after specific patterns
	}

	filename := fmt.Sprintf("metrics/%s.sql", metricFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, params)
}


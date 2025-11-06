package indexer

import (
	"fmt"
	"time"
)

// toStartOfPeriod returns the start of the period for given granularity
func toStartOfPeriod(t time.Time, granularity string) time.Time {
	t = t.UTC()
	switch granularity {
	case "hour":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case "week":
		// Start of week (Sunday)
		for t.Weekday() != time.Sunday {
			t = t.AddDate(0, 0, -1)
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	default:
		panic(fmt.Sprintf("unknown granularity: %s", granularity))
	}
}

// getPeriodDuration returns the duration of one period
func getPeriodDuration(granularity string) time.Duration {
	switch granularity {
	case "hour":
		return time.Hour
	case "day":
		return 24 * time.Hour
	case "week":
		return 7 * 24 * time.Hour
	case "month":
		return 30 * 24 * time.Hour // approximation
	default:
		panic(fmt.Sprintf("unknown granularity: %s", granularity))
	}
}

// nextPeriod returns the start of the next period
func nextPeriod(t time.Time, granularity string) time.Time {
	// First, round down to start of current period
	currentPeriod := toStartOfPeriod(t, granularity)

	// Then add one period
	switch granularity {
	case "hour":
		return currentPeriod.Add(time.Hour)
	case "day":
		return currentPeriod.AddDate(0, 0, 1)
	case "week":
		return currentPeriod.AddDate(0, 0, 7)
	case "month":
		return currentPeriod.AddDate(0, 1, 0)
	default:
		panic(fmt.Sprintf("unknown granularity: %s", granularity))
	}
}

// isPeriodComplete checks if a period is complete (we have data from next period)
func isPeriodComplete(periodStart, latestBlockTime time.Time, granularity string) bool {
	periodEnd := nextPeriod(periodStart, granularity)
	return latestBlockTime.After(periodEnd) || latestBlockTime.Equal(periodEnd)
}

// getPeriodsToProcess returns all complete periods to process
func getPeriodsToProcess(lastProcessed, latestBlockTime time.Time, granularity string) []time.Time {
	var periods []time.Time

	// Start point
	var currentPeriod time.Time
	if lastProcessed.IsZero() {
		// Never processed - this shouldn't happen as caller provides earliest block
		return periods
	} else {
		// Start from next period after last processed
		currentPeriod = nextPeriod(lastProcessed, granularity)
	}

	// Collect all complete periods
	for isPeriodComplete(currentPeriod, latestBlockTime, granularity) {
		periods = append(periods, currentPeriod)
		currentPeriod = nextPeriod(currentPeriod, granularity)

		// Safety limit to prevent infinite loops
		if len(periods) > 100000 {
			fmt.Printf("[Periods] Limiting to 100000 periods for %s\n", granularity)
			break
		}
	}

	return periods
}

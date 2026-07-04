package main

import (
	"context"
	"icicle/cmd"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func main() {
	_ = godotenv.Load()

	// Configure slog as the default logger
	// Use JSON in production (LOG_FORMAT=json), text otherwise
	if os.Getenv("LOG_FORMAT") == "json" {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
	} else {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}

	// Ignore SIGPIPE - common in network servers when clients disconnect
	signal.Ignore(syscall.SIGPIPE)

	// Create a cancellable root context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Catch fatal signals and cancel context for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		sig := <-sigChan
		slog.Info("Signal received, initiating graceful shutdown", "signal", sig)
		cancel()
	}()

	root := &cobra.Command{Use: "clickhouse-ingest"}

	wipeCmd := &cobra.Command{
		Use:   "wipe",
		Short: "Drop calculated tables (keeps raw_* and sync_watermark)",
		Run: func(command *cobra.Command, args []string) {
			all, _ := command.Flags().GetBool("all")
			chainID, _ := command.Flags().GetUint32("chain")
			pchain, _ := command.Flags().GetBool("pchain")
			cmd.RunWipe(all, chainID, pchain)
		},
	}
	wipeCmd.Flags().Bool("all", false, "Drop all tables including raw_* tables")
	wipeCmd.Flags().Uint32("chain", 0, "Wipe data for a specific chain ID only")
	wipeCmd.Flags().Bool("pchain", false, "Wipe P-chain calculated tables (validator history, fee stats, subnets)")

	ingestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "Start the continuous ingestion process",
		Run: func(command *cobra.Command, args []string) {
			fast, _ := command.Flags().GetBool("fast")
			cmd.RunIngest(ctx, fast)
		},
	}
	ingestCmd.Flags().Bool("fast", false, "Skip all indexers (incremental and metrics)")

	apiCmd := &cobra.Command{
		Use:   "api",
		Short: "Start the HTTP API server",
		Run: func(command *cobra.Command, args []string) {
			opts := cmd.DefaultAPIOptions()
			opts.Port, _ = command.Flags().GetInt("port")
			opts.RateLimitPerMin, _ = command.Flags().GetInt("rate-limit")
			opts.RateLimitBurst, _ = command.Flags().GetInt("burst")
			trustedProxies, _ := command.Flags().GetString("trusted-proxies")
			opts.TrustedProxies = cmd.ParseCSVFlag(trustedProxies)
			opts.ExemptLoopback, _ = command.Flags().GetBool("exempt-loopback")
			opts.MetricsToken, _ = command.Flags().GetString("metrics-token")
			opts.WSMaxConnections, _ = command.Flags().GetInt("ws-max-connections")
			opts.WSMaxConnectionsPerIP, _ = command.Flags().GetInt("ws-max-connections-per-ip")
			opts.WSMaxConnectionsPerChain, _ = command.Flags().GetInt("ws-max-connections-per-chain")
			cmd.RunAPI(ctx, opts)
		},
	}
	apiCmd.Flags().Int("port", 8080, "Port to listen on")
	apiCmd.Flags().Int("rate-limit", 60, "Rate limit requests per minute per IP")
	apiCmd.Flags().Int("burst", 10, "Rate limit burst size")
	apiCmd.Flags().String("trusted-proxies", "", "Comma-separated trusted proxy IPs/CIDRs for X-Forwarded-For/X-Real-IP")
	apiCmd.Flags().Bool("exempt-loopback", true, "Skip rate limiting for same-box direct callers (loopback peer, no forwarding header)")
	apiCmd.Flags().String("metrics-token", os.Getenv("ICICLE_METRICS_TOKEN"), "Bearer token required to enable and access /metrics")
	apiCmd.Flags().Int("ws-max-connections", 1000, "Maximum concurrent WebSocket connections")
	apiCmd.Flags().Int("ws-max-connections-per-ip", 20, "Maximum concurrent WebSocket connections per client IP")
	apiCmd.Flags().Int("ws-max-connections-per-chain", 250, "Maximum concurrent WebSocket connections per chain")

	lendingCmd := &cobra.Command{
		Use:   "lending",
		Short: "Start the lending liquidation-risk engine (Aave v3 + Benqi)",
		Run: func(command *cobra.Command, args []string) {
			opts := cmd.DefaultLendingOptions()
			cid, _ := command.Flags().GetUint32("chain")
			opts.ChainID = cid
			if v, _ := command.Flags().GetString("archive-rpc"); v != "" {
				opts.ArchiveRPC = v
			}
			if v, _ := command.Flags().GetString("fallback-rpc"); v != "" {
				opts.FallbackRPC = v
			}
			opts.AaveProvider, _ = command.Flags().GetString("aave-provider")
			opts.BenqiComptroller, _ = command.Flags().GetString("benqi-comptroller")
			opts.DiscoveryBatch, _ = command.Flags().GetUint64("discovery-batch")
			opts.ParamsRefreshHours, _ = command.Flags().GetInt("params-refresh-hours")
			opts.MetricsPort, _ = command.Flags().GetInt("metrics-port")
			cmd.RunLending(ctx, opts)
		},
	}
	lendingCmd.Flags().Uint32("chain", 43114, "EVM chain ID to track")
	lendingCmd.Flags().String("archive-rpc", os.Getenv("ICICLE_ARCHIVE_RPC"), "Archive node RPC URL (required)")
	lendingCmd.Flags().String("fallback-rpc", os.Getenv("ICICLE_FALLBACK_RPC"), "Optional public RPC fallback")
	lendingCmd.Flags().String("aave-provider", "", "Aave v3 PoolAddressesProvider (default: canonical Avalanche)")
	lendingCmd.Flags().String("benqi-comptroller", "", "Benqi Comptroller (default: canonical Avalanche)")
	lendingCmd.Flags().Uint64("discovery-batch", 5000, "Blocks per discovery batch")
	lendingCmd.Flags().Int("params-refresh-hours", 6, "Hours between protocol parameter refreshes")
	lendingCmd.Flags().Int("metrics-port", 9092, "Port for the Prometheus /metrics endpoint (0 to disable)")

	kmeasureCmd := &cobra.Command{
		Use:   "kmeasure",
		Short: "Read-only Stage 1 K-measurement: profitable liquidations after costs",
		Run: func(command *cobra.Command, args []string) {
			opts := cmd.DefaultKMeasureOptions()
			cid, _ := command.Flags().GetUint32("chain")
			opts.ChainID = cid
			if v, _ := command.Flags().GetString("feed-url"); v != "" {
				opts.FeedBaseURL = v
			}
			if v, _ := command.Flags().GetString("archive-rpc"); v != "" {
				opts.ArchiveRPC = v
			}
			if v, _ := command.Flags().GetString("fallback-rpc"); v != "" {
				opts.FallbackRPC = v
			}
			opts.IntervalMinutes, _ = command.Flags().GetInt("interval-min")
			opts.GasUnits, _ = command.Flags().GetUint64("gas-units")
			opts.MinProfitUSD, _ = command.Flags().GetFloat64("min-profit-usd")
			opts.FlashFeeBps, _ = command.Flags().GetUint64("flash-fee-bps")
			opts.MinDebtBase, _ = command.Flags().GetString("min-debt-base")
			opts.Persist, _ = command.Flags().GetBool("persist")
			cmd.RunKMeasure(ctx, opts)
		},
	}
	kmeasureCmd.Flags().Uint32("chain", 43114, "EVM chain ID")
	kmeasureCmd.Flags().String("feed-url", "https://api.l1beat.io/api/v1/data/evm/43114/lending", "Lending feed base URL")
	kmeasureCmd.Flags().String("archive-rpc", os.Getenv("ICICLE_ARCHIVE_RPC"), "Archive node RPC URL (required)")
	kmeasureCmd.Flags().String("fallback-rpc", os.Getenv("ICICLE_FALLBACK_RPC"), "Optional public RPC fallback")
	kmeasureCmd.Flags().Int("interval-min", 0, "Repeat every N minutes (0 = one-shot)")
	kmeasureCmd.Flags().Uint64("gas-units", 700000, "Estimated full-bundle gas units")
	kmeasureCmd.Flags().Float64("min-profit-usd", 25, "Minimum net profit in USD to count as profitable")
	kmeasureCmd.Flags().Uint64("flash-fee-bps", 5, "Flash-loan fee in bps, fallback if on-chain read fails")
	kmeasureCmd.Flags().String("min-debt-base", "", "Optional feed-side dust pre-cut, 1e18 USD (default off)")
	kmeasureCmd.Flags().Bool("persist", true, "Persist each run summary to kmeasure_runs")

	stealtimeCmd := &cobra.Command{
		Use:   "stealtime",
		Short: "Offline backtest: how long profitable liquidations sat before being taken",
		Run: func(command *cobra.Command, args []string) {
			opts := cmd.DefaultStealtimeOptions()
			cid, _ := command.Flags().GetUint32("chain")
			opts.ChainID = cid
			if v, _ := command.Flags().GetString("archive-rpc"); v != "" {
				opts.ArchiveRPC = v
			}
			if v, _ := command.Flags().GetString("fallback-rpc"); v != "" {
				opts.FallbackRPC = v
			}
			opts.FromBlock, _ = command.Flags().GetUint64("from-block")
			opts.ToBlock, _ = command.Flags().GetUint64("to-block")
			opts.MaxLookbackBlocks, _ = command.Flags().GetUint64("max-lookback-blocks")
			opts.SampleStride, _ = command.Flags().GetUint64("sample-stride")
			opts.MinDebtUSD, _ = command.Flags().GetFloat64("min-debt-usd")
			opts.MinProfitUSD, _ = command.Flags().GetFloat64("min-profit-usd")
			opts.GasUnits, _ = command.Flags().GetUint64("gas-units")
			opts.TopN, _ = command.Flags().GetInt("top-n")
			opts.Persist, _ = command.Flags().GetBool("persist")
			opts.Debug, _ = command.Flags().GetBool("debug")
			cmd.RunStealtime(ctx, opts)
		},
	}
	stealtimeCmd.Flags().Uint32("chain", 43114, "EVM chain ID")
	stealtimeCmd.Flags().String("archive-rpc", os.Getenv("ICICLE_ARCHIVE_RPC"), "Archive node RPC URL (required)")
	stealtimeCmd.Flags().String("fallback-rpc", os.Getenv("ICICLE_FALLBACK_RPC"), "Optional public RPC fallback")
	stealtimeCmd.Flags().Uint64("from-block", 0, "Start block of the liquidation scan window")
	stealtimeCmd.Flags().Uint64("to-block", 0, "End block of the liquidation scan window")
	stealtimeCmd.Flags().Uint64("max-lookback-blocks", 43200, "Backward search cap; older crossings are right-censored")
	stealtimeCmd.Flags().Uint64("sample-stride", 600, "Coarse backward step before binary-searching the crossing block")
	stealtimeCmd.Flags().Float64("min-debt-usd", 50, "Skip liquidations whose repaid debt is below this USD value (dust pre-filter)")
	stealtimeCmd.Flags().Float64("min-profit-usd", 25, "Minimum net profit in USD to count an opportunity")
	stealtimeCmd.Flags().Uint64("gas-units", 700000, "Estimated full-bundle gas units")
	stealtimeCmd.Flags().Int("top-n", 10, "Top-N liquidators to report for incumbent concentration")
	stealtimeCmd.Flags().Bool("persist", true, "Persist per-liquidation rows to stealtime_results")
	stealtimeCmd.Flags().Bool("debug", false, "Log per-position legs and result for the first evaluated liquidations")

	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "Crash-day capture replay: V2-executable profitability on the days that carry the prize",
		Run: func(command *cobra.Command, args []string) {
			opts := cmd.DefaultReplayOptions()
			cid, _ := command.Flags().GetUint32("chain")
			opts.ChainID = cid
			if v, _ := command.Flags().GetString("archive-rpc"); v != "" {
				opts.ArchiveRPC = v
			}
			if v, _ := command.Flags().GetString("fallback-rpc"); v != "" {
				opts.FallbackRPC = v
			}
			opts.TopDays, _ = command.Flags().GetInt("top-days")
			opts.MinSizeUSD, _ = command.Flags().GetFloat64("min-size-usd")
			opts.GasUnits, _ = command.Flags().GetUint64("gas-units")
			opts.MinProfitUSD, _ = command.Flags().GetFloat64("min-profit-usd")
			opts.Label, _ = command.Flags().GetString("label")
			opts.ProbeVenues, _ = command.Flags().GetBool("probe-venues")
			opts.ProbeBlock, _ = command.Flags().GetUint64("probe-block")
			cmd.RunReplay(ctx, opts)
		},
	}
	replayCmd.Flags().Uint32("chain", 43114, "EVM chain ID")
	replayCmd.Flags().String("archive-rpc", os.Getenv("ICICLE_ARCHIVE_RPC"), "Archive node RPC URL (required)")
	replayCmd.Flags().String("fallback-rpc", os.Getenv("ICICLE_FALLBACK_RPC"), "Optional public RPC fallback")
	replayCmd.Flags().Int("top-days", 12, "Number of crash days to replay (by sized-liquidation count)")
	replayCmd.Flags().Float64("min-size-usd", 1000, "Sized-liquidation threshold by repaid debt")
	replayCmd.Flags().Uint64("gas-units", 700000, "Estimated full-bundle gas units")
	replayCmd.Flags().Float64("min-profit-usd", 5, "Minimum net profit in USD to count as profitable")
	replayCmd.Flags().String("label", "", "Tag for replay_results rows so runs do not overwrite each other (e.g. real_oct)")
	replayCmd.Flags().Bool("probe-venues", false, "Self-test: quote WAVAX/USDC against every venue at --probe-block and exit (verify addresses/encoding live)")
	replayCmd.Flags().Uint64("probe-block", 0, "Block for --probe-venues (0 = recent head)")

	fixturesCmd := &cobra.Command{
		Use:   "fixtures",
		Short: "Extract per-venue fork-test fixtures from replay_results (read-only)",
		Run: func(command *cobra.Command, args []string) {
			opts := cmd.DefaultFixturesOptions()
			cid, _ := command.Flags().GetUint32("chain")
			opts.ChainID = cid
			if v, _ := command.Flags().GetString("archive-rpc"); v != "" {
				opts.ArchiveRPC = v
			}
			if v, _ := command.Flags().GetString("fallback-rpc"); v != "" {
				opts.FallbackRPC = v
			}
			if v, _ := command.Flags().GetString("label"); v != "" {
				opts.Label = v
			}
			opts.Protocol, _ = command.Flags().GetString("protocol")
			opts.MinSteal, _ = command.Flags().GetUint64("min-steal")
			cmd.RunFixtures(ctx, opts)
		},
	}
	fixturesCmd.Flags().Uint32("chain", 43114, "EVM chain ID")
	fixturesCmd.Flags().String("archive-rpc", os.Getenv("ICICLE_ARCHIVE_RPC"), "Archive node RPC URL (required)")
	fixturesCmd.Flags().String("fallback-rpc", os.Getenv("ICICLE_FALLBACK_RPC"), "Optional public RPC fallback")
	fixturesCmd.Flags().String("label", "real_oct", "replay_results label to pick fixtures from")
	fixturesCmd.Flags().String("protocol", "", "Also emit a protocol-scoped fixture preferring a CL route (e.g. benqi)")
	fixturesCmd.Flags().Uint64("min-steal", 1, "Protocol fixture: minimum steal_time (blocks) so the case is fork-replayable")

	validatorBackfillCmd := &cobra.Command{
		Use:   "validator-backfill",
		Short: "Backfill historical validator counts via platform.getValidatorsAt (one-time, idempotent)",
		Run: func(command *cobra.Command, args []string) {
			var opts cmd.ValidatorBackfillOptions
			opts.From, _ = command.Flags().GetString("from")
			opts.To, _ = command.Flags().GetString("to")
			opts.Interval, _ = command.Flags().GetString("interval")
			cmd.RunValidatorBackfill(ctx, opts)
		},
	}
	validatorBackfillCmd.Flags().String("from", "", "Start date YYYY-MM-DD (default 2020-10-01)")
	validatorBackfillCmd.Flags().String("to", "", "End date YYYY-MM-DD (default today)")
	validatorBackfillCmd.Flags().String("interval", "month", "Sample interval: day, week, or month")

	root.AddCommand(
		ingestCmd,
		apiCmd,
		lendingCmd,
		kmeasureCmd,
		stealtimeCmd,
		replayCmd,
		fixturesCmd,
		validatorBackfillCmd,
		&cobra.Command{
			Use:   "cache",
			Short: "Fill RPC cache at max speed (no ClickHouse)",
			Run:   func(command *cobra.Command, args []string) { cmd.RunCache() },
		},
		&cobra.Command{
			Use:   "size",
			Short: "Show ClickHouse table sizes and disk usage",
			Run:   func(command *cobra.Command, args []string) { cmd.RunSize() },
		},
		&cobra.Command{
			Use:   "duplicates",
			Short: "Check for duplicate records in raw tables",
			Run:   func(command *cobra.Command, args []string) { cmd.RunDuplicates() },
		},
		wipeCmd,
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

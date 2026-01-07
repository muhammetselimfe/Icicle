package main

import (
	"icicle/cmd"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func main() {
	_ = godotenv.Load()

	// Catch signals and log them before exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGPIPE)
	go func() {
		sig := <-sigChan
		log.Printf("SIGNAL RECEIVED: %v - shutting down", sig)
		os.Exit(1)
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
			cmd.RunIngest(fast)
		},
	}
	ingestCmd.Flags().Bool("fast", false, "Skip all indexers (incremental and metrics)")

	root.AddCommand(
		ingestCmd,
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

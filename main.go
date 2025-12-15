package main

import (
	"icicle/cmd"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func main() {
	_ = godotenv.Load()

	root := &cobra.Command{Use: "clickhouse-ingest"}

	wipeCmd := &cobra.Command{
		Use:   "wipe",
		Short: "Drop calculated tables (keeps raw_* and sync_watermark)",
		Run: func(command *cobra.Command, args []string) {
			all, _ := command.Flags().GetBool("all")
			chainID, _ := command.Flags().GetUint32("chain")
			cmd.RunWipe(all, chainID)
		},
	}
	wipeCmd.Flags().Bool("all", false, "Drop all tables including raw_* tables")
	wipeCmd.Flags().Uint32("chain", 0, "Wipe data for a specific chain ID only")

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

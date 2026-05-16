package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	cfgFile   string
	logLevel  string
	logFormat string
)

var rootCmd = &cobra.Command{
	Use:   "mull",
	Short: "Lightweight EVM log indexer",
	Long: `mull indexes EVM contract logs into a local SQLite database.

Configure a contract address, RPC endpoint, and event topics in YAML,
then run 'mull index' to stream historical and head logs, resuming
from the last persisted checkpoint on restart.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	pf := rootCmd.PersistentFlags()
	pf.StringVar(&cfgFile, "config", "mull.yaml", "path to config file")
	pf.StringVar(&logLevel, "log-level", "info", "log level (debug|info|warn|error)")
	pf.StringVar(&logFormat, "log-format", "text", "log output format (text|json)")
}

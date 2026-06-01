package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/indexer"
	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Run the EVM log indexer",
	Long: `Reads contract logs from the configured RPC endpoint in chunked
block ranges and writes them to SQLite, resuming from the last
persisted checkpoint. Runs until interrupted (SIGINT/SIGTERM).`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runIndex(ctx)
	},
}

func init() {
	rootCmd.AddCommand(indexCmd)
}

func runIndex(ctx context.Context) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return err
	}
	logger, err := newLogger(logLevel, logFormat)
	if err != nil {
		return err
	}

	st, err := store.OpenSQLite(ctx, cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	idx := indexer.New(rpc.NewHTTPClient(cfg.RPCURL, nil, rpc.RetryPolicy{
		Base:        cfg.RPCRetryBase,
		MaxDelay:    cfg.RPCRetryMaxDelay,
		MaxAttempts: cfg.RPCRetryMaxAttempts,
	}), st, indexer.Options{
		Contract:     cfg.Contract,
		Topics:       cfg.Topics,
		ChunkSize:    cfg.ChunkSize,
		PollInterval: cfg.PollInterval,
		StartBlock:   cfg.StartBlock,
		Concurrency:  cfg.Concurrency,
		ReorgDepth:   cfg.ReorgDepth,
		Logger:       logger,
	})
	return idx.Run(ctx)
}

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/gen"
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

	if err := gen.ApplySchema(ctx, st.DB()); err != nil {
		return fmt.Errorf("apply generated schema: %w", err)
	}
	sinks := gen.NewSinks(st.DB())

	httpClient := rpc.NewHTTPClient(cfg.RPCURL, nil, rpc.RetryPolicy{
		Base:        cfg.RPCRetryBase,
		MaxDelay:    cfg.RPCRetryMaxDelay,
		MaxAttempts: cfg.RPCRetryMaxAttempts,
	})

	headSource := buildHeadSource(cfg, httpClient, logger)

	idx := indexer.New(httpClient, st, indexer.Options{
		Contract:     cfg.Contract,
		Topics:       cfg.Topics,
		ChunkSize:    cfg.ChunkSize,
		PollInterval: cfg.PollInterval,
		StartBlock:   cfg.StartBlock,
		Concurrency:  cfg.Concurrency,
		ReorgDepth:   cfg.ReorgDepth,
		Logger:       logger,
		Sinks:        sinks,
		HeadSource:   headSource,
	})
	return idx.Run(ctx)
}

// buildHeadSource picks the HeadSource implementation per cfg.HeadSource:
//   - "poll": polling only (the pre-WSS behaviour)
//   - "ws":   WS source backed by the polling fallback for Latest + demotion
//   - "auto": WS when ws_rpc_url is set, otherwise poll
//
// Validation in config.Load guarantees that "ws" implies WSRPCURL != "".
func buildHeadSource(cfg *config.Config, client *rpc.HTTPClient, logger *slog.Logger) indexer.HeadSource {
	polling := &indexer.PollingHeadSource{Client: client, PollInterval: cfg.PollInterval}
	switch cfg.HeadSource {
	case "poll":
		return polling
	case "ws":
		return rpc.NewWebSocketHeadSource(cfg.WSRPCURL, polling, rpc.WSOptions{
			FallbackAfter: cfg.HeadSourceFallbackAfter,
			Logger:        logger,
		})
	default: // "auto"
		if cfg.WSRPCURL == "" {
			return polling
		}
		return rpc.NewWebSocketHeadSource(cfg.WSRPCURL, polling, rpc.WSOptions{
			FallbackAfter: cfg.HeadSourceFallbackAfter,
			Logger:        logger,
		})
	}
}

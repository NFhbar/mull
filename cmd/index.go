package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/gen"
	"github.com/NFhbar/mull/internal/indexer"
	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Run the EVM log indexer",
	Long: `Reads contract logs from the configured RPC endpoints in chunked
block ranges and writes them to SQLite, resuming from the last
persisted checkpoint per source. Each entry under sources: in the
config spins up its own indexer goroutine; all run under one
errgroup so a SIGINT/SIGTERM (or any source's hard error) gracefully
stops every source.`,
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
		return translateMigrationSentinel(err, cfg.DBPath)
	}
	defer st.Close()

	if err := gen.ApplySchema(ctx, st.DB(), logger); err != nil {
		return fmt.Errorf("apply generated schema: %w", err)
	}
	// Typed-event tables are global to the binary; sinks themselves are
	// stateless. Every source dispatches into the same sink set — the
	// generated INSERT binds source onto the row, so cross-source rows don't
	// collide on (source, tx_hash, log_index).
	sinks := gen.NewSinks(st.DB())

	g, gctx := errgroup.WithContext(ctx)
	for i := range cfg.Sources {
		src := cfg.Sources[i]
		idx, err := newSourceIndexer(cfg, src, st, sinks, logger)
		if err != nil {
			return fmt.Errorf("source %q: %w", src.Name, err)
		}
		g.Go(func() error {
			err := idx.Run(gctx)
			logger.Info("indexer source stopped", "source", src.Name, "err", err)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

// newSourceIndexer wires one Source's RPC client + head source into a fresh
// Indexer. Each source gets its own RPC client because their endpoint URLs
// and retry policies (well, the head_source_fallback_after for WS) are
// per-source. The global retry policy + concurrency come from cfg.
func newSourceIndexer(cfg *config.Config, src config.Source, st store.Store, sinks []store.EventSink, logger *slog.Logger) (*indexer.Indexer, error) {
	httpClient := rpc.NewHTTPClient(src.RPCURL, nil, rpc.RetryPolicy{
		Base:        cfg.RPCRetryBase,
		MaxDelay:    cfg.RPCRetryMaxDelay,
		MaxAttempts: cfg.RPCRetryMaxAttempts,
	})
	headSource := buildHeadSource(cfg, &src, httpClient, logger)
	return indexer.New(httpClient, st, indexer.Options{
		Source:       src.Name,
		Contract:     src.Contract,
		Topics:       src.Topics,
		ChunkSize:    src.ChunkSize,
		PollInterval: cfg.PollInterval,
		StartBlock:   src.StartBlock,
		Concurrency:  cfg.Concurrency,
		ReorgDepth:   cfg.ReorgDepth,
		Logger:       logger,
		Sinks:        sinks,
		HeadSource:   headSource,
	}), nil
}

// buildHeadSource picks the HeadSource implementation per source.HeadSource:
//   - "poll": polling only (the pre-WSS behaviour)
//   - "ws":   WS source backed by the polling fallback for Latest + demotion
//   - "auto": WS when ws_rpc_url is set, otherwise poll
//
// Validation in config.Load guarantees that head_source: ws implies
// ws_rpc_url != "" at the per-source level.
func buildHeadSource(cfg *config.Config, src *config.Source, client *rpc.HTTPClient, logger *slog.Logger) indexer.HeadSource {
	polling := &indexer.PollingHeadSource{Client: client, PollInterval: cfg.PollInterval}
	switch src.HeadSource {
	case "poll":
		return polling
	case "ws":
		return rpc.NewWebSocketHeadSource(src.WSRPCURL, polling, rpc.WSOptions{
			FallbackAfter: src.HeadSourceFallbackAfter,
			Logger:        logger,
		})
	default: // "auto"
		if src.WSRPCURL == "" {
			return polling
		}
		return rpc.NewWebSocketHeadSource(src.WSRPCURL, polling, rpc.WSOptions{
			FallbackAfter: src.HeadSourceFallbackAfter,
			Logger:        logger,
		})
	}
}

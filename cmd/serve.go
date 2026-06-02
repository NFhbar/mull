package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/serve"
	"github.com/NFhbar/mull/internal/store"
)

var serveAddr string

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve indexed events over HTTP/JSON",
	Long: `Starts a read-only HTTP server exposing the indexed events table.

Routes:
  GET /healthz     liveness probe
  GET /checkpoint  current indexer checkpoint (next block to index)
  GET /events      paginated query with contract / topic / block-range filters

The server runs against the same SQLite database as 'mull index'. WAL mode
is enabled at open time, so the two can run in the same process or as
separate processes pointing at the same db_path.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runServe(ctx)
	},
}

func init() {
	serveCmd.Flags().StringVar(&serveAddr, "addr", ":8080", "address to bind the HTTP server")
	rootCmd.AddCommand(serveCmd)
}

func runServe(ctx context.Context) error {
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

	srv := serve.NewServer(st, logger)

	// Timeouts: ReadHeaderTimeout mitigates gosec G114 (Slowloris).
	// WriteTimeout bounds slow-client paginated reads. IdleTimeout keeps
	// keep-alive connections from accumulating.
	httpSrv := &http.Server{
		Addr:              serveAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Intentional exception to the repo-wide errgroup-only goroutine policy
	// (see repo-knowledge): http.Server.Shutdown must be invoked from a goroutine
	// that does NOT own the ListenAndServe call, and the natural shape — block on
	// ctx.Done in the caller, run ListenAndServe in a goroutine, then call
	// Shutdown after ctx.Done — composes more cleanly as raw goroutine + select
	// than as two errgroup members where the "wait for ctx, then Shutdown" leg
	// would need its own context branch.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("mull serve listening", "addr", serveAddr)
		err := httpSrv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

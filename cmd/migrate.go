package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/store"

	_ "modernc.org/sqlite"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate the SQLite database to the latest schema version",
	Long: `Rewrites a v1-shaped mull database in place to the v2 multi-source shape.

The migration runs inside a single transaction so a failure mid-sequence
rolls back to the original on-disk state. All existing rows are stamped with
source = "default" — matching the legacy-config shim that wraps a
single-source mull.yaml as a synthetic "default" source.

Idempotent: running on a database already at v2 is a no-op.

After running, mull index and mull serve will accept the database.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMigrate(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}

func runMigrate(ctx context.Context) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return err
	}
	// Open the raw sql.DB directly — OpenSQLite's start-gate would refuse a v1
	// DB with ErrDBNeedsMigration, but that's exactly the case we want to
	// migrate. Skip the gate, run the migration, then close.
	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	// WAL mode for parity with OpenSQLite — keeps the migration consistent with
	// the runtime journal mode the rest of mull expects. Assert the resulting
	// mode so a silent fallback (read-only FS, non-WAL-capable medium) surfaces
	// here at migrate time rather than confusingly at the next OpenSQLite.
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode); err != nil {
		return fmt.Errorf("enable wal: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf("enable wal: journal_mode=%q after PRAGMA, want %q", mode, "wal")
	}

	if err := store.MigrateV1ToV2(ctx, db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	fmt.Fprintf(os.Stdout, "migrated %s → schema v%d\n", cfg.DBPath, version)
	return nil
}

// translateMigrationSentinel is used by cmd/index + cmd/serve to render
// ErrDBNeedsMigration as an actionable user message rather than a raw error.
func translateMigrationSentinel(err error, dbPath string) error {
	if errors.Is(err, store.ErrDBNeedsMigration) {
		return fmt.Errorf("database %q is on schema v1; run 'mull migrate --config %s' to upgrade to v%d",
			dbPath, cfgFile, store.SchemaVersion)
	}
	return err
}

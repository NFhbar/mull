package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/gen"
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

After the v1→v2 step, any generated typed-event table whose on-disk shape
drifted from the committed codegen output (or that was dropped by hand) is
rebuilt: dropped, recreated from the fresh DDL, restamped in
gen_schema_versions, and repopulated by replaying matching rows from the
raw events table — one transaction per table.

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
	// Close the raw handle before reopening through the gate — the DB is v2
	// by now, so OpenSQLite passes and the rebuild inherits WAL + the
	// busy_timeout DSN pragma on every pooled connection.
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}
	st, err := store.OpenSQLite(ctx, cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()
	rebuilt, err := gen.RebuildDrifted(ctx, st)
	if err != nil {
		// Rebuilds commit per table, so earlier tables are already durable;
		// tell the operator so a re-run's smaller rebuild list isn't a surprise.
		if len(rebuilt) > 0 {
			fmt.Fprintf(os.Stderr, "note: %d typed table(s) already rebuilt and committed before the failure: %s; a re-run will rebuild only the remainder\n",
				len(rebuilt), strings.Join(rebuilt, ", "))
		}
		return fmt.Errorf("rebuild drifted tables: %w", err)
	}

	if len(rebuilt) > 0 {
		fmt.Fprintf(os.Stdout, "migrated %s → schema v%d; rebuilt %d drifted typed table(s): %s\n",
			cfg.DBPath, version, len(rebuilt), strings.Join(rebuilt, ", "))
	} else {
		fmt.Fprintf(os.Stdout, "migrated %s → schema v%d\n", cfg.DBPath, version)
	}
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

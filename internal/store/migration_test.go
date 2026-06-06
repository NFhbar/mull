package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// openRawV1DB writes a v1-shaped SQLite database to a temp path, seeds it with
// a representative row in each table, and returns the open handle + path.
// Tests use this as the input to migration paths so we exercise the actual
// `CREATE TABLE … SELECT FROM …` rewrite, not a synthetic mock.
func openRawV1DB(t *testing.T, seed bool) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open v1: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	// Enable WAL so the post-migration OpenSQLite handshake doesn't re-mode.
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("wal: %v", err)
	}
	const v1Schema = `
        CREATE TABLE events (
            block_number INTEGER NOT NULL,
            tx_hash      TEXT    NOT NULL,
            log_index    INTEGER NOT NULL,
            address      TEXT    NOT NULL,
            topics       TEXT    NOT NULL,
            data         TEXT    NOT NULL,
            PRIMARY KEY (tx_hash, log_index)
        );
        CREATE INDEX idx_events_block ON events(block_number);
        CREATE TABLE checkpoint (
            id           INTEGER PRIMARY KEY CHECK (id = 1),
            block_number INTEGER NOT NULL
        );
        CREATE TABLE block_hashes (
            block_number INTEGER PRIMARY KEY,
            hash         TEXT    NOT NULL,
            parent_hash  TEXT    NOT NULL
        );
    `
	if _, err := db.ExecContext(ctx, v1Schema); err != nil {
		t.Fatalf("v1 schema: %v", err)
	}
	if seed {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO events (block_number, tx_hash, log_index, address, topics, data) VALUES (1, '0xtx', 0, '0xc', '0xt', '0x')`); err != nil {
			t.Fatalf("seed event: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO checkpoint (id, block_number) VALUES (1, 42)`); err != nil {
			t.Fatalf("seed checkpoint: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO block_hashes (block_number, hash, parent_hash) VALUES (5, '0xh5', '0xh4')`); err != nil {
			t.Fatalf("seed block hash: %v", err)
		}
	}
	return db, path
}

func TestMigrateV1ToV2GoldenPath(t *testing.T) {
	db, _ := openRawV1DB(t, true)
	ctx := context.Background()

	if err := MigrateV1ToV2(ctx, db); err != nil {
		t.Fatalf("MigrateV1ToV2: %v", err)
	}

	// Shape assertions: each table has a source column with the default value.
	var src string
	if err := db.QueryRowContext(ctx, `SELECT source FROM events`).Scan(&src); err != nil {
		t.Fatalf("read events source: %v", err)
	}
	if src != DefaultSourceName {
		t.Fatalf("events.source = %q, want %q", src, DefaultSourceName)
	}
	var cpBlock uint64
	if err := db.QueryRowContext(ctx, `SELECT block_number FROM checkpoint WHERE source = ?`, DefaultSourceName).Scan(&cpBlock); err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if cpBlock != 42 {
		t.Fatalf("checkpoint block = %d, want 42", cpBlock)
	}
	var bhSrc string
	if err := db.QueryRowContext(ctx, `SELECT source FROM block_hashes`).Scan(&bhSrc); err != nil {
		t.Fatalf("read block hash source: %v", err)
	}
	if bhSrc != DefaultSourceName {
		t.Fatalf("block_hashes.source = %q, want %q", bhSrc, DefaultSourceName)
	}
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if version != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, SchemaVersion)
	}

	// New index present.
	var idxCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_events_source_block'`,
	).Scan(&idxCount); err != nil {
		t.Fatalf("count idx: %v", err)
	}
	if idxCount != 1 {
		t.Fatalf("idx_events_source_block missing")
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db, _ := openRawV1DB(t, true)
	ctx := context.Background()

	if err := MigrateV1ToV2(ctx, db); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	// Second call is a no-op (user_version already at SchemaVersion).
	if err := MigrateV1ToV2(ctx, db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	// Row count unchanged.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("event count = %d, want 1 (idempotent)", n)
	}
}

func TestMigrateRejectsAlreadyV2(t *testing.T) {
	// Build a fresh v2 DB via OpenSQLite, then call the migration: must be a no-op.
	path := filepath.Join(t.TempDir(), "v2.db")
	s, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatalf("open v2: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := MigrateV1ToV2(context.Background(), s.DB()); err != nil {
		t.Fatalf("migrate on v2: %v, want nil (no-op)", err)
	}
}

// TestMigrateRollsBackOnError drives the migration statements through a raw
// *sql.Tx and injects a typed-column failure mid-sequence — proves SQLite's
// atomic-commit contract leaves the v1 tables and user_version=0 untouched.
//
// Approach: factor the migration into migrationStatementsV1ToV2() (public to
// the package), then run the same statements in a test-owned transaction with
// one deliberate bad insert appended. Tx.Rollback() must restore the original
// state.
func TestMigrateRollsBackOnError(t *testing.T) {
	db, _ := openRawV1DB(t, true)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Run the first few migration statements to mutate state inside the tx.
	stmts := migrationStatementsV1ToV2()
	for _, s := range stmts[:5] {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			t.Fatalf("setup stmt: %v\nsql=%s", err, s)
		}
	}
	// Inject a deliberate failure: try to insert a non-integer into
	// checkpoint.block_number (INTEGER NOT NULL). SQLite type affinity will
	// accept the string but we use a constraint-violating row instead — drop
	// a NULL into the NOT NULL column to force a hard error.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO checkpoint_v2 (source, block_number) VALUES ('default', NULL)`,
	); err == nil {
		// If for some reason the table doesn't exist yet (statement ordering
		// changed), try a direct constraint violation against the v1 events table.
		if _, err2 := tx.ExecContext(ctx, `INSERT INTO events DEFAULT VALUES`); err2 == nil {
			t.Fatal("expected an error from the injected statement, got nil")
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Assertion (a): v1 tables still present, with the v1-shape checkpoint PK.
	var cols int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('checkpoint') WHERE name = 'id'`,
	).Scan(&cols); err != nil {
		t.Fatalf("pragma table_info: %v", err)
	}
	if cols != 1 {
		t.Fatalf("checkpoint.id column missing post-rollback — table state mutated despite rollback")
	}
	// Assertion (b): events table has NO source column (still v1 shape).
	var srcCols int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('events') WHERE name = 'source'`,
	).Scan(&srcCols); err != nil {
		t.Fatalf("pragma table_info events: %v", err)
	}
	if srcCols != 0 {
		t.Fatalf("events.source present post-rollback — table state mutated despite rollback")
	}
	// Assertion (c): user_version still 0 (PRAGMA writes are not transactional
	// in SQLite, so this asserts we did NOT stamp it inside the tx before the
	// failure point).
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if version != 0 {
		t.Fatalf("user_version = %d, want 0 (rollback should leave it untouched)", version)
	}
}

// TestOpenSQLite_OnV1DBReturnsSentinel pins the start-gate: re-opening a
// v1-shaped DB through OpenSQLite surfaces ErrDBNeedsMigration (the actionable
// signal cmd/index + cmd/serve translate into a user-facing message).
func TestOpenSQLite_OnV1DBReturnsSentinel(t *testing.T) {
	_, path := openRawV1DB(t, true)
	_, err := OpenSQLite(context.Background(), path)
	if err == nil {
		t.Fatal("expected ErrDBNeedsMigration, got nil")
	}
	if err != ErrDBNeedsMigration {
		t.Fatalf("err = %v, want ErrDBNeedsMigration", err)
	}
}

// TestOpenSQLite_OnFreshDBStampsVersion pins that a brand-new DB created
// through OpenSQLite immediately has user_version = SchemaVersion, so the
// next open's start-gate flows past the migration check without re-prompting.
func TestOpenSQLite_OnFreshDBStampsVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	s, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	var v int
	if err := s.db.QueryRowContext(context.Background(), `PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if v != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", v, SchemaVersion)
	}
}

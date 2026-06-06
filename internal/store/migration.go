package store

import (
	"context"
	"database/sql"
	"fmt"
)

// DefaultSourceName is the source name assigned to all rows migrated from a
// v1 database. Operators upgrading a single-source mull.db end up with one
// `default` source after `mull migrate`, matching the legacy-shim default
// applied by config.Load when the YAML still uses the v1 single-source shape.
const DefaultSourceName = "default"

// migrationStatementsV1ToV2 returns the ordered SQL statements that rewrite a
// v1 database into the v2 shape. Exposed as a helper (rather than inlined into
// MigrateV1ToV2) so the rollback test can drive the same statements through a
// raw *sql.Tx and inject a deliberate failure mid-sequence — proving that the
// transactional contract leaves the on-disk DB unchanged on rollback.
func migrationStatementsV1ToV2() []string {
	return []string{
		// Events: rebuild with source column + (source, tx_hash, log_index) PK.
		`CREATE TABLE events_v2 (
            source       TEXT    NOT NULL,
            block_number INTEGER NOT NULL,
            tx_hash      TEXT    NOT NULL,
            log_index    INTEGER NOT NULL,
            address      TEXT    NOT NULL,
            topics       TEXT    NOT NULL,
            data         TEXT    NOT NULL,
            PRIMARY KEY (source, tx_hash, log_index)
        )`,
		`INSERT INTO events_v2 (source, block_number, tx_hash, log_index, address, topics, data)
         SELECT 'default', block_number, tx_hash, log_index, address, topics, data FROM events`,
		`DROP INDEX IF EXISTS idx_events_block`,
		`DROP TABLE events`,
		`ALTER TABLE events_v2 RENAME TO events`,
		`CREATE INDEX idx_events_source_block ON events(source, block_number)`,

		// Checkpoint: rebuild with source PK.
		`CREATE TABLE checkpoint_v2 (
            source       TEXT PRIMARY KEY,
            block_number INTEGER NOT NULL
        )`,
		`INSERT INTO checkpoint_v2 (source, block_number)
         SELECT 'default', block_number FROM checkpoint WHERE id = 1`,
		`DROP TABLE checkpoint`,
		`ALTER TABLE checkpoint_v2 RENAME TO checkpoint`,

		// Block hashes: rebuild with (source, block_number) PK.
		`CREATE TABLE block_hashes_v2 (
            source       TEXT    NOT NULL,
            block_number INTEGER NOT NULL,
            hash         TEXT    NOT NULL,
            parent_hash  TEXT    NOT NULL,
            PRIMARY KEY (source, block_number)
        )`,
		`INSERT INTO block_hashes_v2 (source, block_number, hash, parent_hash)
         SELECT 'default', block_number, hash, parent_hash FROM block_hashes`,
		`DROP TABLE block_hashes`,
		`ALTER TABLE block_hashes_v2 RENAME TO block_hashes`,

		// Stamp user_version so the start-gate accepts the DB on next open.
		fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion),
	}
}

// MigrateV1ToV2 rewrites a v1-shaped mull database in place to the v2 shape.
//
// Behaviour:
//   - Idempotent: a database already at user_version >= SchemaVersion is a
//     no-op, returns nil.
//   - Fresh databases (no tables) are out of scope — those go through
//     OpenSQLite + migrate() which builds the v2 shape directly.
//   - All schema rewrites happen inside one BEGIN IMMEDIATE … COMMIT
//     transaction. A failure mid-sequence rolls back automatically; the
//     on-disk file is unchanged.
//
// All migrated rows are stamped with source = DefaultSourceName ("default"),
// matching the legacy-config shim in internal/config.
func MigrateV1ToV2(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	if version >= SchemaVersion {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, stmt := range migrationStatementsV1ToV2() {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate v1→v2: %w (statement: %q)", err, firstLine(stmt))
		}
	}
	return tx.Commit()
}

// firstLine returns the first non-blank line of a multi-line SQL statement,
// trimmed — keeps error messages readable when a CREATE TABLE block fails.
func firstLine(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			return trimAround(s[:i])
		}
	}
	return trimAround(s)
}

func trimAround(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

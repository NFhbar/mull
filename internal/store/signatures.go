package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// EnsureSchemaSignatures verifies that each generated typed-event table's
// on-disk column shape matches the signature codegen computed for it, and
// stamps the signature into the gen_schema_versions meta table.
//
// Behaviour per table in sigs (iterated in sorted order so failures are
// deterministic):
//   - The PRAGMA-derived shape is the ground truth. A mismatch against the
//     expected signature returns a loud error naming the table and both
//     signatures — this covers both a stamped row that disagrees and a
//     pre-stamping table whose shape already drifted.
//   - On match the signature is upserted, which stamps first-creates,
//     backfills pre-existing unstamped tables (no spurious failures for
//     deployments that predate stamping), and self-repairs a stale stamp.
//
// Stamped tables absent from sigs (event removed from the ABI) are returned
// as orphans for the caller to warn about — never deleted; regeneration must
// not destroy user data.
//
// createGenSchemaVersionsDDL and upsertGenSchemaVersionSQL are shared with
// the rebuild engine (rebuild.go) so the meta table's shape and stamping
// semantics can't diverge between the two sites.
const createGenSchemaVersionsDDL = `CREATE TABLE IF NOT EXISTS gen_schema_versions (
        table_name       TEXT PRIMARY KEY,
        column_signature TEXT NOT NULL
    )`

const upsertGenSchemaVersionSQL = `INSERT INTO gen_schema_versions (table_name, column_signature) VALUES (?, ?)
        ON CONFLICT(table_name) DO UPDATE SET column_signature = excluded.column_signature`

// Like MigrateV1ToV2 this is a package-level helper on *sql.DB rather than a
// Store method: the caller (generated ApplySchema) already holds the handle.
func EnsureSchemaSignatures(ctx context.Context, db *sql.DB, sigs map[string]string) ([]string, error) {
	if _, err := db.ExecContext(ctx, createGenSchemaVersionsDDL); err != nil {
		return nil, fmt.Errorf("create gen_schema_versions: %w", err)
	}

	tables := make([]string, 0, len(sigs))
	for t := range sigs {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	for _, table := range tables {
		actual, err := tableColumnSignature(ctx, db, table)
		if err != nil {
			return nil, err
		}
		expected := sigs[table]
		if actual != expected {
			return nil, fmt.Errorf("codegen: %s on disk has columns [%s], regenerated codegen expects [%s]; run `mull migrate` to rebuild it from the raw events table, or drop it manually (see README \"Codegen Caveats\" § Schema regeneration)", table, actual, expected)
		}
		if _, err := db.ExecContext(ctx, upsertGenSchemaVersionSQL, table, expected); err != nil {
			return nil, fmt.Errorf("stamp %s: %w", table, err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT table_name FROM gen_schema_versions ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("scan gen_schema_versions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var orphans []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan gen_schema_versions row: %w", err)
		}
		if _, ok := sigs[t]; !ok {
			orphans = append(orphans, t)
		}
	}
	return orphans, rows.Err()
}

// tableColumnSignature derives the table's "colname:SQLTYPE" signature from
// pragma_table_info in declared column order. The table-valued pragma form is
// used so the identifier binds as a parameter instead of being interpolated
// into SQL text.
func tableColumnSignature(ctx context.Context, db *sql.DB, table string) (string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name, type FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		return "", fmt.Errorf("table_info %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	var parts []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return "", fmt.Errorf("table_info %s row: %w", table, err)
		}
		parts = append(parts, name+":"+typ)
	}
	return strings.Join(parts, ","), rows.Err()
}

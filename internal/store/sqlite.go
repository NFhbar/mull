package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// SchemaVersion is the user_version PRAGMA value the v2 schema sets. v1 (the
// pre-multi-source shape) had user_version=0 (the SQLite default — mull never
// set it). Bumping this is how the start-gate distinguishes pre- and
// post-migration databases.
const SchemaVersion = 2

type SQLite struct {
	db *sql.DB
}

func OpenSQLite(ctx context.Context, path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// WAL admits a concurrent reader (e.g. `mull serve`) alongside the
	// indexer's writer without serializing them. PRAGMA returns the resulting
	// journal mode; assert it equals "wal" so a silent fallback (e.g. on a
	// read-only filesystem) surfaces as an error instead of slipping through.
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if mode != "wal" {
		_ = db.Close()
		return nil, fmt.Errorf("enable wal: journal_mode=%q after PRAGMA, want %q", mode, "wal")
	}

	// Start-gate: a v1-shaped database (tables exist, user_version < 2) is not
	// safe to open as v2. Surface ErrDBNeedsMigration so cmd/index + cmd/serve
	// can print an actionable message pointing the operator at `mull migrate`.
	// Fresh databases (no tables) fall through to migrate() which creates the
	// v2 shape and stamps user_version directly.
	hasV1, err := hasV1Tables(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("inspect schema: %w", err)
	}
	if hasV1 {
		var version int
		if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("read user_version: %w", err)
		}
		if version < SchemaVersion {
			_ = db.Close()
			return nil, ErrDBNeedsMigration
		}
	}

	s := &SQLite{db: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// hasV1Tables reports whether the database file already contains any of the
// three core tables. Used by the start-gate to distinguish "fresh DB" (create
// v2-shaped tables in migrate()) from "pre-existing v1 DB" (require migrate).
func hasV1Tables(ctx context.Context, db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('events','checkpoint','block_hashes')`,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLite) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS events (
    source       TEXT    NOT NULL,
    block_number INTEGER NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    address      TEXT    NOT NULL,
    topics       TEXT    NOT NULL,
    data         TEXT    NOT NULL,
    PRIMARY KEY (source, tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_events_source_block ON events(source, block_number);

CREATE TABLE IF NOT EXISTS checkpoint (
    source       TEXT PRIMARY KEY,
    block_number INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS block_hashes (
    source       TEXT    NOT NULL,
    block_number INTEGER NOT NULL,
    hash         TEXT    NOT NULL,
    parent_hash  TEXT    NOT NULL,
    PRIMARY KEY (source, block_number)
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion)); err != nil {
		return fmt.Errorf("stamp user_version: %w", err)
	}
	return nil
}

func (s *SQLite) SaveEvents(ctx context.Context, source string, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO events
		(source, block_number, tx_hash, log_index, address, topics, data)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, e := range events {
		topics := strings.Join(e.Topics, ",")
		if _, err := stmt.ExecContext(ctx, source, e.BlockNumber, e.TxHash, e.LogIndex, e.Address, topics, e.Data); err != nil {
			return fmt.Errorf("insert event %s/%d: %w", e.TxHash, e.LogIndex, err)
		}
	}
	return tx.Commit()
}

func (s *SQLite) Checkpoint(ctx context.Context, source string) (uint64, error) {
	var block uint64
	err := s.db.QueryRowContext(ctx, `SELECT block_number FROM checkpoint WHERE source = ?`, source).Scan(&block)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}
	return block, nil
}

func (s *SQLite) SetCheckpoint(ctx context.Context, source string, block uint64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO checkpoint (source, block_number) VALUES (?, ?)
		ON CONFLICT(source) DO UPDATE SET block_number = excluded.block_number`, source, block)
	if err != nil {
		return fmt.Errorf("set checkpoint: %w", err)
	}
	return nil
}

func (s *SQLite) Checkpoints(ctx context.Context) (map[string]uint64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source, block_number FROM checkpoint`)
	if err != nil {
		return nil, fmt.Errorf("query checkpoints: %w", err)
	}
	defer rows.Close()
	out := make(map[string]uint64)
	for rows.Next() {
		var src string
		var block uint64
		if err := rows.Scan(&src, &block); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		out[src] = block
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}
	return out, nil
}

func (s *SQLite) RecordBlockHash(ctx context.Context, source string, number uint64, hash, parentHash string, capDepth uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `INSERT INTO block_hashes (source, block_number, hash, parent_hash)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(source, block_number) DO UPDATE SET hash = excluded.hash, parent_hash = excluded.parent_hash`,
		source, number, hash, parentHash); err != nil {
		return fmt.Errorf("upsert block hash %d: %w", number, err)
	}
	if capDepth > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM block_hashes
			WHERE source = ? AND block_number <= (SELECT MAX(block_number) FROM block_hashes WHERE source = ?) - ?`,
			source, source, capDepth); err != nil {
			return fmt.Errorf("trim block hashes: %w", err)
		}
	}
	return tx.Commit()
}

func (s *SQLite) RecentBlockHashes(ctx context.Context, source string, limit uint64) ([]BlockHashEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT block_number, hash, parent_hash FROM block_hashes
		WHERE source = ? ORDER BY block_number DESC LIMIT ?`, source, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent block hashes: %w", err)
	}
	defer rows.Close()
	out := make([]BlockHashEntry, 0)
	for rows.Next() {
		var e BlockHashEntry
		if err := rows.Scan(&e.Number, &e.Hash, &e.ParentHash); err != nil {
			return nil, fmt.Errorf("scan block hash: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate block hashes: %w", err)
	}
	return out, nil
}

func (s *SQLite) BlockHashAt(ctx context.Context, source string, number uint64) (BlockHashEntry, bool, error) {
	var e BlockHashEntry
	err := s.db.QueryRowContext(ctx, `SELECT block_number, hash, parent_hash FROM block_hashes
		WHERE source = ? AND block_number = ?`, source, number).Scan(&e.Number, &e.Hash, &e.ParentHash)
	if errors.Is(err, sql.ErrNoRows) {
		return BlockHashEntry{}, false, nil
	}
	if err != nil {
		return BlockHashEntry{}, false, fmt.Errorf("read block hash %d: %w", number, err)
	}
	return e, true, nil
}

func (s *SQLite) RewindTo(ctx context.Context, source string, block uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE source = ? AND block_number >= ?`, source, block); err != nil {
		return fmt.Errorf("rewind events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM block_hashes WHERE source = ? AND block_number >= ?`, source, block); err != nil {
		return fmt.Errorf("rewind block hashes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint (source, block_number) VALUES (?, ?)
		ON CONFLICT(source) DO UPDATE SET block_number = excluded.block_number`, source, block); err != nil {
		return fmt.Errorf("rewind checkpoint: %w", err)
	}
	return tx.Commit()
}

// Query implements Store.Query against the events table.
//
// SQL pushdown covers Source, Contract, FromBlock, ToBlock, Topic0, and cursor
// position; higher topics (Topic1..Topic3) are post-filtered in Go because the
// table stores topics as a comma-joined string and a position-aware LIKE chain
// over multiple positions would be both fragile and expensive.
//
// Cursor strategy: fetch limit+1 rows so the next cursor can be derived
// without a second query. The cursor is uniquely positioned on
// (block_number, log_index, source) — adding source to the tuple is the
// multi-source disambiguator. A legacy cursor decoded with empty Source
// (from the v1 wire format) sorts strictly before any real source, so paging
// resumes from a deterministic boundary; one event at the boundary may
// re-emit (documented in MIGRATION.md).
func (s *SQLite) Query(ctx context.Context, filter QueryFilter) ([]Event, *EventCursor, error) {
	const (
		defaultLimit = 100
		maxLimit     = 1000
	)
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	var (
		where []string
		args  []any
	)
	if filter.Source != nil {
		where = append(where, "source = ?")
		args = append(args, *filter.Source)
	}
	if filter.Contract != "" {
		where = append(where, "address = ?")
		args = append(args, filter.Contract)
	}
	if filter.FromBlock != nil {
		where = append(where, "block_number >= ?")
		args = append(args, *filter.FromBlock)
	}
	if filter.ToBlock != nil {
		where = append(where, "block_number <= ?")
		args = append(args, *filter.ToBlock)
	}
	if filter.Topic0 != nil {
		// topics is a comma-joined list; topic0 is the first element. Exact
		// match when it's the only topic, prefix match (with comma) otherwise.
		// Escape LIKE metachars in the user-supplied topic so e.g. ?topic0=%
		// can't widen the prefix pattern into a match-everything wildcard.
		where = append(where, `(topics = ? OR topics LIKE ? ESCAPE '\')`)
		args = append(args, *filter.Topic0, escapeLikePattern(*filter.Topic0)+",%")
	}
	if filter.After != nil {
		// Strictly after (block, log_index, source) — three-tuple lexicographic
		// comparison preserves total ordering across sources.
		where = append(where,
			"(block_number > ? OR (block_number = ? AND log_index > ?) OR (block_number = ? AND log_index = ? AND source > ?))")
		args = append(args,
			filter.After.Block,
			filter.After.Block, filter.After.LogIndex,
			filter.After.Block, filter.After.LogIndex, filter.After.Source,
		)
	}

	q := `SELECT source, block_number, tx_hash, log_index, address, topics, data FROM events`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY block_number ASC, log_index ASC, source ASC LIMIT ?"
	// Over-fetch by 1 so a fully-saturated raw fetch is a reliable "SQL has
	// more downstream" signal even when the Go-side post-filter prunes rows.
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	// Single-pass scan that decouples the cursor anchor from the post-filter:
	// matches are accumulated into out (capped at limit); lastRaw tracks the
	// last SQL-scanned (block, log_index, source) regardless of whether it
	// survived the post-filter. Two "more downstream" signals are handled
	// inline:
	//   1. matches saturate mid-scan — return cursor = last out entry so the
	//      just-arrived match is the first hit of the next page.
	//   2. SQL returned its full limit+1 raw rows but matches did NOT saturate
	//      — cursor = lastRaw so the next page resumes past the whole window.
	out := make([]Event, 0, limit)
	rawCount := 0
	var lastRaw EventCursor
	for rows.Next() {
		var (
			e      Event
			topics string
		)
		if err := rows.Scan(&e.Source, &e.BlockNumber, &e.TxHash, &e.LogIndex, &e.Address, &topics, &e.Data); err != nil {
			return nil, nil, fmt.Errorf("scan event: %w", err)
		}
		rawCount++
		lastRaw = EventCursor{Block: e.BlockNumber, LogIndex: e.LogIndex, Source: e.Source}
		if topics != "" {
			e.Topics = strings.Split(topics, ",")
		}
		if !matchesHigherTopics(e.Topics, filter) {
			continue
		}
		if len(out) >= limit {
			last := out[len(out)-1]
			return out, &EventCursor{Block: last.BlockNumber, LogIndex: last.LogIndex, Source: last.Source}, nil
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate events: %w", err)
	}

	if rawCount > limit {
		anchor := lastRaw
		return out, &anchor, nil
	}
	return out, nil, nil
}

// escapeLikePattern prefixes the SQL LIKE metacharacters (%, _) and the
// escape character itself with a backslash so the caller's input can't
// accidentally widen a prefix match. Pair with `ESCAPE '\'` in the SQL.
func escapeLikePattern(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\\' || r == '%' || r == '_' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func matchesHigherTopics(topics []string, f QueryFilter) bool {
	for i, want := range []*string{f.Topic1, f.Topic2, f.Topic3} {
		if want == nil {
			continue
		}
		pos := i + 1
		var got string
		if pos < len(topics) {
			got = topics[pos]
		}
		if got != *want {
			return false
		}
	}
	return true
}

func (s *SQLite) Close() error {
	return s.db.Close()
}

func (s *SQLite) DB() *sql.DB { return s.db }

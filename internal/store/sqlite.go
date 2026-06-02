package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

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
	s := &SQLite{db: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS events (
    block_number INTEGER NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    address      TEXT    NOT NULL,
    topics       TEXT    NOT NULL,
    data         TEXT    NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_events_block ON events(block_number);

CREATE TABLE IF NOT EXISTS checkpoint (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    block_number INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS block_hashes (
    block_number INTEGER PRIMARY KEY,
    hash         TEXT    NOT NULL,
    parent_hash  TEXT    NOT NULL
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

func (s *SQLite) SaveEvents(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO events
		(block_number, tx_hash, log_index, address, topics, data)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, e := range events {
		topics := strings.Join(e.Topics, ",")
		if _, err := stmt.ExecContext(ctx, e.BlockNumber, e.TxHash, e.LogIndex, e.Address, topics, e.Data); err != nil {
			return fmt.Errorf("insert event %s/%d: %w", e.TxHash, e.LogIndex, err)
		}
	}
	return tx.Commit()
}

func (s *SQLite) Checkpoint(ctx context.Context) (uint64, error) {
	var block uint64
	err := s.db.QueryRowContext(ctx, `SELECT block_number FROM checkpoint WHERE id = 1`).Scan(&block)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}
	return block, nil
}

func (s *SQLite) SetCheckpoint(ctx context.Context, block uint64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO checkpoint (id, block_number) VALUES (1, ?)
		ON CONFLICT(id) DO UPDATE SET block_number = excluded.block_number`, block)
	if err != nil {
		return fmt.Errorf("set checkpoint: %w", err)
	}
	return nil
}

func (s *SQLite) RecordBlockHash(ctx context.Context, number uint64, hash, parentHash string, capDepth uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `INSERT INTO block_hashes (block_number, hash, parent_hash)
		VALUES (?, ?, ?)
		ON CONFLICT(block_number) DO UPDATE SET hash = excluded.hash, parent_hash = excluded.parent_hash`,
		number, hash, parentHash); err != nil {
		return fmt.Errorf("upsert block hash %d: %w", number, err)
	}
	if capDepth > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM block_hashes
			WHERE block_number <= (SELECT MAX(block_number) FROM block_hashes) - ?`, capDepth); err != nil {
			return fmt.Errorf("trim block hashes: %w", err)
		}
	}
	return tx.Commit()
}

func (s *SQLite) RecentBlockHashes(ctx context.Context, limit uint64) ([]BlockHashEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT block_number, hash, parent_hash FROM block_hashes
		ORDER BY block_number DESC LIMIT ?`, limit)
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

func (s *SQLite) BlockHashAt(ctx context.Context, number uint64) (BlockHashEntry, bool, error) {
	var e BlockHashEntry
	err := s.db.QueryRowContext(ctx, `SELECT block_number, hash, parent_hash FROM block_hashes
		WHERE block_number = ?`, number).Scan(&e.Number, &e.Hash, &e.ParentHash)
	if errors.Is(err, sql.ErrNoRows) {
		return BlockHashEntry{}, false, nil
	}
	if err != nil {
		return BlockHashEntry{}, false, fmt.Errorf("read block hash %d: %w", number, err)
	}
	return e, true, nil
}

func (s *SQLite) RewindTo(ctx context.Context, block uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE block_number >= ?`, block); err != nil {
		return fmt.Errorf("rewind events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM block_hashes WHERE block_number >= ?`, block); err != nil {
		return fmt.Errorf("rewind block hashes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint (id, block_number) VALUES (1, ?)
		ON CONFLICT(id) DO UPDATE SET block_number = excluded.block_number`, block); err != nil {
		return fmt.Errorf("rewind checkpoint: %w", err)
	}
	return tx.Commit()
}

// Query implements Store.Query against the events table.
//
// SQL pushdown covers Contract, FromBlock, ToBlock, Topic0, and cursor
// position; higher topics (Topic1..Topic3) are post-filtered in Go because
// the table stores topics as a comma-joined string and a position-aware LIKE
// chain over multiple positions would be both fragile and expensive. v2 may
// push these down once typed per-event tables are the primary read path.
//
// Cursor strategy: fetch limit+1 rows so the next cursor can be derived
// without a second query. If the +1 row was returned, the next cursor points
// at the last row of the limit-truncated result.
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
		where = append(where, "(topics = ? OR topics LIKE ?)")
		args = append(args, *filter.Topic0, *filter.Topic0+",%")
	}
	if filter.After != nil {
		// Strictly after (block, log_index).
		where = append(where, "(block_number > ? OR (block_number = ? AND log_index > ?))")
		args = append(args, filter.After.Block, filter.After.Block, filter.After.LogIndex)
	}

	q := `SELECT block_number, tx_hash, log_index, address, topics, data FROM events`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY block_number ASC, log_index ASC LIMIT ?"
	// Higher-topic filters are applied in Go after the SQL scan. To keep the
	// effective page size correct in that case we'd need a streaming loop;
	// for now the post-filter is rare, so we still fetch limit+1 and accept
	// that a page may come back short when topic1..3 strip rows. Documented
	// in README.
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	out := make([]Event, 0, limit)
	for rows.Next() {
		var (
			e      Event
			topics string
		)
		if err := rows.Scan(&e.BlockNumber, &e.TxHash, &e.LogIndex, &e.Address, &topics, &e.Data); err != nil {
			return nil, nil, fmt.Errorf("scan event: %w", err)
		}
		if topics != "" {
			e.Topics = strings.Split(topics, ",")
		}
		if !matchesHigherTopics(e.Topics, filter) {
			continue
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate events: %w", err)
	}

	// We over-fetched by 1 to detect "more rows exist". If the fetched
	// (post-filter) result is strictly greater than limit, the row at index
	// limit-1 is the cursor anchor and we trim to limit; otherwise no more
	// pages.
	if len(out) > limit {
		anchor := out[limit-1]
		out = out[:limit]
		return out, &EventCursor{Block: anchor.BlockNumber, LogIndex: anchor.LogIndex}, nil
	}
	return out, nil, nil
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

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

func (s *SQLite) Close() error {
	return s.db.Close()
}

func (s *SQLite) DB() *sql.DB { return s.db }

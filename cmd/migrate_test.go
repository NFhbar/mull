package cmd

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/NFhbar/mull/internal/store"

	_ "modernc.org/sqlite"
)

// writeV1DBAndConfig builds a v1-shaped DB plus a minimal mull.yaml pointing
// at it under tmp. Returns the config path so the test can plug it into
// cfgFile.
func writeV1DBAndConfig(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "mull.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("wal: %v", err)
	}
	if _, err := db.Exec(`
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
        INSERT INTO checkpoint (id, block_number) VALUES (1, 123);
    `); err != nil {
		t.Fatalf("v1 schema: %v", err)
	}

	cfgPath := filepath.Join(tmp, "mull.yaml")
	body := `
db_path: ` + dbPath + `
sources:
  - name: default
    rpc_url: https://example
    contract: "0xC"
`
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write cfg: %v", err)
	}
	return cfgPath
}

func TestMigrateCommandUpgradesV1DB(t *testing.T) {
	cfgPath := writeV1DBAndConfig(t)

	// runMigrate reads the package-level cfgFile var (set via the persistent
	// --config flag). Swap it out for the test and restore on cleanup.
	prev := cfgFile
	t.Cleanup(func() { cfgFile = prev })
	cfgFile = cfgPath

	if err := runMigrate(context.Background()); err != nil {
		t.Fatalf("runMigrate: %v", err)
	}

	// Open the migrated DB raw and assert v2 shape.
	db, err := sql.Open("sqlite", dbPathFromCfg(t, cfgPath))
	if err != nil {
		t.Fatalf("open migrated: %v", err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if version != store.SchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, store.SchemaVersion)
	}
	// Checkpoint row migrated to (source='default', block_number=123).
	var src string
	var block uint64
	if err := db.QueryRow(`SELECT source, block_number FROM checkpoint`).Scan(&src, &block); err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if src != "default" || block != 123 {
		t.Fatalf("checkpoint = (%s, %d), want (default, 123)", src, block)
	}
}

func TestMigrateCommandIdempotentOnV2(t *testing.T) {
	cfgPath := writeV1DBAndConfig(t)

	prev := cfgFile
	t.Cleanup(func() { cfgFile = prev })
	cfgFile = cfgPath

	if err := runMigrate(context.Background()); err != nil {
		t.Fatalf("first runMigrate: %v", err)
	}
	// Second run: v1→v2 is a no-op and the drifted-table rebuild (empty
	// generated set in the committed stub) finds nothing to do.
	if err := runMigrate(context.Background()); err != nil {
		t.Fatalf("second runMigrate: %v", err)
	}

	db, err := sql.Open("sqlite", dbPathFromCfg(t, cfgPath))
	if err != nil {
		t.Fatalf("open migrated: %v", err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if version != store.SchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, store.SchemaVersion)
	}
}

// dbPathFromCfg parses the temp config to discover the dbpath. The test wrote
// it; we re-read it here to avoid coupling the test to writeV1DBAndConfig's
// path layout.
func dbPathFromCfg(t *testing.T, cfgPath string) string {
	t.Helper()
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read cfg: %v", err)
	}
	// Cheap parse: scan for `db_path:` line.
	const key = "db_path:"
	s := string(data)
	idx := indexOf(s, key)
	if idx < 0 {
		t.Fatalf("no db_path in cfg: %s", s)
	}
	rest := s[idx+len(key):]
	// trim leading spaces and a possible newline
	for len(rest) > 0 && (rest[0] == ' ' || rest[0] == '\t') {
		rest = rest[1:]
	}
	end := 0
	for end < len(rest) && rest[end] != '\n' {
		end++
	}
	return rest[:end]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

const transferSig = "source:TEXT,block_number:INTEGER,tx_hash:TEXT,log_index:INTEGER,from:TEXT,to:TEXT,value:TEXT"

const transferDDL = `CREATE TABLE IF NOT EXISTS events_erc20_transfer (
    source       TEXT    NOT NULL,
    block_number INTEGER NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    "from"       TEXT    NOT NULL,
    "to"         TEXT    NOT NULL,
    "value"      TEXT    NOT NULL,
    PRIMARY KEY (source, tx_hash, log_index)
)`

func openSignaturesDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "sig.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func stampedSignature(t *testing.T, db *sql.DB, table string) (string, bool) {
	t.Helper()
	var sig string
	err := db.QueryRow(`SELECT column_signature FROM gen_schema_versions WHERE table_name = ?`, table).Scan(&sig)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("read stamp for %s: %v", table, err)
	}
	return sig, true
}

func TestEnsureSchemaSignatures_FirstRunStamps(t *testing.T) {
	db := openSignaturesDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, transferDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	orphans, err := EnsureSchemaSignatures(ctx, db, map[string]string{"events_erc20_transfer": transferSig})
	if err != nil {
		t.Fatalf("EnsureSchemaSignatures: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %v, want none", orphans)
	}
	sig, ok := stampedSignature(t, db, "events_erc20_transfer")
	if !ok {
		t.Fatal("no stamp row inserted")
	}
	if sig != transferSig {
		t.Fatalf("stamped signature = %q, want %q", sig, transferSig)
	}
}

func TestEnsureSchemaSignatures_MatchProceeds(t *testing.T) {
	db := openSignaturesDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, transferDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	sigs := map[string]string{"events_erc20_transfer": transferSig}

	for i := 0; i < 2; i++ {
		if _, err := EnsureSchemaSignatures(ctx, db, sigs); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	sig, ok := stampedSignature(t, db, "events_erc20_transfer")
	if !ok || sig != transferSig {
		t.Fatalf("stamp after second run = %q (present=%v), want %q", sig, ok, transferSig)
	}
}

func TestEnsureSchemaSignatures_MismatchFailsLoudly(t *testing.T) {
	db := openSignaturesDB(t)
	ctx := context.Background()
	// Old shape: no "to" column — simulates an ABI field-set change after the
	// table was created.
	if _, err := db.ExecContext(ctx, `CREATE TABLE events_erc20_transfer (
        source       TEXT    NOT NULL,
        block_number INTEGER NOT NULL,
        tx_hash      TEXT    NOT NULL,
        log_index    INTEGER NOT NULL,
        "from"       TEXT    NOT NULL,
        "value"      TEXT    NOT NULL,
        PRIMARY KEY (source, tx_hash, log_index)
    )`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	oldSig := "source:TEXT,block_number:INTEGER,tx_hash:TEXT,log_index:INTEGER,from:TEXT,value:TEXT"

	_, err := EnsureSchemaSignatures(ctx, db, map[string]string{"events_erc20_transfer": transferSig})
	if err == nil {
		t.Fatal("expected drift error, got nil")
	}
	for _, want := range []string{"events_erc20_transfer", oldSig, transferSig} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
}

func TestEnsureSchemaSignatures_BackfillsUnstampedTable(t *testing.T) {
	db := openSignaturesDB(t)
	ctx := context.Background()
	// Pre-stamping deployment: typed table and meta table both exist, but no
	// signature row was ever written for the table.
	if _, err := db.ExecContext(ctx, transferDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE gen_schema_versions (
        table_name       TEXT PRIMARY KEY,
        column_signature TEXT NOT NULL
    )`); err != nil {
		t.Fatalf("create meta table: %v", err)
	}

	orphans, err := EnsureSchemaSignatures(ctx, db, map[string]string{"events_erc20_transfer": transferSig})
	if err != nil {
		t.Fatalf("EnsureSchemaSignatures: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %v, want none", orphans)
	}
	sig, ok := stampedSignature(t, db, "events_erc20_transfer")
	if !ok {
		t.Fatal("expected backfilled stamp row")
	}
	if sig != transferSig {
		t.Fatalf("backfilled signature = %q, want %q", sig, transferSig)
	}
}

func TestEnsureSchemaSignatures_RepairsStaleStamp(t *testing.T) {
	db := openSignaturesDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, transferDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE gen_schema_versions (
        table_name       TEXT PRIMARY KEY,
        column_signature TEXT NOT NULL
    )`); err != nil {
		t.Fatalf("create meta table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO gen_schema_versions (table_name, column_signature) VALUES ('events_erc20_transfer', 'stale:WRONG')`); err != nil {
		t.Fatalf("seed stale stamp: %v", err)
	}

	if _, err := EnsureSchemaSignatures(ctx, db, map[string]string{"events_erc20_transfer": transferSig}); err != nil {
		t.Fatalf("EnsureSchemaSignatures: %v", err)
	}
	sig, _ := stampedSignature(t, db, "events_erc20_transfer")
	if sig != transferSig {
		t.Fatalf("stamp after repair = %q, want %q (PRAGMA shape is ground truth)", sig, transferSig)
	}
}

func TestEnsureSchemaSignatures_OrphanReturnedNotDeleted(t *testing.T) {
	db := openSignaturesDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, transferDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// Orphan: event removed from the ABI — its typed table and stamp survive.
	if _, err := db.ExecContext(ctx, `CREATE TABLE events_erc20_approval (
        source       TEXT    NOT NULL,
        block_number INTEGER NOT NULL,
        tx_hash      TEXT    NOT NULL,
        log_index    INTEGER NOT NULL,
        "owner"      TEXT    NOT NULL,
        PRIMARY KEY (source, tx_hash, log_index)
    )`); err != nil {
		t.Fatalf("create orphan table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE gen_schema_versions (
        table_name       TEXT PRIMARY KEY,
        column_signature TEXT NOT NULL
    )`); err != nil {
		t.Fatalf("create meta table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO gen_schema_versions (table_name, column_signature) VALUES ('events_erc20_approval', 'source:TEXT,block_number:INTEGER,tx_hash:TEXT,log_index:INTEGER,owner:TEXT')`); err != nil {
		t.Fatalf("seed orphan stamp: %v", err)
	}

	orphans, err := EnsureSchemaSignatures(ctx, db, map[string]string{"events_erc20_transfer": transferSig})
	if err != nil {
		t.Fatalf("EnsureSchemaSignatures: %v", err)
	}
	if len(orphans) != 1 || orphans[0] != "events_erc20_approval" {
		t.Fatalf("orphans = %v, want [events_erc20_approval]", orphans)
	}
	if _, ok := stampedSignature(t, db, "events_erc20_approval"); !ok {
		t.Fatal("orphan stamp row was deleted; must be left in place")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events_erc20_approval`).Scan(&n); err != nil {
		t.Fatalf("orphan typed table was dropped: %v", err)
	}
}

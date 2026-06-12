package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const rebuildSig = "source:TEXT,block_number:INTEGER,tx_hash:TEXT,log_index:INTEGER,value:TEXT"

const rebuildDDL = `CREATE TABLE IF NOT EXISTS events_test_transfer (
    source       TEXT    NOT NULL,
    block_number INTEGER NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    "value"      TEXT    NOT NULL,
    PRIMARY KEY (source, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS events_test_approval (
    source       TEXT    NOT NULL,
    block_number INTEGER NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    "value"      TEXT    NOT NULL,
    PRIMARY KEY (source, tx_hash, log_index)
);`

// driftedTransferDDL is the "old shape" — missing the "value" column.
const driftedTransferDDL = `CREATE TABLE events_test_transfer (
    source       TEXT    NOT NULL,
    block_number INTEGER NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    PRIMARY KEY (source, tx_hash, log_index)
)`

const (
	transferTopic0 = "0xtransfer"
	approvalTopic0 = "0xapproval"
)

// rebuildTestSink mirrors a generated sink: idempotent INSERT OR IGNORE on
// (source, tx_hash, log_index), skip on topic0 mismatch, writes through the
// Execer it was constructed over. The stored "value" is e.Data — a pure
// function of the input log, like a real decoder.
type rebuildTestSink struct {
	db     Execer
	id     string
	topic0 string
	table  string
	calls  *int
	failAt int // fail on the Nth matching Handle call (1-based); 0 = never
}

func (s *rebuildTestSink) SinkID() string { return s.id }
func (s *rebuildTestSink) Topic0() string { return s.topic0 }

func (s *rebuildTestSink) Handle(ctx context.Context, e Event) error {
	if len(e.Topics) == 0 || e.Topics[0] != s.topic0 {
		return nil
	}
	*s.calls++
	if s.failAt > 0 && *s.calls >= s.failAt {
		return fmt.Errorf("sink boom")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO `+s.table+` (source, block_number, tx_hash, log_index, "value") VALUES (?, ?, ?, ?, ?)`,
		e.Source, e.BlockNumber, e.TxHash, e.LogIndex, e.Data)
	return err
}

func (s *rebuildTestSink) RewindTo(ctx context.Context, source string, block uint64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM `+s.table+` WHERE source = ? AND block_number >= ?`, source, block)
	return err
}

func rebuildTestSpec(calls *int, failAt int) RebuildSpec {
	return RebuildSpec{
		DDL: rebuildDDL,
		Signatures: map[string]string{
			"events_test_transfer": rebuildSig,
			"events_test_approval": rebuildSig,
		},
		Topics: map[string]string{
			"events_test_transfer": transferTopic0,
			"events_test_approval": approvalTopic0,
		},
		NewSinks: func(db Execer) []EventSink {
			return []EventSink{
				&rebuildTestSink{db: db, id: "Transfer", topic0: transferTopic0, table: "events_test_transfer", calls: calls, failAt: failAt},
				&rebuildTestSink{db: db, id: "Approval", topic0: approvalTopic0, table: "events_test_approval", calls: calls},
			}
		},
	}
}

func openRebuildStore(t *testing.T) *SQLite {
	t.Helper()
	s, err := OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "rebuild.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type typedRow struct {
	Source string
	Block  uint64
	TxHash string
	LogIdx uint
	Value  string
}

func readTypedRows(t *testing.T, s *SQLite, table string) []typedRow {
	t.Helper()
	rows, err := s.db.Query(`SELECT source, block_number, tx_hash, log_index, "value" FROM ` + table + ` ORDER BY block_number, log_index, source`)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer rows.Close()
	var out []typedRow
	for rows.Next() {
		var r typedRow
		if err := rows.Scan(&r.Source, &r.Block, &r.TxHash, &r.LogIdx, &r.Value); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s: %v", table, err)
	}
	return out
}

func TestRebuildDriftedTables_RebuildsDriftedFromRaw(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	// Transfer table at the old (drifted) shape with a stale stamp; approval
	// table at the correct shape with a pre-existing marker row.
	if _, err := s.db.ExecContext(ctx, driftedTransferDDL); err != nil {
		t.Fatalf("create drifted: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, createGenSchemaVersionsDDL); err != nil {
		t.Fatalf("create meta: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, upsertGenSchemaVersionSQL, "events_test_transfer", "stale-sig"); err != nil {
		t.Fatalf("stale stamp: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, rebuildDDL); err != nil {
		t.Fatalf("create approval: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO events_test_approval (source, block_number, tx_hash, log_index, "value") VALUES ('a', 9, '0xm', 0, 'marker')`); err != nil {
		t.Fatalf("marker row: %v", err)
	}

	if err := s.SaveEvents(ctx, "a", []Event{
		{Source: "a", BlockNumber: 2, TxHash: "0x2", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0}, Data: "v2"},
		{Source: "a", BlockNumber: 1, TxHash: "0x1", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0, "0xextra"}, Data: "v1"},
		{Source: "a", BlockNumber: 3, TxHash: "0x3", LogIndex: 0, Address: "0xC", Topics: []string{"0xother"}, Data: "skip"},
	}); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := s.SaveEvents(ctx, "b", []Event{
		{Source: "b", BlockNumber: 1, TxHash: "0xb1", LogIndex: 1, Address: "0xC", Topics: []string{transferTopic0}, Data: "vb"},
	}); err != nil {
		t.Fatalf("save b: %v", err)
	}

	var calls int
	rebuilt, err := s.RebuildDriftedTables(ctx, rebuildTestSpec(&calls, 0))
	if err != nil {
		t.Fatalf("RebuildDriftedTables: %v", err)
	}
	if !reflect.DeepEqual(rebuilt, []string{"events_test_transfer"}) {
		t.Fatalf("rebuilt = %v, want [events_test_transfer]", rebuilt)
	}

	sig, err := tableColumnSignature(ctx, s.db, "events_test_transfer")
	if err != nil {
		t.Fatalf("signature: %v", err)
	}
	if sig != rebuildSig {
		t.Fatalf("on-disk signature = %q, want %q", sig, rebuildSig)
	}
	stamped, ok := stampedSignature(t, s.db, "events_test_transfer")
	if !ok || stamped != rebuildSig {
		t.Fatalf("stamp = %q (present=%v), want %q", stamped, ok, rebuildSig)
	}

	got := readTypedRows(t, s, "events_test_transfer")
	want := []typedRow{
		{Source: "a", Block: 1, TxHash: "0x1", LogIdx: 0, Value: "v1"},
		{Source: "b", Block: 1, TxHash: "0xb1", LogIdx: 1, Value: "vb"},
		{Source: "a", Block: 2, TxHash: "0x2", LogIdx: 0, Value: "v2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rebuilt rows = %+v, want %+v", got, want)
	}

	approval := readTypedRows(t, s, "events_test_approval")
	if len(approval) != 1 || approval[0].Value != "marker" {
		t.Fatalf("approval table touched: %+v", approval)
	}
}

func TestRebuildDriftedTables_NoDriftNoOp(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, rebuildDDL); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO events_test_transfer (source, block_number, tx_hash, log_index, "value") VALUES ('a', 1, '0x1', 0, 'keep')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	var calls int
	rebuilt, err := s.RebuildDriftedTables(ctx, rebuildTestSpec(&calls, 0))
	if err != nil {
		t.Fatalf("RebuildDriftedTables: %v", err)
	}
	if rebuilt != nil {
		t.Fatalf("rebuilt = %v, want nil", rebuilt)
	}
	if calls != 0 {
		t.Fatalf("sink Handle calls = %d, want 0", calls)
	}
	got := readTypedRows(t, s, "events_test_transfer")
	if len(got) != 1 || got[0].Value != "keep" {
		t.Fatalf("rows touched: %+v", got)
	}
}

// TestRebuildDriftedTables_RebuildEqualsLivePath pins the change's done-when
// invariant: rebuilding from the raw events table reproduces exactly the
// typed content the live sink path (indexer dispatch) produced.
func TestRebuildDriftedTables_RebuildEqualsLivePath(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, rebuildDDL); err != nil {
		t.Fatalf("create tables: %v", err)
	}

	events := []Event{
		{Source: "a", BlockNumber: 1, TxHash: "0x1", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0}, Data: "v1"},
		{Source: "a", BlockNumber: 2, TxHash: "0x2", LogIndex: 3, Address: "0xC", Topics: []string{transferTopic0, "0xextra"}, Data: "v2"},
		{Source: "b", BlockNumber: 2, TxHash: "0xb2", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0}, Data: "vb"},
		{Source: "a", BlockNumber: 3, TxHash: "0x3", LogIndex: 0, Address: "0xC", Topics: []string{"0xother"}, Data: "skip"},
	}
	if err := s.SaveEvents(ctx, "a", []Event{events[0], events[1], events[3]}); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := s.SaveEvents(ctx, "b", []Event{events[2]}); err != nil {
		t.Fatalf("save b: %v", err)
	}

	// Live path: dispatch every raw event through the same sink the indexer
	// would use, constructed over the live handle.
	var liveCalls int
	spec := rebuildTestSpec(&liveCalls, 0)
	var live EventSink
	for _, sink := range spec.NewSinks(s.db) {
		if sink.Topic0() == transferTopic0 {
			live = sink
		}
	}
	for _, e := range events {
		if err := live.Handle(ctx, e); err != nil {
			t.Fatalf("live Handle: %v", err)
		}
	}
	snapshot := readTypedRows(t, s, "events_test_transfer")
	if len(snapshot) != 3 {
		t.Fatalf("live rows = %d, want 3", len(snapshot))
	}

	// Hand-drop the table (the old README remedy) — absent counts as drifted.
	if _, err := s.db.ExecContext(ctx, `DROP TABLE events_test_transfer`); err != nil {
		t.Fatalf("drop: %v", err)
	}

	var calls int
	rebuilt, err := s.RebuildDriftedTables(ctx, rebuildTestSpec(&calls, 0))
	if err != nil {
		t.Fatalf("RebuildDriftedTables: %v", err)
	}
	if !reflect.DeepEqual(rebuilt, []string{"events_test_transfer"}) {
		t.Fatalf("rebuilt = %v, want [events_test_transfer]", rebuilt)
	}
	got := readTypedRows(t, s, "events_test_transfer")
	if !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("rebuild != live path\n got: %+v\nwant: %+v", got, snapshot)
	}
}

func TestRebuildDriftedTables_RollbackOnSinkFailure(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, driftedTransferDDL); err != nil {
		t.Fatalf("create drifted: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO events_test_transfer (source, block_number, tx_hash, log_index) VALUES ('a', 1, '0x1', 0)`); err != nil {
		t.Fatalf("seed old row: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, createGenSchemaVersionsDDL); err != nil {
		t.Fatalf("create meta: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, upsertGenSchemaVersionSQL, "events_test_transfer", "old-sig"); err != nil {
		t.Fatalf("old stamp: %v", err)
	}
	// Approval at correct shape so only transfer is rebuilt.
	if _, err := s.db.ExecContext(ctx, rebuildDDL); err != nil {
		t.Fatalf("create approval: %v", err)
	}
	if err := s.SaveEvents(ctx, "a", []Event{
		{Source: "a", BlockNumber: 1, TxHash: "0x1", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0}, Data: "v1"},
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	const oldShapeSig = "source:TEXT,block_number:INTEGER,tx_hash:TEXT,log_index:INTEGER"

	var calls int
	_, err := s.RebuildDriftedTables(ctx, rebuildTestSpec(&calls, 1))
	if err == nil {
		t.Fatal("expected sink failure to surface, got nil")
	}
	if !strings.Contains(err.Error(), "rebuild events_test_transfer") {
		t.Fatalf("error %q missing table context", err)
	}

	// The per-table transaction rolled back: old shape, original row, and
	// original stamp all intact.
	sig, sigErr := tableColumnSignature(ctx, s.db, "events_test_transfer")
	if sigErr != nil {
		t.Fatalf("signature: %v", sigErr)
	}
	if sig != oldShapeSig {
		t.Fatalf("on-disk signature after rollback = %q, want %q", sig, oldShapeSig)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events_test_transfer`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows after rollback = %d, want 1", n)
	}
	stamped, ok := stampedSignature(t, s.db, "events_test_transfer")
	if !ok || stamped != "old-sig" {
		t.Fatalf("stamp after rollback = %q (present=%v), want old-sig", stamped, ok)
	}
}

// TestRebuildDriftedTables_MultipleAbsentTablesAllReplayed pins the
// multi-table contract: with two absent tables, both must be dropped,
// replayed, and restamped — rebuilding the first (sorted order) must not
// cause the sibling to be skipped on a now-matching signature.
func TestRebuildDriftedTables_MultipleAbsentTablesAllReplayed(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	if err := s.SaveEvents(ctx, "a", []Event{
		{Source: "a", BlockNumber: 1, TxHash: "0x1", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0}, Data: "t1"},
		{Source: "a", BlockNumber: 2, TxHash: "0x2", LogIndex: 0, Address: "0xC", Topics: []string{approvalTopic0}, Data: "a1"},
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var calls int
	rebuilt, err := s.RebuildDriftedTables(ctx, rebuildTestSpec(&calls, 0))
	if err != nil {
		t.Fatalf("RebuildDriftedTables: %v", err)
	}
	if !reflect.DeepEqual(rebuilt, []string{"events_test_approval", "events_test_transfer"}) {
		t.Fatalf("rebuilt = %v, want both tables", rebuilt)
	}

	for table, want := range map[string]typedRow{
		"events_test_approval": {Source: "a", Block: 2, TxHash: "0x2", LogIdx: 0, Value: "a1"},
		"events_test_transfer": {Source: "a", Block: 1, TxHash: "0x1", LogIdx: 0, Value: "t1"},
	} {
		got := readTypedRows(t, s, table)
		if len(got) != 1 || got[0] != want {
			t.Fatalf("%s rows = %+v, want [%+v]", table, got, want)
		}
		stamped, ok := stampedSignature(t, s.db, table)
		if !ok || stamped != rebuildSig {
			t.Fatalf("%s stamp = %q (present=%v), want %q", table, stamped, ok, rebuildSig)
		}
	}
}

// TestRebuildDriftedTables_FailureThenRerunRebuildsRemainder pins the
// failure-path contract: rebuilding one table must not durably create an
// absent sibling. Approval (first in sorted order) commits, then transfer's
// sink fails mid-replay; had approval's rebuild created transfer empty at
// the fresh shape, the re-run's drift scan would see a matching signature
// and silently skip it — losing transfer's history.
func TestRebuildDriftedTables_FailureThenRerunRebuildsRemainder(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	if err := s.SaveEvents(ctx, "a", []Event{
		{Source: "a", BlockNumber: 1, TxHash: "0x1", LogIndex: 0, Address: "0xC", Topics: []string{transferTopic0}, Data: "t1"},
		{Source: "a", BlockNumber: 2, TxHash: "0x2", LogIndex: 0, Address: "0xC", Topics: []string{approvalTopic0}, Data: "a1"},
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Both tables absent, so both count as drifted. Approval's replay handles
	// one matching event (calls=1); transfer's first matching call is calls=2,
	// where failAt fires.
	var calls int
	rebuilt, err := s.RebuildDriftedTables(ctx, rebuildTestSpec(&calls, 2))
	if err == nil {
		t.Fatal("expected transfer sink failure, got nil")
	}
	if !strings.Contains(err.Error(), "rebuild events_test_transfer") {
		t.Fatalf("error %q missing table context", err)
	}
	if !reflect.DeepEqual(rebuilt, []string{"events_test_approval"}) {
		t.Fatalf("rebuilt = %v, want [events_test_approval]", rebuilt)
	}

	// Transfer's failed rebuild rolled back, and approval's committed rebuild
	// must not have created transfer as a side effect: still absent, still
	// unstamped — i.e. still drifted on a re-run.
	sig, err := tableColumnSignature(ctx, s.db, "events_test_transfer")
	if err != nil {
		t.Fatalf("signature: %v", err)
	}
	if sig != "" {
		t.Fatalf("transfer exists after failed rebuild (signature %q), want absent", sig)
	}
	if stamped, ok := stampedSignature(t, s.db, "events_test_transfer"); ok {
		t.Fatalf("transfer stamped after failed rebuild: %q", stamped)
	}

	var rerunCalls int
	rebuilt, err = s.RebuildDriftedTables(ctx, rebuildTestSpec(&rerunCalls, 0))
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if !reflect.DeepEqual(rebuilt, []string{"events_test_transfer"}) {
		t.Fatalf("re-run rebuilt = %v, want [events_test_transfer]", rebuilt)
	}
	got := readTypedRows(t, s, "events_test_transfer")
	want := []typedRow{{Source: "a", Block: 1, TxHash: "0x1", LogIdx: 0, Value: "t1"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transfer rows after re-run = %+v, want %+v", got, want)
	}
	stamped, ok := stampedSignature(t, s.db, "events_test_transfer")
	if !ok || stamped != rebuildSig {
		t.Fatalf("transfer stamp after re-run = %q (present=%v), want %q", stamped, ok, rebuildSig)
	}
	approval := readTypedRows(t, s, "events_test_approval")
	if len(approval) != 1 || approval[0].Value != "a1" {
		t.Fatalf("approval rows = %+v, want the committed a1 row", approval)
	}
}

func TestRebuildDriftedTables_MissingSinkErrors(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	if _, err := s.db.ExecContext(ctx, driftedTransferDDL); err != nil {
		t.Fatalf("create drifted: %v", err)
	}

	var calls int
	spec := rebuildTestSpec(&calls, 0)
	// Restrict to the one drifted table — the absent approval table would
	// otherwise count as drifted too and rebuild first (sorted order).
	spec.Signatures = map[string]string{"events_test_transfer": rebuildSig}
	spec.Topics = map[string]string{"events_test_transfer": transferTopic0}
	spec.NewSinks = func(Execer) []EventSink { return nil }
	_, err := s.RebuildDriftedTables(ctx, spec)
	if err == nil {
		t.Fatal("expected missing-sink error, got nil")
	}
	for _, want := range []string{"events_test_transfer", "no generated sink"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
}

func TestEventsByTopic0_FilterAndOrder(t *testing.T) {
	s := openRebuildStore(t)
	ctx := context.Background()

	// topic0 contains a LIKE metacharacter; "0xabXcd" would match an
	// unescaped "0xab%cd,%" pattern and must be excluded.
	const topic = "0xab%cd"
	if err := s.SaveEvents(ctx, "a", []Event{
		{Source: "a", BlockNumber: 2, TxHash: "0x2", LogIndex: 0, Address: "0xC", Topics: []string{topic}, Data: "d2"},
		{Source: "a", BlockNumber: 1, TxHash: "0x1", LogIndex: 0, Address: "0xC", Topics: []string{topic, "0xz"}, Data: "d1"},
		{Source: "a", BlockNumber: 3, TxHash: "0x3", LogIndex: 0, Address: "0xC", Topics: []string{"0xabXcd"}, Data: "wild"},
		{Source: "a", BlockNumber: 4, TxHash: "0x4", LogIndex: 0, Address: "0xC", Topics: []string{"0xother"}, Data: "no"},
	}); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := s.SaveEvents(ctx, "b", []Event{
		{Source: "b", BlockNumber: 1, TxHash: "0xb1", LogIndex: 0, Address: "0xC", Topics: []string{topic}, Data: "db"},
	}); err != nil {
		t.Fatalf("save b: %v", err)
	}

	var got []Event
	if err := s.eventsByTopic0(ctx, topic, func(e Event) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("eventsByTopic0: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("matched %d events, want 3: %+v", len(got), got)
	}
	order := []struct {
		source string
		block  uint64
		data   string
	}{
		{"a", 1, "d1"},
		{"b", 1, "db"},
		{"a", 2, "d2"},
	}
	for i, want := range order {
		if got[i].Source != want.source || got[i].BlockNumber != want.block || got[i].Data != want.data {
			t.Fatalf("event[%d] = %+v, want %+v", i, got[i], want)
		}
	}

	// Callback errors propagate.
	sentinel := errors.New("stop")
	err := s.eventsByTopic0(ctx, topic, func(Event) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback error = %v, want %v", err, sentinel)
	}
}

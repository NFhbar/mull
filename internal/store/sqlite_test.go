package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *SQLite {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

const defaultSrc = "default"

func TestCheckpointRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	got, err := s.Checkpoint(ctx, defaultSrc)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got != 0 {
		t.Fatalf("empty store checkpoint = %d, want 0", got)
	}

	if err := s.SetCheckpoint(ctx, defaultSrc, 100); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, _ := s.Checkpoint(ctx, defaultSrc); got != 100 {
		t.Fatalf("after set: %d, want 100", got)
	}

	if err := s.SetCheckpoint(ctx, defaultSrc, 250); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got, _ := s.Checkpoint(ctx, defaultSrc); got != 250 {
		t.Fatalf("after update: %d, want 250", got)
	}
}

func TestSaveEventsDedupes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	events := []Event{
		{BlockNumber: 1, TxHash: "0xa", LogIndex: 0, Address: "0xc", Topics: []string{"0xt"}, Data: "0x"},
		{BlockNumber: 1, TxHash: "0xa", LogIndex: 1, Address: "0xc", Topics: []string{"0xt"}, Data: "0x"},
	}
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Re-saving the same events must be a no-op (PRIMARY KEY conflict ignored).
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("save again: %v", err)
	}

	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("event count = %d, want 2", n)
	}
}

func TestSaveEventsEmpty(t *testing.T) {
	s := newTestStore(t)
	if err := s.SaveEvents(context.Background(), defaultSrc, nil); err != nil {
		t.Fatalf("empty save: %v", err)
	}
}

func TestBlockHashRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.RecordBlockHash(ctx, defaultSrc, 10, "0xh10", "0xh9", 64); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordBlockHash(ctx, defaultSrc, 11, "0xh11", "0xh10", 64); err != nil {
		t.Fatalf("record: %v", err)
	}

	got, ok, err := s.BlockHashAt(ctx, defaultSrc, 10)
	if err != nil {
		t.Fatalf("BlockHashAt: %v", err)
	}
	if !ok || got.Number != 10 || got.Hash != "0xh10" || got.ParentHash != "0xh9" {
		t.Fatalf("BlockHashAt(10) = %+v, ok=%v", got, ok)
	}

	_, ok, err = s.BlockHashAt(ctx, defaultSrc, 999)
	if err != nil {
		t.Fatalf("BlockHashAt missing: %v", err)
	}
	if ok {
		t.Fatal("missing block should return ok=false")
	}

	recent, err := s.RecentBlockHashes(ctx, defaultSrc, 10)
	if err != nil {
		t.Fatalf("RecentBlockHashes: %v", err)
	}
	if len(recent) != 2 || recent[0].Number != 11 || recent[1].Number != 10 {
		t.Fatalf("recent = %+v, want [11,10] DESC", recent)
	}
}

func TestBlockHashRecentOrderingAndCap(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const capDepth = 10
	for n := uint64(1); n <= 100; n++ {
		if err := s.RecordBlockHash(ctx, defaultSrc, n, "h", "p", capDepth); err != nil {
			t.Fatalf("record %d: %v", n, err)
		}
	}
	recent, err := s.RecentBlockHashes(ctx, defaultSrc, 20)
	if err != nil {
		t.Fatalf("RecentBlockHashes: %v", err)
	}
	if len(recent) != capDepth {
		t.Fatalf("len(recent) = %d, want %d (capped)", len(recent), capDepth)
	}
	// Most-recent first; trimmed to (max - capDepth, max] = (90, 100] = 91..100.
	for i, e := range recent {
		want := uint64(100 - i)
		if e.Number != want {
			t.Fatalf("recent[%d].Number = %d, want %d", i, e.Number, want)
		}
	}
}

func TestOpenSQLiteEnablesWAL(t *testing.T) {
	s := newTestStore(t)
	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want %q", mode, "wal")
	}
}

func TestOpenSQLiteStampsUserVersionOnFreshDB(t *testing.T) {
	s := newTestStore(t)
	var v int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if v != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", v, SchemaVersion)
	}
}

func strPtr(s string) *string { return &s }
func u64Ptr(u uint64) *uint64 { return &u }

func seedQueryEvents(t *testing.T, s *SQLite) {
	t.Helper()
	ctx := context.Background()
	events := []Event{
		// Two contracts, two topic0s, blocks 100..104, two events per block.
		{BlockNumber: 100, TxHash: "0xt100a", LogIndex: 0, Address: "0xA", Topics: []string{"0xT0a", "0xT1a"}, Data: "0x"},
		{BlockNumber: 100, TxHash: "0xt100b", LogIndex: 1, Address: "0xB", Topics: []string{"0xT0b", "0xT1a"}, Data: "0x"},
		{BlockNumber: 101, TxHash: "0xt101a", LogIndex: 0, Address: "0xA", Topics: []string{"0xT0a", "0xT1b"}, Data: "0x"},
		{BlockNumber: 101, TxHash: "0xt101b", LogIndex: 1, Address: "0xB", Topics: []string{"0xT0b", "0xT1b"}, Data: "0x"},
		{BlockNumber: 102, TxHash: "0xt102a", LogIndex: 0, Address: "0xA", Topics: []string{"0xT0a", "0xT1a"}, Data: "0x"},
		{BlockNumber: 102, TxHash: "0xt102b", LogIndex: 1, Address: "0xB", Topics: []string{"0xT0b", "0xT1a"}, Data: "0x"},
		{BlockNumber: 103, TxHash: "0xt103a", LogIndex: 0, Address: "0xA", Topics: []string{"0xT0a"}, Data: "0x"},
		{BlockNumber: 103, TxHash: "0xt103b", LogIndex: 1, Address: "0xB", Topics: []string{"0xT0b"}, Data: "0x"},
		{BlockNumber: 104, TxHash: "0xt104a", LogIndex: 0, Address: "0xA", Topics: []string{"0xT0a"}, Data: "0x"},
		{BlockNumber: 104, TxHash: "0xt104b", LogIndex: 1, Address: "0xB", Topics: []string{"0xT0b"}, Data: "0x"},
	}
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestQueryFilters(t *testing.T) {
	s := newTestStore(t)
	seedQueryEvents(t, s)
	ctx := context.Background()

	cases := []struct {
		name   string
		filter QueryFilter
		want   int
	}{
		{"no filter", QueryFilter{}, 10},
		{"contract A", QueryFilter{Contract: "0xA"}, 5},
		{"contract B", QueryFilter{Contract: "0xB"}, 5},
		{"topic0 A", QueryFilter{Topic0: strPtr("0xT0a")}, 5},
		{"topic1 a", QueryFilter{Topic1: strPtr("0xT1a")}, 4},
		{"from 102", QueryFilter{FromBlock: u64Ptr(102)}, 6},
		{"to 101", QueryFilter{ToBlock: u64Ptr(101)}, 4},
		{"range 101..102", QueryFilter{FromBlock: u64Ptr(101), ToBlock: u64Ptr(102)}, 4},
		{"contract+topic0", QueryFilter{Contract: "0xA", Topic0: strPtr("0xT0a")}, 5},
		{"contract+topic1", QueryFilter{Contract: "0xA", Topic1: strPtr("0xT1b")}, 1},
		{"zero limit applies default", QueryFilter{Limit: 0}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events, _, err := s.Query(ctx, tc.filter)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(events) != tc.want {
				t.Fatalf("got %d events, want %d (%+v)", len(events), tc.want, events)
			}
		})
	}
}

func TestQueryLimitClamping(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	events := make([]Event, 0, 1500)
	for n := uint64(1); n <= 1500; n++ {
		events = append(events, Event{
			BlockNumber: n,
			TxHash:      fmt.Sprintf("0xt%d", n),
			LogIndex:    0,
			Address:     "0xc",
			Topics:      []string{"0xT0"},
			Data:        "0x",
		})
	}
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, _, err := s.Query(ctx, QueryFilter{Limit: 5000})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1000 {
		t.Fatalf("len = %d, want 1000 (clamped)", len(got))
	}
	got, _, err = s.Query(ctx, QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("len = %d, want 100 (default)", len(got))
	}
}

func TestQueryCursorPagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed 250 events across 3 contracts: A (blocks 1..100), B (101..200), C (201..250).
	events := make([]Event, 0, 250)
	for n := uint64(1); n <= 100; n++ {
		events = append(events, Event{BlockNumber: n, TxHash: fmt.Sprintf("0xta%d", n), LogIndex: 0, Address: "0xA", Topics: []string{"0xT"}, Data: "0x"})
	}
	for n := uint64(101); n <= 200; n++ {
		events = append(events, Event{BlockNumber: n, TxHash: fmt.Sprintf("0xtb%d", n), LogIndex: 0, Address: "0xB", Topics: []string{"0xT"}, Data: "0x"})
	}
	for n := uint64(201); n <= 250; n++ {
		events = append(events, Event{BlockNumber: n, TxHash: fmt.Sprintf("0xtc%d", n), LogIndex: 0, Address: "0xC", Topics: []string{"0xT"}, Data: "0x"})
	}
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seen := make(map[string]bool, 250)
	pageSizes := []int{}
	var cursor *EventCursor
	for {
		filter := QueryFilter{Limit: 100, After: cursor}
		page, next, err := s.Query(ctx, filter)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		pageSizes = append(pageSizes, len(page))
		for _, e := range page {
			if seen[e.TxHash] {
				t.Fatalf("duplicate event %s", e.TxHash)
			}
			seen[e.TxHash] = true
		}
		if next == nil {
			break
		}
		cursor = next
	}
	wantSizes := []int{100, 100, 50}
	if len(pageSizes) != len(wantSizes) {
		t.Fatalf("page sizes = %v, want %v", pageSizes, wantSizes)
	}
	for i, want := range wantSizes {
		if pageSizes[i] != want {
			t.Fatalf("page %d size = %d, want %d", i, pageSizes[i], want)
		}
	}
	if len(seen) != 250 {
		t.Fatalf("saw %d events, want 250", len(seen))
	}
}

// TestQueryPostFilterCursorPagination guards against silent data loss when a
// topic1..3 post-filter strips enough rows from a fetched window that the
// page comes back shorter than `limit`. Pre-fix the loop bailed with
// `next_cursor: null` as soon as `len(out) <= limit`, hiding ~80% of matches
// past the first window. Post-fix the cursor anchors on the last raw row
// examined when SQL saturates, so iteration is exhaustive even at very low
// match selectivity.
func TestQueryPostFilterCursorPagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const total = 250
	const matchEvery = 5
	const want = total / matchEvery

	events := make([]Event, 0, total)
	wantHashes := make(map[string]bool, want)
	for n := uint64(1); n <= total; n++ {
		topic1 := "0xMISS"
		if n%matchEvery == 0 {
			topic1 = "0xHIT"
		}
		tx := fmt.Sprintf("0xt%d", n)
		events = append(events, Event{
			BlockNumber: n,
			TxHash:      tx,
			LogIndex:    0,
			Address:     "0xc",
			Topics:      []string{"0xT0", topic1},
			Data:        "0x",
		})
		if topic1 == "0xHIT" {
			wantHashes[tx] = true
		}
	}
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seen := make(map[string]bool, want)
	var cursor *EventCursor
	for iter := 0; iter < 100; iter++ {
		filter := QueryFilter{Topic1: strPtr("0xHIT"), Limit: 50, After: cursor}
		page, next, err := s.Query(ctx, filter)
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		for _, e := range page {
			if seen[e.TxHash] {
				t.Fatalf("duplicate event %s on iter %d", e.TxHash, iter)
			}
			if !wantHashes[e.TxHash] {
				t.Fatalf("returned non-matching event %s on iter %d", e.TxHash, iter)
			}
			seen[e.TxHash] = true
		}
		if next == nil {
			break
		}
		cursor = next
	}
	if len(seen) != want {
		t.Fatalf("saw %d matches, want %d (pagination dropped %d)", len(seen), want, want-len(seen))
	}
}

// TestQueryTopic0LikeWildcardNoEscape pins that a user-supplied % in topic0
// can't widen the prefix LIKE pattern into "any row with multiple topics".
// Pre-fix `?topic0=%` produced a SQL pattern of `%,%` that matched every
// multi-topic event regardless of its actual topic0.
func TestQueryTopic0LikeWildcardNoEscape(t *testing.T) {
	s := newTestStore(t)
	seedQueryEvents(t, s)
	ctx := context.Background()

	got, _, err := s.Query(ctx, QueryFilter{Topic0: strPtr("%")})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d events for topic0=%%, want 0 (LIKE metachars must be escaped)", len(got))
	}
}

func TestRewindToDeletesEventsAndHashesAndCheckpoint(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	events := make([]Event, 0, 10)
	for n := uint64(1); n <= 10; n++ {
		events = append(events, Event{
			BlockNumber: n,
			TxHash:      "0xtx",
			LogIndex:    uint(n),
			Address:     "0xc",
			Topics:      []string{"0xt"},
			Data:        "0x",
		})
	}
	if err := s.SaveEvents(ctx, defaultSrc, events); err != nil {
		t.Fatalf("save: %v", err)
	}
	for n := uint64(1); n <= 10; n++ {
		if err := s.RecordBlockHash(ctx, defaultSrc, n, "0xh", "0xp", 64); err != nil {
			t.Fatalf("record %d: %v", n, err)
		}
	}
	if err := s.SetCheckpoint(ctx, defaultSrc, 11); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if err := s.RewindTo(ctx, defaultSrc, 5); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE block_number >= 5`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Fatalf("events >= 5 remained: %d", n)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events all: %v", err)
	}
	if n != 4 {
		t.Fatalf("events count = %d, want 4 (blocks 1..4)", n)
	}

	recent, err := s.RecentBlockHashes(ctx, defaultSrc, 100)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(recent) != 4 {
		t.Fatalf("hashes count = %d, want 4 (blocks 1..4)", len(recent))
	}

	got, err := s.Checkpoint(ctx, defaultSrc)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got != 5 {
		t.Fatalf("checkpoint = %d, want 5", got)
	}
}

// TestSourceIsolation pins that two sources writing into the same DB don't
// leak rows into each other's queries / checkpoints / block_hashes. The new
// (source, …) PKs and the SQL pushdown of filter.Source carry the load.
func TestSourceIsolation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	aEvents := []Event{
		{BlockNumber: 10, TxHash: "0xta1", LogIndex: 0, Address: "0xA", Topics: []string{"0xT"}, Data: "0x"},
		{BlockNumber: 11, TxHash: "0xta2", LogIndex: 0, Address: "0xA", Topics: []string{"0xT"}, Data: "0x"},
	}
	bEvents := []Event{
		{BlockNumber: 10, TxHash: "0xtb1", LogIndex: 0, Address: "0xB", Topics: []string{"0xT"}, Data: "0x"},
		{BlockNumber: 12, TxHash: "0xtb2", LogIndex: 0, Address: "0xB", Topics: []string{"0xT"}, Data: "0x"},
	}
	if err := s.SaveEvents(ctx, "a", aEvents); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := s.SaveEvents(ctx, "b", bEvents); err != nil {
		t.Fatalf("save b: %v", err)
	}
	if err := s.SetCheckpoint(ctx, "a", 100); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.SetCheckpoint(ctx, "b", 200); err != nil {
		t.Fatalf("set b: %v", err)
	}

	src := "a"
	rows, _, err := s.Query(ctx, QueryFilter{Source: &src})
	if err != nil {
		t.Fatalf("query a: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("query a: %d rows, want 2", len(rows))
	}
	for _, e := range rows {
		if e.Source != "a" {
			t.Fatalf("query a returned source %q", e.Source)
		}
	}

	ca, _ := s.Checkpoint(ctx, "a")
	cb, _ := s.Checkpoint(ctx, "b")
	if ca != 100 || cb != 200 {
		t.Fatalf("checkpoints a=%d b=%d, want 100/200", ca, cb)
	}

	// Block hashes isolated: record block 10 under both sources with different hashes.
	if err := s.RecordBlockHash(ctx, "a", 10, "0xA10", "0xA9", 64); err != nil {
		t.Fatalf("record a: %v", err)
	}
	if err := s.RecordBlockHash(ctx, "b", 10, "0xB10", "0xB9", 64); err != nil {
		t.Fatalf("record b: %v", err)
	}
	ga, _, _ := s.BlockHashAt(ctx, "a", 10)
	gb, _, _ := s.BlockHashAt(ctx, "b", 10)
	if ga.Hash != "0xA10" || gb.Hash != "0xB10" {
		t.Fatalf("block hashes leaked: a=%s b=%s", ga.Hash, gb.Hash)
	}

	// RewindTo on "a" must not touch "b".
	if err := s.RewindTo(ctx, "a", 11); err != nil {
		t.Fatalf("rewind a: %v", err)
	}
	rowsAfter, _, _ := s.Query(ctx, QueryFilter{})
	srcCounts := map[string]int{}
	for _, e := range rowsAfter {
		srcCounts[e.Source]++
	}
	if srcCounts["b"] != 2 {
		t.Fatalf("source b lost rows after a-rewind: counts=%+v", srcCounts)
	}
}

func TestCheckpointsReturnsAll(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.SetCheckpoint(ctx, "a", 10); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.SetCheckpoint(ctx, "b", 20); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if err := s.SetCheckpoint(ctx, "c", 30); err != nil {
		t.Fatalf("set c: %v", err)
	}

	got, err := s.Checkpoints(ctx)
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}
	want := map[string]uint64{"a": 10, "b": 20, "c": 30}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("got[%q]=%d, want %d", k, got[k], v)
		}
	}
}

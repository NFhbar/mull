package store

import (
	"context"
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

func TestCheckpointRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	got, err := s.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got != 0 {
		t.Fatalf("empty store checkpoint = %d, want 0", got)
	}

	if err := s.SetCheckpoint(ctx, 100); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, _ := s.Checkpoint(ctx); got != 100 {
		t.Fatalf("after set: %d, want 100", got)
	}

	if err := s.SetCheckpoint(ctx, 250); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got, _ := s.Checkpoint(ctx); got != 250 {
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
	if err := s.SaveEvents(ctx, events); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Re-saving the same events must be a no-op (PRIMARY KEY conflict ignored).
	if err := s.SaveEvents(ctx, events); err != nil {
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
	if err := s.SaveEvents(context.Background(), nil); err != nil {
		t.Fatalf("empty save: %v", err)
	}
}

func TestBlockHashRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.RecordBlockHash(ctx, 10, "0xh10", "0xh9", 64); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordBlockHash(ctx, 11, "0xh11", "0xh10", 64); err != nil {
		t.Fatalf("record: %v", err)
	}

	got, ok, err := s.BlockHashAt(ctx, 10)
	if err != nil {
		t.Fatalf("BlockHashAt: %v", err)
	}
	if !ok || got.Number != 10 || got.Hash != "0xh10" || got.ParentHash != "0xh9" {
		t.Fatalf("BlockHashAt(10) = %+v, ok=%v", got, ok)
	}

	_, ok, err = s.BlockHashAt(ctx, 999)
	if err != nil {
		t.Fatalf("BlockHashAt missing: %v", err)
	}
	if ok {
		t.Fatal("missing block should return ok=false")
	}

	recent, err := s.RecentBlockHashes(ctx, 10)
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
		if err := s.RecordBlockHash(ctx, n, "h", "p", capDepth); err != nil {
			t.Fatalf("record %d: %v", n, err)
		}
	}
	recent, err := s.RecentBlockHashes(ctx, 20)
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
	if err := s.SaveEvents(ctx, events); err != nil {
		t.Fatalf("save: %v", err)
	}
	for n := uint64(1); n <= 10; n++ {
		if err := s.RecordBlockHash(ctx, n, "0xh", "0xp", 64); err != nil {
			t.Fatalf("record %d: %v", n, err)
		}
	}
	if err := s.SetCheckpoint(ctx, 11); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if err := s.RewindTo(ctx, 5); err != nil {
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

	recent, err := s.RecentBlockHashes(ctx, 100)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(recent) != 4 {
		t.Fatalf("hashes count = %d, want 4 (blocks 1..4)", len(recent))
	}

	got, err := s.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got != 5 {
		t.Fatalf("checkpoint = %d, want 5", got)
	}
}

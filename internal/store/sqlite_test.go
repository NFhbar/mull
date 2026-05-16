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

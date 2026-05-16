package indexer

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

type fakeRPC struct {
	head    uint64
	logsFor func(from, to uint64) []rpc.Log
}

func (f *fakeRPC) BlockNumber(context.Context) (uint64, error) { return f.head, nil }
func (f *fakeRPC) GetLogs(_ context.Context, from, to uint64, _ string, _ []string) ([]rpc.Log, error) {
	if f.logsFor == nil {
		return nil, nil
	}
	return f.logsFor(from, to), nil
}

type fakeStore struct {
	mu         sync.Mutex
	events     []store.Event
	checkpoint uint64
	ranges     [][2]uint64
}

func (s *fakeStore) SaveEvents(_ context.Context, events []store.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, events...)
	return nil
}
func (s *fakeStore) Checkpoint(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoint, nil
}
func (s *fakeStore) SetCheckpoint(_ context.Context, b uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = b
	return nil
}
func (s *fakeStore) Close() error { return nil }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCatchUpChunksAndCheckpoints(t *testing.T) {
	st := &fakeStore{}
	r := &fakeRPC{
		logsFor: func(from, to uint64) []rpc.Log {
			st.mu.Lock()
			st.ranges = append(st.ranges, [2]uint64{from, to})
			st.mu.Unlock()
			return []rpc.Log{{BlockNumber: from, TxHash: "0x", LogIndex: 0}}
		},
	}
	idx := New(r, st, Options{
		Contract:  "0xc",
		ChunkSize: 10,
		Logger:    quietLogger(),
	})

	next, err := idx.catchUp(context.Background(), 1, 25)
	if err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if next != 26 {
		t.Fatalf("next = %d, want 26", next)
	}
	want := [][2]uint64{{1, 10}, {11, 20}, {21, 25}}
	if len(st.ranges) != len(want) {
		t.Fatalf("ranges = %v, want %v", st.ranges, want)
	}
	for i, r := range st.ranges {
		if r != want[i] {
			t.Fatalf("range[%d] = %v, want %v", i, r, want[i])
		}
	}
	if st.checkpoint != 26 {
		t.Fatalf("checkpoint = %d, want 26", st.checkpoint)
	}
	if len(st.events) != 3 {
		t.Fatalf("events = %d, want 3", len(st.events))
	}
}

func TestCatchUpRespectsStartBlock(t *testing.T) {
	st := &fakeStore{}
	r := &fakeRPC{head: 5, logsFor: func(uint64, uint64) []rpc.Log { return nil }}
	idx := New(r, st, Options{
		ChunkSize:    100,
		StartBlock:   3,
		PollInterval: time.Hour,
		Logger:       quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- idx.Run(ctx) }()
	// Give the loop a beat to do one iteration, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if st.checkpoint != 6 {
		t.Fatalf("checkpoint = %d, want 6 (head+1)", st.checkpoint)
	}
}

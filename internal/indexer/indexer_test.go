package indexer

import (
	"context"
	"errors"
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

func TestCatchUpConcurrentOrderedCommits(t *testing.T) {
	// Gates per chunk; tests release them in a deliberately out-of-order
	// sequence to prove the committer holds back until next-expected lands.
	gates := map[uint64]chan struct{}{
		0:  make(chan struct{}),
		10: make(chan struct{}),
		20: make(chan struct{}),
		30: make(chan struct{}),
	}
	st := &fakeStore{}
	r := &fakeRPC{
		head: 39,
		logsFor: func(from, to uint64) []rpc.Log {
			gate, ok := gates[from]
			if !ok {
				t.Errorf("unexpected chunk from=%d", from)
				return nil
			}
			<-gate
			return []rpc.Log{{BlockNumber: from, TxHash: "0x", LogIndex: 0}}
		},
	}
	idx := New(r, st, Options{
		Contract:    "0xc",
		ChunkSize:   10,
		Concurrency: 4,
		Logger:      quietLogger(),
	})

	done := make(chan struct {
		next uint64
		err  error
	}, 1)
	go func() {
		n, err := idx.catchUp(context.Background(), 0, 39)
		done <- struct {
			next uint64
			err  error
		}{n, err}
	}()

	checkpointEq := func(want uint64, msg string) {
		t.Helper()
		// Allow a beat for the committer to drain after a gate release.
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			st.mu.Lock()
			got := st.checkpoint
			st.mu.Unlock()
			if got == want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		st.mu.Lock()
		got := st.checkpoint
		st.mu.Unlock()
		t.Fatalf("%s: checkpoint = %d, want %d", msg, got, want)
	}

	// Release [20,29] then [10,19]. Neither is next-expected so checkpoint
	// must stay at 0.
	close(gates[20])
	close(gates[10])
	time.Sleep(50 * time.Millisecond)
	st.mu.Lock()
	if st.checkpoint != 0 {
		st.mu.Unlock()
		t.Fatalf("checkpoint advanced before [0,9] released: %d", st.checkpoint)
	}
	st.mu.Unlock()

	// Releasing [0,9] should drain [0,9] → [10,19] → [20,29] in order →
	// checkpoint jumps to 30.
	close(gates[0])
	checkpointEq(30, "after [0,9] released")

	// Final chunk releases — checkpoint jumps to 40 and catchUp returns.
	close(gates[30])
	result := <-done
	if result.err != nil {
		t.Fatalf("catchUp: %v", result.err)
	}
	if result.next != 40 {
		t.Fatalf("next = %d, want 40", result.next)
	}
	if st.checkpoint != 40 {
		t.Fatalf("checkpoint = %d, want 40", st.checkpoint)
	}

	// Saved events must be in ascending block-number order (committer
	// serializes saves in chunk order).
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.events) != 4 {
		t.Fatalf("events = %d, want 4", len(st.events))
	}
	for i := 1; i < len(st.events); i++ {
		if st.events[i].BlockNumber < st.events[i-1].BlockNumber {
			t.Fatalf("events not monotonic: %d then %d", st.events[i-1].BlockNumber, st.events[i].BlockNumber)
		}
	}
}

func TestCatchUpConcurrentChunkFailureCancels(t *testing.T) {
	failErr := errors.New("synthetic rpc fail")
	st := &fakeStore{}
	r := &failingRPC{
		head:      29,
		failFrom:  10,
		failErr:   failErr,
		failDelay: 20 * time.Millisecond,
	}
	idx := New(r, st, Options{
		Contract:    "0xc",
		ChunkSize:   10,
		Concurrency: 3,
		Logger:      quietLogger(),
	})

	next, err := idx.catchUp(context.Background(), 0, 29)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, failErr) {
		t.Fatalf("error = %v, want wrap of %v", err, failErr)
	}
	if next > 10 {
		t.Fatalf("next = %d, want <= 10 (checkpoint must not cross failure)", next)
	}
	if st.checkpoint > 10 {
		t.Fatalf("checkpoint = %d, want <= 10", st.checkpoint)
	}
	// No events from chunks strictly after the failing range may be saved.
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range st.events {
		if e.BlockNumber >= 10 {
			t.Fatalf("event from block %d saved past failure boundary 10", e.BlockNumber)
		}
	}
}

type failingRPC struct {
	head      uint64
	failFrom  uint64
	failErr   error
	failDelay time.Duration
}

func (f *failingRPC) BlockNumber(context.Context) (uint64, error) { return f.head, nil }
func (f *failingRPC) GetLogs(ctx context.Context, from, to uint64, _ string, _ []string) ([]rpc.Log, error) {
	if from == f.failFrom {
		select {
		case <-time.After(f.failDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, f.failErr
	}
	return []rpc.Log{{BlockNumber: from, TxHash: "0x", LogIndex: 0}}, nil
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

package indexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

type fakeRPC struct {
	mu           sync.Mutex
	head         uint64
	logsFor      func(from, to uint64) []rpc.Log
	headers      map[uint64]rpc.Header
	headerByHash map[string]rpc.Header
	headHash     string
}

func (f *fakeRPC) BlockNumber(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head, nil
}
func (f *fakeRPC) GetLogs(_ context.Context, from, to uint64, _ string, _ []string) ([]rpc.Log, error) {
	f.mu.Lock()
	fn := f.logsFor
	f.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(from, to), nil
}
func (f *fakeRPC) BlockByNumber(_ context.Context, tag string) (rpc.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if tag == "latest" {
		if f.headHash != "" {
			if h, ok := f.headerByHash[f.headHash]; ok {
				return h, nil
			}
		}
		if h, ok := f.headers[f.head]; ok {
			return h, nil
		}
		return rpc.Header{Number: f.head, Hash: "0xh-" + rpc.HexUint64(f.head), ParentHash: "0xh-" + rpc.HexUint64(f.head-1)}, nil
	}
	n, err := rpc.ParseHexUint64(tag)
	if err != nil {
		return rpc.Header{}, fmt.Errorf("bad tag %q", tag)
	}
	if h, ok := f.headers[n]; ok {
		return h, nil
	}
	return rpc.Header{}, fmt.Errorf("block not found: %s", tag)
}
func (f *fakeRPC) BlockByHash(_ context.Context, hash string) (rpc.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.headerByHash[hash]; ok {
		return h, nil
	}
	for _, h := range f.headers {
		if h.Hash == hash {
			return h, nil
		}
	}
	return rpc.Header{}, fmt.Errorf("block not found: %s", hash)
}

type fakeStore struct {
	mu            sync.Mutex
	events        []store.Event
	checkpoint    uint64
	ranges        [][2]uint64
	saveOrder     []uint64    // block_number of events[0] for each SaveEvents call
	saveCh        chan uint64 // optional: signals on each SaveEvents call (tests that need synchronization)
	blockHashes   map[uint64]store.BlockHashEntry
	rewindToCalls []uint64
}

func (s *fakeStore) SaveEvents(_ context.Context, events []store.Event) error {
	s.mu.Lock()
	s.events = append(s.events, events...)
	var first uint64
	if len(events) > 0 {
		first = events[0].BlockNumber
		s.saveOrder = append(s.saveOrder, first)
	}
	ch := s.saveCh
	s.mu.Unlock()
	if ch != nil && len(events) > 0 {
		ch <- first
	}
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
func (s *fakeStore) RecordBlockHash(_ context.Context, number uint64, hash, parentHash string, capDepth uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blockHashes == nil {
		s.blockHashes = make(map[uint64]store.BlockHashEntry)
	}
	s.blockHashes[number] = store.BlockHashEntry{Number: number, Hash: hash, ParentHash: parentHash}
	if capDepth > 0 {
		var maxN uint64
		for n := range s.blockHashes {
			if n > maxN {
				maxN = n
			}
		}
		if maxN >= capDepth {
			cutoff := maxN - capDepth
			for n := range s.blockHashes {
				if n <= cutoff {
					delete(s.blockHashes, n)
				}
			}
		}
	}
	return nil
}

func (s *fakeStore) RecentBlockHashes(_ context.Context, limit uint64) ([]store.BlockHashEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nums := make([]uint64, 0, len(s.blockHashes))
	for n := range s.blockHashes {
		nums = append(nums, n)
	}
	// Descending sort.
	for i := 1; i < len(nums); i++ {
		for j := i; j > 0 && nums[j-1] < nums[j]; j-- {
			nums[j-1], nums[j] = nums[j], nums[j-1]
		}
	}
	if uint64(len(nums)) > limit {
		nums = nums[:limit]
	}
	out := make([]store.BlockHashEntry, 0, len(nums))
	for _, n := range nums {
		out = append(out, s.blockHashes[n])
	}
	return out, nil
}

func (s *fakeStore) BlockHashAt(_ context.Context, number uint64) (store.BlockHashEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.blockHashes[number]
	return e, ok, nil
}

func (s *fakeStore) RewindTo(_ context.Context, block uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rewindToCalls = append(s.rewindToCalls, block)
	kept := s.events[:0]
	for _, e := range s.events {
		if e.BlockNumber < block {
			kept = append(kept, e)
		}
	}
	s.events = kept
	for n := range s.blockHashes {
		if n >= block {
			delete(s.blockHashes, n)
		}
	}
	s.checkpoint = block
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
	saveCh := make(chan uint64, 4)
	st := &fakeStore{saveCh: saveCh}
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

	expectSave := func(want uint64) {
		t.Helper()
		select {
		case got := <-saveCh:
			if got != want {
				t.Fatalf("save out of order: got chunk from=%d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for save of chunk from=%d", want)
		}
	}
	expectNoSave := func(window time.Duration, when string) {
		t.Helper()
		select {
		case got := <-saveCh:
			t.Fatalf("unexpected save of chunk from=%d %s — committer did not hold back", got, when)
		case <-time.After(window):
		}
	}

	// Release [20,29] then [10,19]. Neither is next-expected so the
	// committer must hold both in pending and NOT call SaveEvents. The
	// signal-based negative check fires immediately on a stray save rather
	// than relying on a sleep being long enough.
	close(gates[20])
	close(gates[10])
	expectNoSave(100*time.Millisecond, "before [0,9] released")

	// Releasing [0,9] drains [0,9] → [10,19] → [20,29] in order. Each save
	// signal arrives as the committer makes the SaveEvents call.
	close(gates[0])
	expectSave(0)
	expectSave(10)
	expectSave(20)

	// Final chunk releases — fourth save fires and catchUp returns.
	close(gates[30])
	expectSave(30)
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

	st.mu.Lock()
	defer st.mu.Unlock()
	wantOrder := []uint64{0, 10, 20, 30}
	if len(st.saveOrder) != len(wantOrder) {
		t.Fatalf("saveOrder = %v, want %v", st.saveOrder, wantOrder)
	}
	for i, want := range wantOrder {
		if st.saveOrder[i] != want {
			t.Fatalf("saveOrder[%d] = %d, want %d", i, st.saveOrder[i], want)
		}
	}
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
func (f *failingRPC) BlockByNumber(_ context.Context, tag string) (rpc.Header, error) {
	return rpc.Header{Number: f.head, Hash: "0xh", ParentHash: "0xp"}, nil
}
func (f *failingRPC) BlockByHash(_ context.Context, hash string) (rpc.Header, error) {
	return rpc.Header{}, fmt.Errorf("block not found: %s", hash)
}
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
	r := &fakeRPC{
		head:    5,
		logsFor: func(uint64, uint64) []rpc.Log { return nil },
		headers: map[uint64]rpc.Header{
			5: {Number: 5, Hash: "0xh5", ParentHash: "0xh4"},
		},
	}
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

func newReorgIdx(t *testing.T, r *fakeRPC, st *fakeStore, depth uint64) *Indexer {
	t.Helper()
	return New(r, st, Options{
		Contract:    "0xc",
		ChunkSize:   10,
		Concurrency: 1,
		ReorgDepth:  depth,
		Logger:      quietLogger(),
	})
}

func TestReconcileHeadColdStart(t *testing.T) {
	st := &fakeStore{}
	r := &fakeRPC{
		headers: map[uint64]rpc.Header{
			10: {Number: 10, Hash: "0xh10", ParentHash: "0xh9"},
		},
		head: 10,
	}
	idx := newReorgIdx(t, r, st, 4)

	head, hint, err := idx.reconcileHead(context.Background())
	if err != nil {
		t.Fatalf("reconcileHead: %v", err)
	}
	if head != 10 || hint != 0 {
		t.Fatalf("head=%d hint=%d, want 10/0", head, hint)
	}
	if got, ok := st.blockHashes[10]; !ok || got.Hash != "0xh10" {
		t.Fatalf("seed = %+v ok=%v", got, ok)
	}
}

func TestReconcileHeadContiguousExtension(t *testing.T) {
	st := &fakeStore{
		blockHashes: map[uint64]store.BlockHashEntry{
			10: {Number: 10, Hash: "0xh10", ParentHash: "0xh9"},
		},
	}
	r := &fakeRPC{
		head: 11,
		headers: map[uint64]rpc.Header{
			10: {Number: 10, Hash: "0xh10", ParentHash: "0xh9"},
			11: {Number: 11, Hash: "0xh11", ParentHash: "0xh10"},
		},
	}
	idx := newReorgIdx(t, r, st, 4)

	head, hint, err := idx.reconcileHead(context.Background())
	if err != nil {
		t.Fatalf("reconcileHead: %v", err)
	}
	if head != 11 || hint != 0 {
		t.Fatalf("head=%d hint=%d, want 11/0", head, hint)
	}
	if got, ok := st.blockHashes[11]; !ok || got.Hash != "0xh11" {
		t.Fatalf("record = %+v ok=%v", got, ok)
	}
	if len(st.rewindToCalls) != 0 {
		t.Fatalf("rewind unexpectedly called: %v", st.rewindToCalls)
	}
}

func TestReconcileHeadDetectsAndRewindsShallowReorg(t *testing.T) {
	// Stored chain A: blocks 10..14, hash chain A.
	st := &fakeStore{
		blockHashes: map[uint64]store.BlockHashEntry{
			10: {Number: 10, Hash: "0xA10", ParentHash: "0xA9"},
			11: {Number: 11, Hash: "0xA11", ParentHash: "0xA10"},
			12: {Number: 12, Hash: "0xA12", ParentHash: "0xA11"},
			13: {Number: 13, Hash: "0xA13", ParentHash: "0xA12"},
			14: {Number: 14, Hash: "0xA14", ParentHash: "0xA13"},
		},
	}
	// Canonical chain B diverges at block 12: B12 has parent A11, then B13, B14.
	r := &fakeRPC{
		head: 14,
		headers: map[uint64]rpc.Header{
			14: {Number: 14, Hash: "0xB14", ParentHash: "0xB13"},
		},
		headerByHash: map[string]rpc.Header{
			"0xB13": {Number: 13, Hash: "0xB13", ParentHash: "0xB12"},
			"0xB12": {Number: 12, Hash: "0xB12", ParentHash: "0xA11"},
			"0xA11": {Number: 11, Hash: "0xA11", ParentHash: "0xA10"},
		},
	}
	idx := newReorgIdx(t, r, st, 16)

	head, hint, err := idx.reconcileHead(context.Background())
	if err != nil {
		t.Fatalf("reconcileHead: %v", err)
	}
	if head != 14 {
		t.Fatalf("head = %d, want 14", head)
	}
	if hint != 12 {
		t.Fatalf("cursorHint = %d, want 12 (ancestor+1)", hint)
	}
	if len(st.rewindToCalls) != 1 || st.rewindToCalls[0] != 12 {
		t.Fatalf("rewind calls = %v, want [12]", st.rewindToCalls)
	}
	// New canonical-B headers recorded.
	for n, want := range map[uint64]string{12: "0xB12", 13: "0xB13", 14: "0xB14"} {
		if got, ok := st.blockHashes[n]; !ok || got.Hash != want {
			t.Fatalf("block %d = %+v, want hash %s", n, got, want)
		}
	}
}

func TestReconcileHeadAbortsWhenDeeperThanDepth(t *testing.T) {
	st := &fakeStore{
		blockHashes: map[uint64]store.BlockHashEntry{
			10: {Number: 10, Hash: "0xA10", ParentHash: "0xA9"},
			11: {Number: 11, Hash: "0xA11", ParentHash: "0xA10"},
			12: {Number: 12, Hash: "0xA12", ParentHash: "0xA11"},
		},
	}
	// Chain B that doesn't converge within 3 walk steps.
	r := &fakeRPC{
		head: 12,
		headers: map[uint64]rpc.Header{
			12: {Number: 12, Hash: "0xB12", ParentHash: "0xB11"},
		},
		headerByHash: map[string]rpc.Header{
			"0xB11": {Number: 11, Hash: "0xB11", ParentHash: "0xB10"},
			"0xB10": {Number: 10, Hash: "0xB10", ParentHash: "0xB9"},
			"0xB9":  {Number: 9, Hash: "0xB9", ParentHash: "0xB8"},
		},
	}
	idx := newReorgIdx(t, r, st, 3)

	_, _, err := idx.reconcileHead(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "reorg deeper than reorg_depth=3") {
		t.Fatalf("err = %v, want 'reorg deeper than reorg_depth=3'", err)
	}
	if !strings.Contains(err.Error(), "head=12") {
		t.Fatalf("err = %v, want head=12 in message", err)
	}
	if !strings.Contains(err.Error(), "oldest_stored=") {
		t.Fatalf("err = %v, want oldest_stored= in message", err)
	}
	if len(st.rewindToCalls) != 0 {
		t.Fatalf("rewind called on deeper-than-depth: %v", st.rewindToCalls)
	}
}

func TestRunReindexesRewoundRangeAfterReorg(t *testing.T) {
	// Two-cycle controlled run with a chain mutation between cycles.
	st := &fakeStore{}
	// chain A header chain: 195..200 with hash An, parent A(n-1).
	headersA := map[uint64]rpc.Header{}
	for n := uint64(0); n <= 200; n++ {
		headersA[n] = rpc.Header{Number: n, Hash: fmt.Sprintf("0xA%d", n), ParentHash: fmt.Sprintf("0xA%d", n-1)}
	}
	r := &fakeRPC{
		head:         200,
		headers:      headersA,
		headerByHash: map[string]rpc.Header{},
	}
	for _, h := range headersA {
		r.headerByHash[h.Hash] = h
	}
	cycle := 0
	var cycleMu sync.Mutex
	r.logsFor = func(from, to uint64) []rpc.Log {
		cycleMu.Lock()
		c := cycle
		cycleMu.Unlock()
		out := make([]rpc.Log, 0, to-from+1)
		for n := from; n <= to; n++ {
			chain := "A"
			if c >= 1 && n >= 199 {
				chain = "B"
			}
			out = append(out, rpc.Log{
				BlockNumber: n,
				TxHash:      fmt.Sprintf("0x%s-%d", chain, n),
				LogIndex:    0,
			})
		}
		return out
	}
	// Pre-seed checkpoint at 195 so cycle 1 indexes 195..200.
	_ = st.SetCheckpoint(context.Background(), 195)
	// Pre-populate block_hashes so backfill no-ops.
	_ = st.RecordBlockHash(context.Background(), 194, "0xA194", "0xA193", 64)

	idx := New(r, st, Options{
		Contract:     "0xc",
		ChunkSize:    100,
		PollInterval: 5 * time.Millisecond,
		ReorgDepth:   16,
		Logger:       quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- idx.Run(ctx) }()

	// Wait for cycle 1 to complete: checkpoint reaches 201.
	if !waitFor(func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.checkpoint == 201
	}, 2*time.Second) {
		cancel()
		<-done
		t.Fatalf("cycle 1 did not reach checkpoint=201")
	}

	// Mutate to chain B for blocks 199..200, then bump cycle counter so logsFor returns B-events.
	r.mu.Lock()
	bHeaders := map[uint64]rpc.Header{
		199: {Number: 199, Hash: "0xB199", ParentHash: "0xA198"},
		200: {Number: 200, Hash: "0xB200", ParentHash: "0xB199"},
	}
	for n, h := range bHeaders {
		r.headers[n] = h
		r.headerByHash[h.Hash] = h
	}
	r.mu.Unlock()
	cycleMu.Lock()
	cycle = 1
	cycleMu.Unlock()

	// Wait for cycle 2: RewindTo(199) called, then checkpoint back to 201 with chain-B events.
	if !waitFor(func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		if len(st.rewindToCalls) == 0 {
			return false
		}
		if st.rewindToCalls[0] != 199 {
			return false
		}
		if st.checkpoint != 201 {
			return false
		}
		for _, e := range st.events {
			if e.BlockNumber >= 199 && !strings.HasPrefix(e.TxHash, "0xB") {
				return false
			}
		}
		return true
	}, 3*time.Second) {
		cancel()
		<-done
		st.mu.Lock()
		t.Fatalf("cycle 2 did not converge; rewinds=%v checkpoint=%d events=%v", st.rewindToCalls, st.checkpoint, txHashes(st.events))
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestBackfillBlockHashesOnColdResume(t *testing.T) {
	st := &fakeStore{checkpoint: 200}
	headers := map[uint64]rpc.Header{}
	for n := uint64(0); n <= 200; n++ {
		headers[n] = rpc.Header{Number: n, Hash: fmt.Sprintf("0xH%d", n), ParentHash: fmt.Sprintf("0xH%d", n-1)}
	}
	r := &fakeRPC{head: 200, headers: headers}

	idx := New(r, st, Options{
		Contract:     "0xc",
		ChunkSize:    100,
		PollInterval: time.Hour,
		ReorgDepth:   64,
		Logger:       quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := idx.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Should have backfilled blocks 137..199 (200 - 64 to 200 - 1).
	for n := uint64(137); n <= 199; n++ {
		if _, ok := st.blockHashes[n]; !ok {
			t.Fatalf("missing backfilled hash for block %d", n)
		}
	}
	if _, ok := st.blockHashes[136]; ok {
		t.Fatalf("backfill went too deep — block 136 should not be present")
	}
}

// TestRunMakesProgressWhenStartBlockFarBehindHead exercises the deep-gap
// startup path end-to-end: cursor begins more than reorg_depth blocks behind
// head, so backfill is skipped in Run and reconcileHead re-anchors on head
// (covering both the cold-start branch via the first iteration AND the
// warm-restart re-anchor branch via a pre-seeded stale entry). Run must make
// forward progress, never error with "reorg deeper than reorg_depth".
func TestRunMakesProgressWhenStartBlockFarBehindHead(t *testing.T) {
	// Pre-seed a stale block_hashes entry to simulate a warm restart where
	// the indexer was offline long enough for head to outrun reorg_depth.
	// This forces reconcileHead into the new re-anchor branch (recent is
	// non-empty but newHead.Number > recent[0].Number + reorg_depth).
	st := &fakeStore{
		checkpoint: 30,
		blockHashes: map[uint64]store.BlockHashEntry{
			29: {Number: 29, Hash: "0xH29", ParentHash: "0xH28"},
		},
	}
	headers := map[uint64]rpc.Header{}
	for n := uint64(0); n <= 200; n++ {
		headers[n] = rpc.Header{Number: n, Hash: fmt.Sprintf("0xH%d", n), ParentHash: fmt.Sprintf("0xH%d", n-1)}
	}
	r := &fakeRPC{
		head:    200,
		headers: headers,
		logsFor: func(from, to uint64) []rpc.Log {
			return []rpc.Log{{BlockNumber: from, TxHash: fmt.Sprintf("0xtx-%d", from), LogIndex: 0}}
		},
	}
	idx := New(r, st, Options{
		Contract:     "0xc",
		ChunkSize:    50,
		StartBlock:   10,
		PollInterval: 5 * time.Millisecond,
		ReorgDepth:   16,
		Logger:       quietLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- idx.Run(ctx) }()

	// Forward progress = checkpoint advances past the starting cursor (30).
	// If reconcileHead aborts with "reorg deeper than reorg_depth", Run
	// returns immediately and checkpoint stays at 30.
	progressed := waitFor(func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.checkpoint > 30
	}, 2*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !progressed {
		t.Fatalf("checkpoint did not advance past 30 (checkpoint=%d) — Run aborted on deep gap", st.checkpoint)
	}
}

// TestBackfillBlockHashesSwallowsMidWalkRPCError covers the
// `log.Warn(...); return nil` path in backfillBlockHashes: when an
// intermediate BlockByNumber call fails, backfill records what it has so far
// and returns nil rather than propagating. Reorg detection then warms up
// once the indexer catches up to within reorg_depth of head.
func TestBackfillBlockHashesSwallowsMidWalkRPCError(t *testing.T) {
	st := &fakeStore{}
	headers := map[uint64]rpc.Header{}
	// Walk for cursor=100 covers 92..99. Populate 92..98 so they record
	// successfully; leave 99 missing so the fakeRPC returns "block not found"
	// — the error backfill must swallow.
	for n := uint64(92); n <= 98; n++ {
		headers[n] = rpc.Header{Number: n, Hash: fmt.Sprintf("0xH%d", n), ParentHash: fmt.Sprintf("0xH%d", n-1)}
	}
	r := &fakeRPC{head: 100, headers: headers}

	idx := New(r, st, Options{
		Contract:   "0xc",
		ReorgDepth: 8,
		Logger:     quietLogger(),
	})

	if err := idx.backfillBlockHashes(context.Background(), 100); err != nil {
		t.Fatalf("backfill must swallow mid-walk error, got: %v", err)
	}
	for n := uint64(92); n <= 98; n++ {
		if _, ok := st.blockHashes[n]; !ok {
			t.Fatalf("expected backfilled hash at block %d before the failure", n)
		}
	}
	if _, ok := st.blockHashes[99]; ok {
		t.Fatalf("block 99 lookup failed — should not have been recorded")
	}
}

type fakeSink struct {
	id      string
	topic0  string
	mu      sync.Mutex
	handled []store.Event
	failOn  string
	failErr error
}

func (s *fakeSink) SinkID() string { return s.id }
func (s *fakeSink) Topic0() string { return s.topic0 }
func (s *fakeSink) Handle(_ context.Context, e store.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failErr != nil && e.TxHash == s.failOn {
		return s.failErr
	}
	s.handled = append(s.handled, e)
	return nil
}

func TestRun_FansOutToSinks(t *testing.T) {
	st := &fakeStore{}
	r := &fakeRPC{
		logsFor: func(from, to uint64) []rpc.Log {
			return []rpc.Log{
				{BlockNumber: from, TxHash: fmt.Sprintf("0xtx-%d", from), LogIndex: 0},
			}
		},
	}
	sinkA := &fakeSink{id: "a"}
	sinkB := &fakeSink{id: "b"}
	idx := New(r, st, Options{
		Contract:  "0xc",
		ChunkSize: 10,
		Logger:    quietLogger(),
		Sinks:     []store.EventSink{sinkA, sinkB},
	})

	next, err := idx.catchUp(context.Background(), 1, 25)
	if err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	if next != 26 {
		t.Fatalf("next = %d, want 26", next)
	}

	// Three chunks → three events; each event flows through both sinks.
	for _, s := range []*fakeSink{sinkA, sinkB} {
		s.mu.Lock()
		got := len(s.handled)
		s.mu.Unlock()
		if got != 3 {
			t.Fatalf("sink %s handled %d events, want 3", s.id, got)
		}
	}
}

func TestRun_SinkErrorAbortsRun(t *testing.T) {
	st := &fakeStore{}
	r := &fakeRPC{
		logsFor: func(from, to uint64) []rpc.Log {
			return []rpc.Log{
				{BlockNumber: from, TxHash: fmt.Sprintf("0xtx-%d", from), LogIndex: 0},
			}
		},
	}
	sinkErr := errors.New("sink boom")
	sink := &fakeSink{id: "boom", failOn: "0xtx-11", failErr: sinkErr}
	idx := New(r, st, Options{
		Contract:  "0xc",
		ChunkSize: 10,
		Logger:    quietLogger(),
		Sinks:     []store.EventSink{sink},
	})

	_, err := idx.catchUp(context.Background(), 1, 25)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sinkErr) {
		t.Fatalf("err = %v, want wrap of %v", err, sinkErr)
	}
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func txHashes(events []store.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, fmt.Sprintf("%d:%s", e.BlockNumber, e.TxHash))
	}
	return out
}

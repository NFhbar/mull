package indexer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

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
	mu sync.Mutex
	events []store.Event
	checkpoint uint64
	// checkpointSource captures the source string the indexer-under-test
	// passes to SetCheckpoint, so Checkpoints can report the real key
	// instead of a hard-coded "test" — matches the real sqlite.go
	// contract where rows determine the map.
	checkpointSource string
	ranges [][2]uint64
	saveOrder []uint64
	saveCh chan uint64
	blockHashes map[uint64]store.BlockHashEntry
	rewindToCalls []uint64
}

func (s *fakeStore) SaveEvents(_ context.Context, _ string, events []store.Event) error {
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
func (s *fakeStore) Checkpoint(_ context.Context, _ string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoint, nil
}
func (s *fakeStore) SetCheckpoint(_ context.Context, source string, b uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = b
	s.checkpointSource = source
	return nil
}
func (s *fakeStore) Checkpoints(context.Context) (map[string]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Mirror sqlite.Checkpoints: empty map until at least one SetCheckpoint
	// row has been written.
	if s.checkpointSource == "" {
		return map[string]uint64{}, nil
	}
	return map[string]uint64{s.checkpointSource: s.checkpoint}, nil
}
func (s *fakeStore) RecordBlockHash(_ context.Context, _ string, number uint64, hash, parentHash string, capDepth uint64) error {
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

func (s *fakeStore) RecentBlockHashes(_ context.Context, _ string, limit uint64) ([]store.BlockHashEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nums := make([]uint64, 0, len(s.blockHashes))
	for n := range s.blockHashes {
		nums = append(nums, n)
	}
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

func (s *fakeStore) BlockHashAt(_ context.Context, _ string, number uint64) (store.BlockHashEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.blockHashes[number]
	return e, ok, nil
}

func (s *fakeStore) RewindTo(_ context.Context, _ string, block uint64) error {
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

func (s *fakeStore) Query(context.Context, store.QueryFilter) ([]store.Event, *store.EventCursor, error) {
	return nil, nil, nil
}

func (s *fakeStore) Close() error { return nil }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// callReconcileHead is a test helper: fetches the canonical head from the
// fake RPC the same way Indexer.Run does (head source's Latest call) and
// passes it to the parameterised reconcileHead. Centralised so test wiring
// matches Run's wiring as the interface evolves.
func callReconcileHead(t interface {
	Fatalf(string, ...any)
	Helper()
}, idx *Indexer, r *fakeRPC) (uint64, uint64, error) {
	t.Helper()
	newHead, err := r.BlockByNumber(context.Background(), "latest")
	if err != nil {
		t.Fatalf("fakeRPC.BlockByNumber(latest): %v", err)
	}
	return idx.reconcileHead(context.Background(), newHead)
}

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/NFhbar/mull/internal/indexer"
	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

// stubRPC is a minimal in-memory rpc.Client. Returns a fixed head + headers
// and no logs — enough for indexer.Run to drive a single catch-up to head
// and advance the per-source checkpoint to head+1.
type stubRPC struct {
	mu      sync.Mutex
	head    uint64
	headers map[uint64]rpc.Header
}

func (s *stubRPC) BlockNumber(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head, nil
}

func (s *stubRPC) GetLogs(context.Context, uint64, uint64, string, []string) ([]rpc.Log, error) {
	return nil, nil
}

func (s *stubRPC) BlockByNumber(_ context.Context, tag string) (rpc.Header, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tag == "latest" {
		if h, ok := s.headers[s.head]; ok {
			return h, nil
		}
		return rpc.Header{}, fmt.Errorf("no head header: %d", s.head)
	}
	n, err := rpc.ParseHexUint64(tag)
	if err != nil {
		return rpc.Header{}, fmt.Errorf("bad tag %q", tag)
	}
	if h, ok := s.headers[n]; ok {
		return h, nil
	}
	return rpc.Header{}, fmt.Errorf("block not found: %s", tag)
}

func (s *stubRPC) BlockByHash(_ context.Context, hash string) (rpc.Header, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.headers {
		if h.Hash == hash {
			return h, nil
		}
	}
	return rpc.Header{}, fmt.Errorf("block not found: %s", hash)
}

// TestRunIndex_MultiSourceErrgroupCoordinatesShutdownAndIsolatesCheckpoints
// covers the load-bearing addition of the multi-source rewrite: two real
// Indexer instances wired under one errgroup, just like runIndex. Asserts
//
//   - (a) per-source checkpoints diverge — each source writes its own row
//     and one source's progress doesn't contaminate the other's, and
//   - (b) parent ctx cancel returns from g.Wait() within a bounded
//     duration — the coordinated-shutdown contract runIndex relies on.
//
// The test wires the same errgroup pattern runIndex uses (rather than
// invoking runIndex itself, which would require a config-file + on-disk
// fixture) so a regression in the orchestration shape would surface here.
func TestRunIndex_MultiSourceErrgroupCoordinatesShutdownAndIsolatesCheckpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmp := t.TempDir()
	st, err := store.OpenSQLite(ctx, filepath.Join(tmp, "mull.db"))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	type srcSpec struct {
		name       string
		head       uint64
		startBlock uint64
	}
	specs := []srcSpec{
		{"src_a", 5, 3},
		{"src_b", 10, 7},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	g, gctx := errgroup.WithContext(ctx)
	for _, sp := range specs {
		sp := sp
		headers := make(map[uint64]rpc.Header, sp.head+1)
		for i := uint64(0); i <= sp.head; i++ {
			headers[i] = rpc.Header{
				Number:     i,
				Hash:       fmt.Sprintf("0x%s-%d", sp.name, i),
				ParentHash: fmt.Sprintf("0x%s-%d", sp.name, i-1),
			}
		}
		client := &stubRPC{head: sp.head, headers: headers}
		idx := indexer.New(client, st, indexer.Options{
			Source:       sp.name,
			ChunkSize:    100,
			StartBlock:   sp.startBlock,
			PollInterval: time.Hour, // first tick never fires; cancel stops Run
			Logger:       logger,
		})
		g.Go(func() error {
			err := idx.Run(gctx)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
	}

	// Let each indexer complete its initial catch-up (a single cycle is enough
	// to drive SetCheckpoint to head+1) before cancelling.
	if !waitForCondition(200*time.Millisecond, func() bool {
		cps, err := st.Checkpoints(context.Background())
		if err != nil {
			return false
		}
		return cps["src_a"] == 6 && cps["src_b"] == 11
	}) {
		// Don't fail yet — checkpoint assertion below produces a clearer message.
	}

	// (b) Cancel parent ctx; assert g.Wait() returns within a bounded duration.
	cancel()
	waitErr := make(chan error, 1)
	go func() { waitErr <- g.Wait() }()
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("g.Wait after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("g.Wait did not return within 2s of ctx cancel — coordinated shutdown broken")
	}

	// (a) Per-source checkpoints diverged: each indexer wrote head+1 under its
	// own source row, with no cross-contamination.
	cps, err := st.Checkpoints(context.Background())
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}
	if got, want := cps["src_a"], uint64(6); got != want {
		t.Errorf("src_a checkpoint = %d, want %d (head+1)", got, want)
	}
	if got, want := cps["src_b"], uint64(11); got != want {
		t.Errorf("src_b checkpoint = %d, want %d (head+1)", got, want)
	}
	if cps["src_a"] == cps["src_b"] {
		t.Errorf("per-source checkpoints did not diverge: src_a=%d src_b=%d", cps["src_a"], cps["src_b"])
	}
}

// waitForCondition polls cond() at 10ms cadence up to timeout. Returns true
// when cond returns true, false on timeout. Used to wait for asynchronous
// goroutine progress without a fixed sleep.
func waitForCondition(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

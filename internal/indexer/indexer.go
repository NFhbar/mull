package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

type Options struct {
	Contract     string
	Topics       []string
	ChunkSize    uint64
	PollInterval time.Duration
	StartBlock   uint64
	Concurrency  int
	ReorgDepth   uint64
	Logger       *slog.Logger
}

type Indexer struct {
	rpc          rpc.Client
	store        store.Store
	contract     string
	topics       []string
	chunkSize    uint64
	pollInterval time.Duration
	startBlock   uint64
	concurrency  int
	reorgDepth   uint64
	log          *slog.Logger
}

func New(client rpc.Client, st store.Store, opts Options) *Indexer {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	concurrency := opts.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	reorgDepth := opts.ReorgDepth
	if reorgDepth == 0 {
		reorgDepth = 64
	}
	return &Indexer{
		rpc:          client,
		store:        st,
		contract:     opts.Contract,
		topics:       opts.Topics,
		chunkSize:    opts.ChunkSize,
		pollInterval: opts.PollInterval,
		startBlock:   opts.StartBlock,
		concurrency:  concurrency,
		reorgDepth:   reorgDepth,
		log:          logger.With("contract", opts.Contract),
	}
}

// Run polls the chain head and indexes logs in chunked block ranges
// until ctx is cancelled. The checkpoint is the next block to index.
func (i *Indexer) Run(ctx context.Context) error {
	cursor, err := i.store.Checkpoint(ctx)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	if cursor < i.startBlock {
		cursor = i.startBlock
	}
	i.log.Info("indexer starting", "from_block", cursor)

	if err := i.backfillBlockHashes(ctx, cursor); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	ticker := time.NewTicker(i.pollInterval)
	defer ticker.Stop()

	for {
		head, cursorHint, err := i.reconcileHead(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if cursorHint != 0 {
			cursor = cursorHint
			if cursor < i.startBlock {
				cursor = i.startBlock
			}
		}
		cursor, err = i.catchUp(ctx, cursor, head)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			i.log.Info("indexer stopping", "next_block", cursor)
			return nil
		case <-ticker.C:
		}
	}
}

// reconcileHead fetches the canonical head, walks back via parent-hash to find
// a common ancestor with stored hashes, and rewinds the store on divergence.
// cursorHint is non-zero only when a rewind occurred; the caller must replace
// its in-memory cursor with cursorHint so catchUp re-fetches the rewound range.
func (i *Indexer) reconcileHead(ctx context.Context) (head uint64, cursorHint uint64, err error) {
	newHead, err := i.rpc.BlockByNumber(ctx, "latest")
	if err != nil {
		return 0, 0, fmt.Errorf("fetch head header: %w", err)
	}

	recent, err := i.store.RecentBlockHashes(ctx, 1)
	if err != nil {
		return 0, 0, fmt.Errorf("read recent hashes: %w", err)
	}
	if len(recent) == 0 {
		if err := i.store.RecordBlockHash(ctx, newHead.Number, newHead.Hash, newHead.ParentHash, i.reorgDepth); err != nil {
			return 0, 0, fmt.Errorf("seed head hash: %w", err)
		}
		return newHead.Number, 0, nil
	}

	walked := []rpc.Header{newHead}
	cur := newHead
	var ancestor *rpc.Header
	for steps := uint64(0); steps < i.reorgDepth; steps++ {
		stored, ok, err := i.store.BlockHashAt(ctx, cur.Number)
		if err != nil {
			return 0, 0, fmt.Errorf("lookup stored hash: %w", err)
		}
		if ok && stored.Hash == cur.Hash {
			a := cur
			ancestor = &a
			break
		}
		if cur.Number == 0 {
			a := cur
			ancestor = &a
			break
		}
		if cur.ParentHash == "" {
			return 0, 0, fmt.Errorf("walk: empty parentHash at block %d", cur.Number)
		}
		parent, err := i.rpc.BlockByHash(ctx, cur.ParentHash)
		if err != nil {
			return 0, 0, fmt.Errorf("walk back from %d: %w", cur.Number, err)
		}
		cur = parent
		walked = append(walked, cur)
	}
	if ancestor == nil {
		return 0, 0, fmt.Errorf("reorg deeper than reorg_depth=%d (head=%d, oldest_stored=%d)", i.reorgDepth, newHead.Number, recent[0].Number)
	}

	mostRecent := recent[0]
	if mostRecent.Number > ancestor.Number {
		rewindTarget := ancestor.Number + 1
		if err := i.store.RewindTo(ctx, rewindTarget); err != nil {
			return 0, 0, fmt.Errorf("rewind to %d: %w", rewindTarget, err)
		}
		cursorHint = rewindTarget
		i.log.Warn("reorg detected", "ancestor", ancestor.Number, "rewind_to", rewindTarget, "depth", mostRecent.Number-ancestor.Number)
	}

	for _, h := range walked {
		if err := i.store.RecordBlockHash(ctx, h.Number, h.Hash, h.ParentHash, i.reorgDepth); err != nil {
			return 0, 0, fmt.Errorf("record hash %d: %w", h.Number, err)
		}
	}
	return newHead.Number, cursorHint, nil
}

// backfillBlockHashes seeds block_hashes with the last reorg_depth canonical
// headers behind the current cursor, so the first reconcile after a cold
// resume has something to anchor against. No-op when block_hashes already has
// entries or when the indexer hasn't indexed anything yet (cursor == 0).
func (i *Indexer) backfillBlockHashes(ctx context.Context, cursor uint64) error {
	recent, err := i.store.RecentBlockHashes(ctx, 1)
	if err != nil {
		return fmt.Errorf("backfill: read recent: %w", err)
	}
	if len(recent) > 0 {
		return nil
	}
	if cursor == 0 {
		return nil
	}

	end := cursor - 1
	var start uint64
	if end >= i.reorgDepth {
		start = end - i.reorgDepth + 1
	}
	for n := start; n <= end; n++ {
		h, err := i.rpc.BlockByNumber(ctx, hexUint64(n))
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			i.log.Warn("backfill block_hashes failed; reorg detection will warm up over time", "block", n, "err", err)
			return nil
		}
		if err := i.store.RecordBlockHash(ctx, h.Number, h.Hash, h.ParentHash, i.reorgDepth); err != nil {
			return fmt.Errorf("backfill: record hash %d: %w", n, err)
		}
	}
	i.log.Info("block_hashes backfilled for reorg detection", "from", start, "to", end, "count", end-start+1)
	return nil
}

func hexUint64(n uint64) string {
	return fmt.Sprintf("0x%x", n)
}

type chunkRange struct {
	from, to uint64
}

type chunkResult struct {
	from      uint64
	to        uint64
	events    []store.Event
	startAt   time.Time
	fetchedAt time.Time
}

func (i *Indexer) catchUp(ctx context.Context, from, head uint64) (uint64, error) {
	if from > head {
		return from, nil
	}
	ranges := make([]chunkRange, 0)
	for f := from; f <= head; f += i.chunkSize {
		t := f + i.chunkSize - 1
		if t > head {
			t = head
		}
		ranges = append(ranges, chunkRange{from: f, to: t})
	}
	return i.runScheduler(ctx, from, head, ranges)
}

// runScheduler fetches chunk ranges concurrently with up to i.concurrency
// workers and commits results in deterministic ascending order. The returned
// uint64 is the next block to index — equal to the original from when no
// chunk has committed, or to lastCommittedTo+1 after partial progress on
// failure. Invariant: on return, every block strictly less than the returned
// value is durably saved + checkpointed.
func (i *Indexer) runScheduler(ctx context.Context, from, head uint64, ranges []chunkRange) (uint64, error) {
	g, gctx := errgroup.WithContext(ctx)

	jobs := make(chan chunkRange)
	results := make(chan chunkResult)

	g.Go(func() error {
		defer close(jobs)
		for _, r := range ranges {
			select {
			case jobs <- r:
			case <-gctx.Done():
				return nil
			}
		}
		return nil
	})

	workers, wctx := errgroup.WithContext(gctx)
	for w := 0; w < i.concurrency; w++ {
		workers.Go(func() error {
			for r := range jobs {
				start := time.Now()
				logs, err := i.rpc.GetLogs(wctx, r.from, r.to, i.contract, i.topics)
				if err != nil {
					return fmt.Errorf("get logs [%d,%d]: %w", r.from, r.to, err)
				}
				select {
				case results <- chunkResult{from: r.from, to: r.to, events: toEvents(logs), startAt: start, fetchedAt: time.Now()}:
				case <-wctx.Done():
					return wctx.Err()
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		err := workers.Wait()
		close(results)
		return err
	})

	committed := from
	g.Go(func() error {
		nextExpected := from
		pending := make(map[uint64]chunkResult)
		for res := range results {
			pending[res.from] = res
			for {
				ready, ok := pending[nextExpected]
				if !ok {
					break
				}
				if err := i.store.SaveEvents(gctx, ready.events); err != nil {
					return fmt.Errorf("save events: %w", err)
				}
				next := ready.to + 1
				if err := i.store.SetCheckpoint(gctx, next); err != nil {
					return fmt.Errorf("set checkpoint: %w", err)
				}
				i.log.Info("indexed range",
					"from", ready.from,
					"to", ready.to,
					"events", len(ready.events),
					"fetch_ms", ready.fetchedAt.Sub(ready.startAt).Milliseconds(),
					"commit_lag_ms", time.Since(ready.fetchedAt).Milliseconds(),
					"lag_blocks", head-ready.to,
				)
				delete(pending, nextExpected)
				nextExpected = next
				committed = next
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return committed, ctx.Err()
		}
		return committed, err
	}
	return committed, nil
}

func toEvents(logs []rpc.Log) []store.Event {
	out := make([]store.Event, len(logs))
	for i, l := range logs {
		out[i] = store.Event{
			BlockNumber: l.BlockNumber,
			TxHash:      l.TxHash,
			LogIndex:    l.LogIndex,
			Address:     l.Address,
			Topics:      l.Topics,
			Data:        l.Data,
		}
	}
	return out
}

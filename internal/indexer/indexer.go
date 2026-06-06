package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"

	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

type Options struct {
	// Source is the per-source key threaded through every Store call. Required:
	// New panics on an empty Source so an accidental zero-value Options never
	// silently writes rows under source = "" alongside real sources.
	Source       string
	Contract     string
	Topics       []string
	ChunkSize    uint64
	PollInterval time.Duration
	StartBlock   uint64
	Concurrency  int
	ReorgDepth   uint64
	Logger       *slog.Logger
	Sinks        []store.EventSink
	// HeadSource overrides how Run learns about new chain heads. When nil, New
	// constructs a PollingHeadSource wrapping the supplied client + PollInterval
	// — preserves today's behaviour for in-process callers that haven't
	// migrated.
	HeadSource HeadSource
}

type Indexer struct {
	rpc      rpc.Client
	store    store.Store
	source   string
	contract string
	topics   []string
	chunkSize uint64
	// TODO(head-source): consumed only by the default PollingHeadSource
	// constructor — promote into PollingHeadSource and drop from Indexer
	// once Options callers migrate.
	pollInterval time.Duration
	startBlock   uint64
	concurrency  int
	reorgDepth   uint64
	head         HeadSource
	log          *slog.Logger
	// sinksByTopic0 dispatches events in O(1) by their first topic. Built once
	// in New from opts.Sinks; the canonical key is common.HexToHash(topic0).Hex()
	// so casing differences between RPC providers can't cause silent no-ops.
	sinksByTopic0 map[string][]store.EventSink
	// wildcardSinks receive every event (used by sinks that return an empty
	// Topic0(), e.g. test fakes that want to observe everything).
	wildcardSinks []store.EventSink
	// allSinks preserves opts.Sinks in registration order. rewindSinks walks
	// this so partial-failure residue is deterministic across runs — map
	// iteration over sinksByTopic0 is not.
	allSinks []store.EventSink
}

func New(client rpc.Client, st store.Store, opts Options) *Indexer {
	if opts.Source == "" {
		panic("indexer.New: opts.Source is required")
	}
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
	sinksByTopic0 := make(map[string][]store.EventSink)
	var wildcardSinks []store.EventSink
	allSinks := make([]store.EventSink, len(opts.Sinks))
	copy(allSinks, opts.Sinks)
	for _, s := range opts.Sinks {
		t := s.Topic0()
		if t == "" {
			wildcardSinks = append(wildcardSinks, s)
			continue
		}
		key := common.HexToHash(t).Hex()
		sinksByTopic0[key] = append(sinksByTopic0[key], s)
	}
	head := opts.HeadSource
	if head == nil {
		head = &PollingHeadSource{Client: client, PollInterval: opts.PollInterval}
	}
	return &Indexer{
		rpc:           client,
		store:         st,
		source:        opts.Source,
		contract:      opts.Contract,
		topics:        opts.Topics,
		chunkSize:     opts.ChunkSize,
		pollInterval:  opts.PollInterval,
		startBlock:    opts.StartBlock,
		concurrency:   concurrency,
		reorgDepth:    reorgDepth,
		head:          head,
		log:           logger.With("source", opts.Source, "contract", opts.Contract),
		sinksByTopic0: sinksByTopic0,
		wildcardSinks: wildcardSinks,
		allSinks:      allSinks,
	}
}

// Run drives the indexer until ctx is cancelled or the head source closes its
// channel. Cold-start: peek at head via HeadSource.Latest to decide whether to
// backfill block_hashes. Steady-state: each head delivered on the subscription
// channel drives one reconcileHead+catchUp cycle. The checkpoint is the next
// block to index.
func (i *Indexer) Run(ctx context.Context) error {
	cursor, err := i.store.Checkpoint(ctx, i.source)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	if cursor < i.startBlock {
		cursor = i.startBlock
	}
	i.log.Info("indexer starting", "from_block", cursor)

	// Peek at head once so we can skip the backfill when the cursor is more
	// than reorg_depth blocks behind: in that case the first reconcileHead
	// will re-anchor on head and evict any backfilled entries via the cap,
	// so the backfill's RPC calls would be wasted.
	startHead, err := i.head.Latest(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("fetch initial head: %w", err)
	}
	if startHead.Number <= cursor+i.reorgDepth {
		if err := i.backfillBlockHashes(ctx, cursor); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	} else {
		i.log.Info("skipping block_hashes backfill — cursor more than reorg_depth behind head",
			"cursor", cursor, "head", startHead.Number, "reorg_depth", i.reorgDepth)
	}

	headCh, err := i.head.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe to head: %w", err)
	}

	for newHead := range headCh {
		head, cursorHint, err := i.reconcileHead(ctx, newHead)
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
	}
	i.log.Info("indexer stopping", "next_block", cursor)
	return nil
}

// reconcileHead walks back from the supplied head via parent-hash to find a
// common ancestor with stored hashes, and rewinds the store on divergence.
// cursorHint is non-zero only when a rewind occurred; the caller must replace
// its in-memory cursor with cursorHint so catchUp re-fetches the rewound range.
func (i *Indexer) reconcileHead(ctx context.Context, newHead rpc.Header) (head uint64, cursorHint uint64, err error) {
	recent, err := i.store.RecentBlockHashes(ctx, i.source, 1)
	if err != nil {
		return 0, 0, fmt.Errorf("read recent hashes: %w", err)
	}
	if len(recent) == 0 {
		if err := i.store.RecordBlockHash(ctx, i.source, newHead.Number, newHead.Hash, newHead.ParentHash, i.reorgDepth); err != nil {
			return 0, 0, fmt.Errorf("seed head hash: %w", err)
		}
		return newHead.Number, 0, nil
	}

	// Gap larger than reorg_depth: a parent-hash walk can't reach the stored
	// range in i.reorgDepth steps, so reorg detection isn't meaningful here.
	// Re-anchor on the canonical head and let catchUp resume; reorg detection
	// re-arms once the cursor advances to within reorg_depth of the tip.
	if newHead.Number > recent[0].Number+i.reorgDepth {
		if err := i.store.RecordBlockHash(ctx, i.source, newHead.Number, newHead.Hash, newHead.ParentHash, i.reorgDepth); err != nil {
			return 0, 0, fmt.Errorf("re-anchor head hash: %w", err)
		}
		i.log.Warn("re-anchoring on head; reorg detection suspended until cursor catches up", "head", newHead.Number, "oldest_stored", recent[0].Number, "gap", newHead.Number-recent[0].Number)
		return newHead.Number, 0, nil
	}

	walked := []rpc.Header{newHead}
	cur := newHead
	var ancestor *rpc.Header
	for steps := uint64(0); steps < i.reorgDepth; steps++ {
		stored, ok, err := i.store.BlockHashAt(ctx, i.source, cur.Number)
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
		// rewindSinks runs before store.RewindTo so a sink failure leaves the
		// store untouched; the next reconcileHead re-detects the same reorg
		// and retries the whole rewind from scratch. Generated sink rewinds
		// are `DELETE FROM <table> WHERE source = ? AND block_number >= ?` —
		// idempotent, so partial progress on the retry is safe.
		if err := i.rewindSinks(ctx, rewindTarget); err != nil {
			return 0, 0, err
		}
		if err := i.store.RewindTo(ctx, i.source, rewindTarget); err != nil {
			return 0, 0, fmt.Errorf("rewind to %d: %w", rewindTarget, err)
		}
		cursorHint = rewindTarget
		i.log.Warn("reorg detected", "ancestor", ancestor.Number, "rewind_to", rewindTarget, "orphaned_blocks", mostRecent.Number-ancestor.Number)
	}

	for _, h := range walked {
		if err := i.store.RecordBlockHash(ctx, i.source, h.Number, h.Hash, h.ParentHash, i.reorgDepth); err != nil {
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
	recent, err := i.store.RecentBlockHashes(ctx, i.source, 1)
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
		h, err := i.rpc.BlockByNumber(ctx, rpc.HexUint64(n))
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			i.log.Warn("backfill block_hashes failed; reorg detection will warm up over time", "block", n, "err", err)
			return nil
		}
		if err := i.store.RecordBlockHash(ctx, i.source, h.Number, h.Hash, h.ParentHash, i.reorgDepth); err != nil {
			return fmt.Errorf("backfill: record hash %d: %w", n, err)
		}
	}
	i.log.Info("block_hashes backfilled for reorg detection", "from", start, "to", end, "count", end-start+1)
	return nil
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
				case results <- chunkResult{from: r.from, to: r.to, events: i.toEvents(logs), startAt: start, fetchedAt: time.Now()}:
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
				if err := i.store.SaveEvents(gctx, i.source, ready.events); err != nil {
					return fmt.Errorf("save events: %w", err)
				}
				for _, ev := range ready.events {
					if err := i.dispatchSinks(gctx, ev); err != nil {
						return err
					}
				}
				next := ready.to + 1
				if err := i.store.SetCheckpoint(gctx, i.source, next); err != nil {
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

// rewindSinks fans a rewind through every registered sink so each generated
// typed table drops orphaned rows from blocks ≥ block for THIS source.
// Iterates allSinks (registration order) rather than sinksByTopic0 (a Go map)
// so the on-disk residue from a partial-failure mid-fanout is deterministic
// across runs.
func (i *Indexer) rewindSinks(ctx context.Context, block uint64) error {
	for _, sink := range i.allSinks {
		if err := sink.RewindTo(ctx, i.source, block); err != nil {
			return fmt.Errorf("sink %s rewind: %w", sink.SinkID(), err)
		}
	}
	return nil
}

// dispatchSinks fans an event out to wildcard sinks (always) and the per-topic
// bucket keyed by canonical-cased Topics[0] (when present). Casing differs
// across RPC providers, so the key is normalized via common.HexToHash.
func (i *Indexer) dispatchSinks(ctx context.Context, ev store.Event) error {
	for _, sink := range i.wildcardSinks {
		if err := sink.Handle(ctx, ev); err != nil {
			return fmt.Errorf("sink %s: %w", sink.SinkID(), err)
		}
	}
	if len(ev.Topics) == 0 {
		return nil
	}
	key := common.HexToHash(ev.Topics[0]).Hex()
	for _, sink := range i.sinksByTopic0[key] {
		if err := sink.Handle(ctx, ev); err != nil {
			return fmt.Errorf("sink %s: %w", sink.SinkID(), err)
		}
	}
	return nil
}

// toEvents maps RPC log records into store.Event records, stamping each with
// the indexer's source. Sinks downstream see e.Source via Event.Source.
func (i *Indexer) toEvents(logs []rpc.Log) []store.Event {
	out := make([]store.Event, len(logs))
	for idx, l := range logs {
		out[idx] = store.Event{
			Source:      i.source,
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

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
	return &Indexer{
		rpc:          client,
		store:        st,
		contract:     opts.Contract,
		topics:       opts.Topics,
		chunkSize:    opts.ChunkSize,
		pollInterval: opts.PollInterval,
		startBlock:   opts.StartBlock,
		concurrency:  concurrency,
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

	ticker := time.NewTicker(i.pollInterval)
	defer ticker.Stop()

	for {
		head, err := i.rpc.BlockNumber(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch head: %w", err)
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

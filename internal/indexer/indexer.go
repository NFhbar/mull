package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/NFhbar/mull/internal/rpc"
	"github.com/NFhbar/mull/internal/store"
)

type Options struct {
	Contract     string
	Topics       []string
	ChunkSize    uint64
	PollInterval time.Duration
	StartBlock   uint64
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
	log          *slog.Logger
}

func New(client rpc.Client, st store.Store, opts Options) *Indexer {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Indexer{
		rpc:          client,
		store:        st,
		contract:     opts.Contract,
		topics:       opts.Topics,
		chunkSize:    opts.ChunkSize,
		pollInterval: opts.PollInterval,
		startBlock:   opts.StartBlock,
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

func (i *Indexer) catchUp(ctx context.Context, from, head uint64) (uint64, error) {
	for from <= head {
		to := from + i.chunkSize - 1
		if to > head {
			to = head
		}
		start := time.Now()
		logs, err := i.rpc.GetLogs(ctx, from, to, i.contract, i.topics)
		if err != nil {
			return from, fmt.Errorf("get logs [%d,%d]: %w", from, to, err)
		}
		events := toEvents(logs)
		if err := i.store.SaveEvents(ctx, events); err != nil {
			return from, fmt.Errorf("save events: %w", err)
		}
		next := to + 1
		if err := i.store.SetCheckpoint(ctx, next); err != nil {
			return from, fmt.Errorf("set checkpoint: %w", err)
		}
		i.log.Info("indexed range",
			"from", from,
			"to", to,
			"events", len(events),
			"took_ms", time.Since(start).Milliseconds(),
			"lag_blocks", head-to,
		)
		from = next
		if err := ctx.Err(); err != nil {
			return from, err
		}
	}
	return from, nil
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

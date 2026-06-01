package store

import "context"

type Event struct {
	BlockNumber uint64
	TxHash      string
	LogIndex    uint
	Address     string
	Topics      []string
	Data        string
}

type BlockHashEntry struct {
	Number     uint64
	Hash       string
	ParentHash string
}

type Store interface {
	SaveEvents(ctx context.Context, events []Event) error
	Checkpoint(ctx context.Context) (uint64, error)
	SetCheckpoint(ctx context.Context, block uint64) error
	RecordBlockHash(ctx context.Context, number uint64, hash, parentHash string, capDepth uint64) error
	RecentBlockHashes(ctx context.Context, limit uint64) ([]BlockHashEntry, error)
	BlockHashAt(ctx context.Context, number uint64) (BlockHashEntry, bool, error)
	RewindTo(ctx context.Context, block uint64) error
	Close() error
}

// EventSink is a typed-event consumer wired in by generated code.
// Each generated sink targets exactly one event signature; sinks
// are responsible for matching themselves against Topics[0] and
// returning a nil error on signature mismatch (a no-op).
//
// Sinks MUST be idempotent on retry. Generated sinks satisfy this
// by using INSERT OR IGNORE on (tx_hash, log_index); the indexer's
// raw-events save, sink fan-out, and checkpoint advance run in
// separate transactions, so a mid-chunk crash can replay any sink.
type EventSink interface {
	SinkID() string
	Handle(ctx context.Context, e Event) error
}

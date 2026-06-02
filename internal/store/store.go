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
	Query(ctx context.Context, filter QueryFilter) ([]Event, *EventCursor, error)
	Close() error
}

// EventCursor positions a Query strictly after (Block, LogIndex). It is the
// opaque pagination handle exposed by Query: the second return value of a
// previous page is the After of the next page.
type EventCursor struct {
	Block    uint64
	LogIndex uint
}

// QueryFilter is the parameter object for Store.Query. All fields are
// optional; nil/zero means "no filter on that dimension".
//
// Pointer fields on Topic0..Topic3 distinguish "absent" (nil) from
// "explicitly the empty string" (&"") — rare in practice but well-defined.
// FromBlock and ToBlock are inclusive bounds. Limit is clamped to [1, 1000]
// by the impl; zero means "default" (100).
type QueryFilter struct {
	Contract  string
	Topic0    *string
	Topic1    *string
	Topic2    *string
	Topic3    *string
	FromBlock *uint64
	ToBlock   *uint64
	Limit     int
	After     *EventCursor
}

// EventSink is a typed-event consumer wired in by generated code.
// Each generated sink targets exactly one event signature; the
// indexer dispatches by Topic0() in O(1) per event. A sink may
// return an empty Topic0() to opt out of the dispatch index and
// receive every event (wildcard), in which case it is responsible
// for filtering itself.
//
// Sinks MUST be idempotent on retry. Generated sinks satisfy this
// by using INSERT OR IGNORE on (tx_hash, log_index); the indexer's
// raw-events save, sink fan-out, and checkpoint advance run in
// separate transactions, so a mid-chunk crash can replay any sink.
//
// RewindTo mirrors Store.RewindTo for each sink's own table — the
// indexer fans the rewind through every sink on reorg so typed
// tables stay consistent with the raw events table. Without this
// hook, orphaned rows on the abandoned fork would linger and
// SELECTs would return a union of forks (see reorg path in
// Indexer.reconcileHead).
type EventSink interface {
	SinkID() string
	Topic0() string
	Handle(ctx context.Context, e Event) error
	RewindTo(ctx context.Context, block uint64) error
}

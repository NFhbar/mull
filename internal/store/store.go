package store

import (
	"context"
	"database/sql"
	"errors"
)

// ErrDBNeedsMigration is returned by OpenSQLite when the DB file exists but
// is on the v1 schema. cmd/index and cmd/serve translate this into an
// actionable error pointing the operator at `mull migrate`.
var ErrDBNeedsMigration = errors.New("database needs migration to v2 (run `mull migrate`)")

type Event struct {
	Source      string
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
	SaveEvents(ctx context.Context, source string, events []Event) error
	Checkpoint(ctx context.Context, source string) (uint64, error)
	SetCheckpoint(ctx context.Context, source string, block uint64) error
	Checkpoints(ctx context.Context) (map[string]uint64, error)
	RecordBlockHash(ctx context.Context, source string, number uint64, hash, parentHash string, capDepth uint64) error
	RecentBlockHashes(ctx context.Context, source string, limit uint64) ([]BlockHashEntry, error)
	BlockHashAt(ctx context.Context, source string, number uint64) (BlockHashEntry, bool, error)
	RewindTo(ctx context.Context, source string, block uint64) error
	Query(ctx context.Context, filter QueryFilter) ([]Event, *EventCursor, error)
	Close() error
}

// EventCursor positions a Query strictly after (Block, LogIndex, Source). The
// Source field is the multi-source disambiguator: two events from different
// sources can share (Block, LogIndex), and without source in the ordering they
// would silently drop one across a page boundary. Source sorts ASCII so an
// empty Source string sorts strictly before any real source — see
// decodeCursor in internal/serve for the legacy-cursor compat path.
type EventCursor struct {
	Block    uint64
	LogIndex uint
	Source   string
}

// QueryFilter is the parameter object for Store.Query. All fields are
// optional; nil/zero means "no filter on that dimension".
//
// Pointer fields on Topic0..Topic3 distinguish "absent" (nil) from
// "explicitly the empty string" (&"") — rare in practice but well-defined.
// FromBlock and ToBlock are inclusive bounds. Limit is clamped to [1, 1000]
// by the impl; zero means "default" (100). Source uses the same pointer
// convention so a multi-source serve API can either filter by name or
// page across every source.
type QueryFilter struct {
	Source    *string
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

// Execer is the narrow write surface generated sinks need. Satisfied by
// both *sql.DB and *sql.Tx, so a sink can write through the live handle
// during indexing or through a rebuild transaction during `mull migrate`.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// EventSink is a typed-event consumer wired in by generated code.
// Each generated sink targets exactly one event signature; the
// indexer dispatches by Topic0() in O(1) per event. A sink may
// return an empty Topic0() to opt out of the dispatch index and
// receive every event (wildcard), in which case it is responsible
// for filtering itself.
//
// Sinks MUST be idempotent on retry. Generated sinks satisfy this
// by using INSERT OR IGNORE on (source, tx_hash, log_index); the indexer's
// raw-events save, sink fan-out, and checkpoint advance run in
// separate transactions, so a mid-chunk crash can replay any sink.
//
// RewindTo mirrors Store.RewindTo for each sink's own table — the
// indexer fans the rewind through every sink on reorg so typed
// tables stay consistent with the raw events table. The source is
// threaded through so rewinds scoped to one chain don't wipe another.
type EventSink interface {
	SinkID() string
	Topic0() string
	Handle(ctx context.Context, e Event) error
	RewindTo(ctx context.Context, source string, block uint64) error
}

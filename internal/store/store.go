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

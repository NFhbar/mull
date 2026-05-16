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

type Store interface {
	SaveEvents(ctx context.Context, events []Event) error
	Checkpoint(ctx context.Context) (uint64, error)
	SetCheckpoint(ctx context.Context, block uint64) error
	Close() error
}

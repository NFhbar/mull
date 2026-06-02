package indexer

import (
	"context"
	"time"

	"github.com/NFhbar/mull/internal/rpc"
)

// HeadSource abstracts how the indexer learns about new chain heads. The
// implementation (polling vs WebSocket) is selected from config at startup.
//
// Subscribe semantics: the returned channel closes when ctx is cancelled or the
// source is terminally dead. Sends are best-effort — a slow consumer drops
// intermediate heads but never sees them out of order; each delivered head
// supersedes any earlier undelivered ones. This mirrors the pre-WSS poll
// contract: missing intermediate heads on a slow consumer just means the next
// reconcile catches up further.
type HeadSource interface {
	Latest(ctx context.Context) (rpc.Header, error)
	Subscribe(ctx context.Context) (<-chan rpc.Header, error)
}

// PollingHeadSource implements HeadSource by ticking every PollInterval and
// emitting the result of eth_getBlockByNumber("latest"). Preserves the pre-WSS
// behaviour bit-for-bit when configured.
type PollingHeadSource struct {
	Client       rpc.Client
	PollInterval time.Duration
}

func (p *PollingHeadSource) Latest(ctx context.Context) (rpc.Header, error) {
	return p.Client.BlockByNumber(ctx, "latest")
}

func (p *PollingHeadSource) Subscribe(ctx context.Context) (<-chan rpc.Header, error) {
	out := make(chan rpc.Header, 1)
	go func() {
		defer close(out)
		ticker := time.NewTicker(p.PollInterval)
		defer ticker.Stop()
		for {
			head, err := p.Client.BlockByNumber(ctx, "latest")
			if err == nil {
				deliverHead(out, head)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return out, nil
}

// deliverHead sends head on a buffered (size 1) channel without blocking. On a
// full buffer the head is dropped — the next reconcile catches up to whatever
// is current, matching the pre-WSS "the next tick re-fetches latest" contract.
func deliverHead(ch chan<- rpc.Header, head rpc.Header) {
	select {
	case ch <- head:
	default:
	}
}

package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/NFhbar/mull/internal/rpc"
)

func TestPollingHeadSource_DeliversCurrentHead(t *testing.T) {
	r := &fakeRPC{
		head: 42,
		headers: map[uint64]rpc.Header{
			42: {Number: 42, Hash: "0xh42", ParentHash: "0xh41"},
		},
	}
	p := &PollingHeadSource{Client: r, PollInterval: time.Hour}

	got, err := p.Latest(context.Background())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Number != 42 || got.Hash != "0xh42" {
		t.Fatalf("Latest = %+v, want {Number:42 Hash:0xh42}", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := p.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case h := <-ch:
		if h.Number != 42 {
			t.Fatalf("first head = %+v, want Number=42", h)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first head")
	}
}

func TestPollingHeadSource_CloseOnCtxCancel(t *testing.T) {
	r := &fakeRPC{
		head: 7,
		headers: map[uint64]rpc.Header{
			7: {Number: 7, Hash: "0xh7", ParentHash: "0xh6"},
		},
	}
	p := &PollingHeadSource{Client: r, PollInterval: 10 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Drain at least one head so we know the goroutine is running.
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no head delivered before cancel")
	}

	cancel()
	// Channel must close within roughly one tick of the cancel.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel did not close after ctx cancel")
		}
	}
}

func TestPollingHeadSource_SlowConsumerDoesNotBlock(t *testing.T) {
	r := &fakeRPC{
		head: 100,
		headers: map[uint64]rpc.Header{
			100: {Number: 100, Hash: "0xh100", ParentHash: "0xh99"},
		},
	}
	p := &PollingHeadSource{Client: r, PollInterval: time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Deliberately do not consume from ch; the ticker fires many times. The
	// non-blocking deliverHead must keep the goroutine alive. If the goroutine
	// blocked on send, ctx-cancel below would not close the channel promptly.
	time.Sleep(20 * time.Millisecond)
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel did not close after ctx cancel — goroutine leaked")
		}
	}
}

package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type fakeFallback struct {
	latestH    Header
	headCh     chan Header
	subscribed atomic.Bool
}

func newFakeFallback(latest Header) *fakeFallback {
	return &fakeFallback{
		latestH: latest,
		headCh:  make(chan Header, 16),
	}
}

func (f *fakeFallback) Latest(context.Context) (Header, error) {
	return f.latestH, nil
}

func (f *fakeFallback) Subscribe(ctx context.Context) (<-chan Header, error) {
	f.subscribed.Store(true)
	out := make(chan Header, 16)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case h, ok := <-f.headCh:
				if !ok {
					return
				}
				select {
				case out <- h:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func wsQuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func wsURLFor(s *httptest.Server) string {
	return strings.Replace(s.URL, "http://", "ws://", 1)
}

func wsTestRetryPolicy() RetryPolicy {
	return RetryPolicy{Base: 100 * time.Microsecond, MaxDelay: time.Millisecond, MaxAttempts: 100}
}

func makeNotif(t *testing.T, h Header) []byte {
	t.Helper()
	notif := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_subscription",
		"params": map[string]any{
			"subscription": "0xsub",
			"result": map[string]any{
				"number":     HexUint64(h.Number),
				"hash":       h.Hash,
				"parentHash": h.ParentHash,
			},
		},
	}
	b, err := json.Marshal(notif)
	if err != nil {
		t.Fatalf("marshal notif: %v", err)
	}
	return b
}

func readAndAckSubscribe(ctx context.Context, c *websocket.Conn) error {
	_, data, err := c.Read(ctx)
	if err != nil {
		return err
	}
	var req struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	if req.Method != "eth_subscribe" {
		return fmt.Errorf("server: unexpected method %q", req.Method)
	}
	resp := map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  "0xsub",
	}
	b, _ := json.Marshal(resp)
	return c.Write(ctx, websocket.MessageText, b)
}

func TestWebSocketHeadSource_LatestDelegatesToFallback(t *testing.T) {
	sentinel := Header{Number: 999, Hash: "0xsentinel", ParentHash: "0xparent"}
	fb := newFakeFallback(sentinel)
	// Obviously-broken URL — if Latest dialed, this would hang or error.
	ws := NewWebSocketHeadSource("ws://127.0.0.1:1", fb, WSOptions{Logger: wsQuietLogger()})

	got, err := ws.Latest(context.Background())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got != sentinel {
		t.Fatalf("Latest = %+v, want sentinel %+v", got, sentinel)
	}
	if fb.subscribed.Load() {
		t.Fatal("fallback Subscribe was called for Latest — only Latest should run")
	}
}

func TestWebSocketHeadSource_Subscribe(t *testing.T) {
	head := Header{Number: 42, Hash: "0xh42", ParentHash: "0xh41"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		if err := readAndAckSubscribe(ctx, c); err != nil {
			return
		}
		_ = c.Write(ctx, websocket.MessageText, makeNotif(t, head))
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()

	fb := newFakeFallback(Header{})
	ws := NewWebSocketHeadSource(wsURLFor(srv), fb, WSOptions{
		Logger:        wsQuietLogger(),
		FallbackAfter: time.Second,
		PingInterval:  10 * time.Second,
		PingTimeout:   10 * time.Second,
		RetryPolicy:   wsTestRetryPolicy(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := ws.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		if got != head {
			t.Fatalf("got = %+v, want %+v", got, head)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no head received")
	}
}

func TestWebSocketHeadSource_ReconnectAfterDrop(t *testing.T) {
	head1 := Header{Number: 100, Hash: "0xh100", ParentHash: "0xh99"}
	head2 := Header{Number: 101, Hash: "0xh101", ParentHash: "0xh100"}
	var connCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := connCount.Add(1)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := r.Context()
		if err := readAndAckSubscribe(ctx, c); err != nil {
			c.CloseNow()
			return
		}
		if n == 1 {
			_ = c.Write(ctx, websocket.MessageText, makeNotif(t, head1))
			c.CloseNow()
			return
		}
		_ = c.Write(ctx, websocket.MessageText, makeNotif(t, head2))
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
		c.CloseNow()
	}))
	defer srv.Close()

	fb := newFakeFallback(Header{})
	ws := NewWebSocketHeadSource(wsURLFor(srv), fb, WSOptions{
		Logger:        wsQuietLogger(),
		FallbackAfter: 10 * time.Second,
		PingInterval:  10 * time.Second,
		PingTimeout:   10 * time.Second,
		RetryPolicy:   wsTestRetryPolicy(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := ws.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	recv := func(want Header) {
		t.Helper()
		select {
		case got, ok := <-ch:
			if !ok {
				t.Fatal("channel closed unexpectedly")
			}
			if got != want {
				t.Fatalf("got = %+v, want %+v", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for head")
		}
	}
	recv(head1)
	recv(head2)
	if fb.subscribed.Load() {
		t.Fatal("fallback Subscribe was called — reconnect path should not demote")
	}
}

func TestWebSocketHeadSource_FallbackAfterProlongedFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusBadRequest)
	}))
	defer srv.Close()

	head := Header{Number: 5, Hash: "0xfb5", ParentHash: "0xfb4"}
	fb := newFakeFallback(head)
	fb.headCh <- head

	ws := NewWebSocketHeadSource(wsURLFor(srv), fb, WSOptions{
		Logger:        wsQuietLogger(),
		FallbackAfter: 50 * time.Millisecond,
		PingInterval:  10 * time.Second,
		PingTimeout:   10 * time.Second,
		RetryPolicy:   wsTestRetryPolicy(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := ws.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before fallback delivery")
		}
		if got != head {
			t.Fatalf("got = %+v, want fallback head %+v", got, head)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fallback did not deliver head")
	}
	if !fb.subscribed.Load() {
		t.Fatal("fallback.Subscribe was not called after demotion")
	}
}

func TestWebSocketHeadSource_FallbackWindowResetsOnSuccess(t *testing.T) {
	head := Header{Number: 7, Hash: "0xh7", ParentHash: "0xh6"}
	var phase atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := phase.Add(1)
		if p == 2 {
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			ctx := r.Context()
			if err := readAndAckSubscribe(ctx, c); err != nil {
				c.CloseNow()
				return
			}
			_ = c.Write(ctx, websocket.MessageText, makeNotif(t, head))
			c.CloseNow()
			return
		}
		http.Error(w, "no", http.StatusBadRequest)
	}))
	defer srv.Close()

	fbHead := Header{Number: 999, Hash: "0xfb", ParentHash: "0xfb-1"}
	fb := newFakeFallback(fbHead)
	fb.headCh <- fbHead

	fallbackAfter := 100 * time.Millisecond
	ws := NewWebSocketHeadSource(wsURLFor(srv), fb, WSOptions{
		Logger:        wsQuietLogger(),
		FallbackAfter: fallbackAfter,
		PingInterval:  10 * time.Second,
		PingTimeout:   10 * time.Second,
		RetryPolicy:   wsTestRetryPolicy(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := ws.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var firstAt time.Time
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before any delivery")
		}
		firstAt = time.Now()
		if got != head {
			t.Fatalf("first head = %+v, want WS head %+v", got, head)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no first head")
	}

	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before fallback head")
		}
		if got != fbHead {
			t.Fatalf("second head = %+v, want fallback %+v", got, fbHead)
		}
		elapsed := time.Since(firstAt)
		if elapsed < fallbackAfter {
			t.Fatalf("demotion fired %v after WS head, want >= %v (window did not reset)", elapsed, fallbackAfter)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no fallback head")
	}
}

func TestWebSocketHeadSource_PingTimeoutTriggersReconnect(t *testing.T) {
	head := Header{Number: 200, Hash: "0xh200", ParentHash: "0xh199"}
	var connCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := connCount.Add(1)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := r.Context()
		if err := readAndAckSubscribe(ctx, c); err != nil {
			c.CloseNow()
			return
		}
		if n == 1 {
			// First conn: do not Read further, so coder/websocket cannot
			// auto-pong client pings. The client's ping context will time
			// out and the source will close + reconnect.
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			c.CloseNow()
			return
		}
		_ = c.Write(ctx, websocket.MessageText, makeNotif(t, head))
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
		c.CloseNow()
	}))
	defer srv.Close()

	fb := newFakeFallback(Header{})
	ws := NewWebSocketHeadSource(wsURLFor(srv), fb, WSOptions{
		Logger:        wsQuietLogger(),
		FallbackAfter: 10 * time.Second,
		PingInterval:  20 * time.Millisecond,
		PingTimeout:   50 * time.Millisecond,
		RetryPolicy:   wsTestRetryPolicy(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := ws.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before reconnect delivered head")
		}
		if got != head {
			t.Fatalf("got = %+v, want %+v", got, head)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect did not deliver head after ping timeout")
	}
	if connCount.Load() < 2 {
		t.Fatalf("expected at least 2 connections (one ping-timeout + one reconnect), got %d", connCount.Load())
	}
}

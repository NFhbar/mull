package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/coder/websocket"
)

// HeadSourceLike is the minimal interface WebSocketHeadSource needs from its
// fallback. Defined locally so internal/rpc has no cross-package import on
// internal/indexer (which already depends on internal/rpc).
type HeadSourceLike interface {
	Latest(ctx context.Context) (Header, error)
	Subscribe(ctx context.Context) (<-chan Header, error)
}

type WSOptions struct {
	// FallbackAfter is the window of uninterrupted WS failure after which the
	// source demotes to its polling fallback and stays there. Default 30s.
	FallbackAfter time.Duration
	// PingInterval is how often a WS ping frame is sent to keep NAT alive.
	// Default 5s.
	PingInterval time.Duration
	// PingTimeout is the deadline for a single Ping round-trip. Default 10s.
	PingTimeout time.Duration
	// RetryPolicy controls the reconnect backoff. Reuses the same shape as the
	// JSON-RPC retry policy but is its own state machine — failures here are
	// not classified as transientError. Default DefaultRetryPolicy().
	RetryPolicy RetryPolicy
	Logger      *slog.Logger
}

type WebSocketHeadSource struct {
	url           string
	fallback      HeadSourceLike
	fallbackAfter time.Duration
	pingInterval  time.Duration
	pingTimeout   time.Duration
	retry         RetryPolicy
	log           *slog.Logger

	// Manager-goroutine-only state. Subscribe is expected to be called once;
	// the manager is the single writer.
	lastSeen *Header
}

func NewWebSocketHeadSource(wsURL string, fallback HeadSourceLike, opts WSOptions) *WebSocketHeadSource {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	fb := opts.FallbackAfter
	if fb <= 0 {
		fb = 30 * time.Second
	}
	pi := opts.PingInterval
	if pi <= 0 {
		pi = 5 * time.Second
	}
	pt := opts.PingTimeout
	if pt <= 0 {
		pt = 10 * time.Second
	}
	policy := opts.RetryPolicy
	applyRetryDefaults(&policy)
	return &WebSocketHeadSource{
		url:           wsURL,
		fallback:      fallback,
		fallbackAfter: fb,
		pingInterval:  pi,
		pingTimeout:   pt,
		retry:         policy,
		log:           log,
	}
}

// Latest delegates to the fallback. The WS connection itself is not exercised
// — the fallback (always a PollingHeadSource in production) hits HTTP, which
// is what the indexer wants for cold-start. Lets providers split HTTP/WS
// endpoints (Alchemy/Quicknode/…) without URL rewriting here.
func (s *WebSocketHeadSource) Latest(ctx context.Context) (Header, error) {
	return s.fallback.Latest(ctx)
}

func (s *WebSocketHeadSource) Subscribe(ctx context.Context) (<-chan Header, error) {
	out := make(chan Header, 1)
	go s.manager(ctx, out)
	return out, nil
}

// manager is the WS state machine. Each iteration runs one connect → subscribe
// → stream session. On session end (any error) it inspects the fallback window:
// open the window on first failure since the last delivery, demote when the
// window has exceeded fallbackAfter without another delivery. A successful
// head delivery during a session clears the window.
func (s *WebSocketHeadSource) manager(ctx context.Context, out chan<- Header) {
	defer close(out)

	var (
		windowOpen time.Time
		attempt    int
	)
	resetWindow := func() {
		windowOpen = time.Time{}
		attempt = 0
	}

	for {
		if ctx.Err() != nil {
			return
		}
		err := s.runSession(ctx, out, resetWindow)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.log.DebugContext(ctx, "ws session ended", "err", err)
		}
		if windowOpen.IsZero() {
			windowOpen = time.Now()
		}
		if time.Since(windowOpen) >= s.fallbackAfter {
			s.log.Warn("demoting to polling head source",
				"url", s.url,
				"fallback_after_ms", s.fallbackAfter.Milliseconds())
			s.forwardFallback(ctx, out)
			return
		}
		attempt++
		delay := time.Duration(rand.Int64N(int64(backoffCap(s.retry, attempt)) + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// forwardFallback hands head delivery off to the polling fallback for the rest
// of the run. Once-only demotion: the manager exits after this returns.
func (s *WebSocketHeadSource) forwardFallback(ctx context.Context, out chan<- Header) {
	ch, err := s.fallback.Subscribe(ctx)
	if err != nil {
		s.log.Error("fallback head source failed to subscribe", "err", err)
		return
	}
	for h := range ch {
		select {
		case out <- h:
		default:
		}
	}
}

// runSession dials, subscribes, and reads notifications until any failure or
// ctx cancellation. onHeadDelivered is called after each successfully decoded
// head is forwarded to the output channel.
func (s *WebSocketHeadSource) runSession(ctx context.Context, out chan<- Header, onHeadDelivered func()) error {
	conn, _, err := websocket.Dial(ctx, s.url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	subReq := rpcRequest{JSONRPC: "2.0", ID: 1, Method: "eth_subscribe", Params: []any{"newHeads"}}
	reqBytes, err := json.Marshal(subReq)
	if err != nil {
		return fmt.Errorf("marshal subscribe: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, reqBytes); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}

	_, respBytes, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read subscribe response: %w", err)
	}
	var subResp struct {
		Result string    `json:"result"`
		Error  *rpcError `json:"error,omitempty"`
	}
	if err := json.Unmarshal(respBytes, &subResp); err != nil {
		return fmt.Errorf("decode subscribe response: %w", err)
	}
	if subResp.Error != nil {
		return subResp.Error
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ping ticker: on failure, close the connection so the Read loop wakes up
	// with an error and the session ends. The actual ping error is lost — both
	// outcomes (ping timeout, conn drop) collapse to "session over, reconnect".
	go func() {
		t := time.NewTicker(s.pingInterval)
		defer t.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(sessionCtx, s.pingTimeout)
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					conn.CloseNow()
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.Read(sessionCtx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		var notif struct {
			Method string `json:"method"`
			Params struct {
				Subscription string         `json:"subscription"`
				Result       rawBlockHeader `json:"result"`
			} `json:"params"`
		}
		if err := json.Unmarshal(data, &notif); err != nil {
			continue
		}
		if notif.Method != "eth_subscription" {
			continue
		}
		h, err := decodeHeader(&notif.Params.Result)
		if err != nil {
			continue
		}
		if s.lastSeen != nil && h.Number > s.lastSeen.Number+1 {
			s.log.Warn("ws head subscription gap",
				"last_seen", s.lastSeen.Number,
				"new_head", h.Number,
				"gap_blocks", h.Number-(s.lastSeen.Number+1))
		}
		ls := h
		s.lastSeen = &ls
		deliverWSHead(out, h)
		onHeadDelivered()
	}
}

func deliverWSHead(ch chan<- Header, head Header) {
	select {
	case ch <- head:
	default:
	}
}

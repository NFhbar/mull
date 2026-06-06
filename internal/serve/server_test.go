package serve

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NFhbar/mull/internal/store"
)

type fakeStore struct {
	mu          sync.Mutex
	checkpoint  uint64
	checkpoints map[string]uint64 // multi-source Checkpoints() return
	checkpErr   error
	events      []store.Event
	nextCursor  *store.EventCursor
	queryErr    error
	lastFilter  store.QueryFilter
	queryCalled atomic.Bool
	queryBlock  chan struct{} // when non-nil Query blocks until closed or ctx done
}

func (f *fakeStore) SaveEvents(context.Context, string, []store.Event) error { return nil }
func (f *fakeStore) Checkpoint(_ context.Context, _ string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoint, f.checkpErr
}
func (f *fakeStore) SetCheckpoint(context.Context, string, uint64) error { return nil }
func (f *fakeStore) Checkpoints(context.Context) (map[string]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.checkpErr != nil {
		return nil, f.checkpErr
	}
	if f.checkpoints != nil {
		return f.checkpoints, nil
	}
	if f.checkpoint != 0 {
		return map[string]uint64{"default": f.checkpoint}, nil
	}
	return map[string]uint64{}, nil
}
func (f *fakeStore) RecordBlockHash(context.Context, string, uint64, string, string, uint64) error {
	return nil
}
func (f *fakeStore) RecentBlockHashes(context.Context, string, uint64) ([]store.BlockHashEntry, error) {
	return nil, nil
}
func (f *fakeStore) BlockHashAt(context.Context, string, uint64) (store.BlockHashEntry, bool, error) {
	return store.BlockHashEntry{}, false, nil
}
func (f *fakeStore) RewindTo(context.Context, string, uint64) error { return nil }
func (f *fakeStore) Query(ctx context.Context, filter store.QueryFilter) ([]store.Event, *store.EventCursor, error) {
	f.mu.Lock()
	f.lastFilter = filter
	block := f.queryBlock
	events := f.events
	cursor := f.nextCursor
	qerr := f.queryErr
	f.mu.Unlock()
	f.queryCalled.Store(true)
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	return events, cursor, qerr
}
func (f *fakeStore) Close() error { return nil }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestServer(t *testing.T, st store.Store) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewServer(st, quietLogger()).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestHealthz(t *testing.T) {
	srv := newTestServer(t, &fakeStore{})

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("body = %+v, want status=ok", body)
	}

	resp2, err := http.Post(srv.URL+"/healthz", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", resp2.StatusCode)
	}
}

// TestCheckpointAlwaysReturnsMap pins the v2 uniform response shape: regardless
// of whether ?source= is set, /checkpoint always returns
// {"checkpoints": {<src>: <n>, …}}. v1 returned {"checkpoint": <n>} with no
// source; the rename + always-map is the documented breaking change.
func TestCheckpointAlwaysReturnsMap(t *testing.T) {
	t.Run("no source — multi-source map", func(t *testing.T) {
		st := &fakeStore{checkpoints: map[string]uint64{"a": 10, "b": 20}}
		srv := newTestServer(t, st)
		resp, err := http.Get(srv.URL + "/checkpoint")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var body checkpointResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Checkpoints["a"] != 10 || body.Checkpoints["b"] != 20 {
			t.Fatalf("checkpoints = %+v, want {a:10, b:20}", body.Checkpoints)
		}
	})
	t.Run("with source — single-key map", func(t *testing.T) {
		srv := newTestServer(t, &fakeStore{checkpoint: 12345})
		resp, err := http.Get(srv.URL + "/checkpoint?source=usdc")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var body checkpointResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(body.Checkpoints) != 1 || body.Checkpoints["usdc"] != 12345 {
			t.Fatalf("checkpoints = %+v, want {usdc:12345}", body.Checkpoints)
		}
	})
	t.Run("store error", func(t *testing.T) {
		srv := newTestServer(t, &fakeStore{checkpErr: errors.New("boom")})
		resp, err := http.Get(srv.URL + "/checkpoint")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", resp.StatusCode)
		}
	})
}

func TestEventsHappyPath(t *testing.T) {
	st := &fakeStore{events: []store.Event{
		{BlockNumber: 1, TxHash: "0xa", LogIndex: 0, Address: "0xc", Topics: []string{"0xT"}, Data: "0x"},
		{BlockNumber: 2, TxHash: "0xb", LogIndex: 0, Address: "0xc", Topics: []string{"0xT"}, Data: "0x"},
	}}
	srv := newTestServer(t, st)

	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body eventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Events) != 2 {
		t.Fatalf("events len = %d, want 2", len(body.Events))
	}
	if body.NextCursor != "" {
		t.Fatalf("next_cursor = %q, want empty", body.NextCursor)
	}
}

func TestEventsTopicDecodeRules(t *testing.T) {
	cases := []struct {
		name string
		path string
		check func(t *testing.T, f store.QueryFilter)
	}{
		{
			name: "value",
			path: "/events?topic0=0xABC",
			check: func(t *testing.T, f store.QueryFilter) {
				if f.Topic0 == nil || *f.Topic0 != "0xABC" {
					t.Fatalf("Topic0 = %v, want &\"0xABC\"", f.Topic0)
				}
			},
		},
		{
			name: "empty",
			path: "/events?topic0=",
			check: func(t *testing.T, f store.QueryFilter) {
				if f.Topic0 == nil {
					t.Fatalf("Topic0 = nil, want &\"\"")
				}
				if *f.Topic0 != "" {
					t.Fatalf("Topic0 = %q, want empty", *f.Topic0)
				}
			},
		},
		{
			name: "absent",
			path: "/events",
			check: func(t *testing.T, f store.QueryFilter) {
				if f.Topic0 != nil {
					t.Fatalf("Topic0 = %v, want nil", f.Topic0)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{}
			srv := newTestServer(t, st)
			resp, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			st.mu.Lock()
			f := st.lastFilter
			st.mu.Unlock()
			tc.check(t, f)
		})
	}
}

func TestEventsBadParams(t *testing.T) {
	cases := []struct {
		name  string
		path  string
		param string
	}{
		{"bad from", "/events?from=abc", "from"},
		{"bad to", "/events?to=xyz", "to"},
		{"bad limit", "/events?limit=lots", "limit"},
		{"bad cursor", "/events?cursor=!!!notb64!!!", "cursor"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t, &fakeStore{})
			resp, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body=%s)", resp.StatusCode, string(body))
			}
			if !strings.Contains(string(body), tc.param) {
				t.Fatalf("body = %q, want to mention %q", string(body), tc.param)
			}
		})
	}
}

func TestEventsCursorRoundTrip(t *testing.T) {
	cursor := store.EventCursor{Block: 9876, LogIndex: 42}
	encoded := encodeCursor(cursor)

	st := &fakeStore{}
	srv := newTestServer(t, st)
	resp, err := http.Get(srv.URL + "/events?cursor=" + encoded)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	st.mu.Lock()
	got := st.lastFilter.After
	st.mu.Unlock()
	if got == nil {
		t.Fatal("After = nil, want &cursor")
	}
	if got.Block != cursor.Block || got.LogIndex != cursor.LogIndex {
		t.Fatalf("After = %+v, want %+v", *got, cursor)
	}
}

func TestEventsStoreErrorReturns500(t *testing.T) {
	srv := newTestServer(t, &fakeStore{queryErr: errors.New("disk corrupted")})
	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

func TestEventsContextCancellation(t *testing.T) {
	st := &fakeStore{queryBlock: make(chan struct{})}
	srv := newTestServer(t, st)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	done := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()

	// Wait until Query is observed running, then cancel.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !st.queryCalled.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if !st.queryCalled.Load() {
		t.Fatal("Query never invoked")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not return after cancel")
	}
}

// TestEventsStoreCtxErrorIsQuiet pins the contract at server.go's events
// handler: when the store returns context.Canceled or DeadlineExceeded
// (regardless of why — client disconnect, server shutdown), the handler
// returns silently with no 500 and no body. A future refactor that
// accidentally flips this branch into a logged 500 would fail here.
func TestEventsStoreCtxErrorIsQuiet(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t, &fakeStore{queryErr: tc.err})
			resp, err := http.Get(srv.URL + "/events")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// The handler returns without writing a status, so net/http
			// defaults to 200 OK with an empty body. The load-bearing
			// assertion is "not 500" — we must NOT surface the ctx
			// cancellation as a server error.
			if resp.StatusCode == http.StatusInternalServerError {
				t.Fatalf("ctx cancellation surfaced as 500 (body=%q)", string(body))
			}
			if len(body) != 0 {
				t.Fatalf("ctx cancellation wrote body=%q, want empty", string(body))
			}
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, &fakeStore{})
	for _, route := range []string{"/healthz", "/checkpoint", "/events"} {
		t.Run(route, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+route, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Fatalf("%s POST status = %d, want 405", route, resp.StatusCode)
			}
		})
	}
}

func TestEventsFromToLimit(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	resp, err := http.Get(srv.URL + "/events?from=10&to=20&limit=50")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	st.mu.Lock()
	f := st.lastFilter
	st.mu.Unlock()
	if f.FromBlock == nil || *f.FromBlock != 10 {
		t.Fatalf("FromBlock = %v, want &10", f.FromBlock)
	}
	if f.ToBlock == nil || *f.ToBlock != 20 {
		t.Fatalf("ToBlock = %v, want &20", f.ToBlock)
	}
	if f.Limit != 50 {
		t.Fatalf("Limit = %d, want 50", f.Limit)
	}
}

// TestEventsFilterBySource pins that ?source= populates QueryFilter.Source
// (as a pointer-tri-state) so the SQL pushdown can scope reads to one source.
func TestEventsFilterBySource(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		st := &fakeStore{}
		srv := newTestServer(t, st)
		resp, err := http.Get(srv.URL + "/events?source=usdc_mainnet")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		st.mu.Lock()
		f := st.lastFilter
		st.mu.Unlock()
		if f.Source == nil || *f.Source != "usdc_mainnet" {
			t.Fatalf("Source = %v, want &usdc_mainnet", f.Source)
		}
	})
	t.Run("absent", func(t *testing.T) {
		st := &fakeStore{}
		srv := newTestServer(t, st)
		resp, err := http.Get(srv.URL + "/events")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		st.mu.Lock()
		f := st.lastFilter
		st.mu.Unlock()
		if f.Source != nil {
			t.Fatalf("Source = %v, want nil (absent)", f.Source)
		}
	})
}

// TestMultiSourceCursorRoundTrip pins that the Source field of EventCursor
// round-trips through the wire encoding (base64-of-JSON cursorPayload).
func TestMultiSourceCursorRoundTrip(t *testing.T) {
	c := store.EventCursor{Block: 100, LogIndex: 5, Source: "usdc_arb"}
	encoded := encodeCursor(c)

	st := &fakeStore{}
	srv := newTestServer(t, st)
	resp, err := http.Get(srv.URL + "/events?cursor=" + encoded)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	st.mu.Lock()
	got := st.lastFilter.After
	st.mu.Unlock()
	if got == nil {
		t.Fatal("After = nil, want round-tripped cursor")
	}
	if got.Block != c.Block || got.LogIndex != c.LogIndex || got.Source != c.Source {
		t.Fatalf("After = %+v, want %+v", *got, c)
	}
}

// TestLegacyCursorHandledGracefully encodes a v1-shape cursor payload (no
// `s` field) and feeds it through the v2 decoder. The decoded Source must be
// the empty string — paging from this position is deterministic (empty sorts
// strictly before any real source, so the next page resumes at the next event
// after the boundary; one event may re-emit, which is the documented v1→v2
// behaviour).
func TestLegacyCursorHandledGracefully(t *testing.T) {
	legacyJSON := []byte(`{"b":42,"l":7}`)
	encoded := base64.URLEncoding.EncodeToString(legacyJSON)

	st := &fakeStore{}
	srv := newTestServer(t, st)
	resp, err := http.Get(srv.URL + "/events?cursor=" + encoded)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	st.mu.Lock()
	got := st.lastFilter.After
	st.mu.Unlock()
	if got == nil {
		t.Fatal("After = nil — legacy cursor not accepted")
	}
	if got.Block != 42 || got.LogIndex != 7 {
		t.Fatalf("legacy cursor decoded as {%d,%d}, want {42,7}", got.Block, got.LogIndex)
	}
	if got.Source != "" {
		t.Fatalf("legacy cursor decoded with Source = %q, want \"\" (empty sorts first)", got.Source)
	}
}

package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fastPolicy is a small, deterministic-enough policy for retry tests:
// the total wall-clock per test is bounded by MaxAttempts * MaxDelay.
var fastPolicy = RetryPolicy{
	Base:        1 * time.Microsecond,
	MaxDelay:    100 * time.Microsecond,
	MaxAttempts: 4,
}

// countingServer wraps httptest.Server with an atomic request counter.
func countingServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, n int64)) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		handler(w, r, n)
	}))
	t.Cleanup(srv.Close)
	return srv, &count
}

func TestRetryOn5xx(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, n int64) {
		if n < 3 {
			http.Error(w, "transient", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	got, err := c.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if got != 16 {
		t.Fatalf("BlockNumber = %d, want 16", got)
	}
	if c := count.Load(); c != 3 {
		t.Fatalf("request count = %d, want 3", c)
	}
}

func TestRetryOn429(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, n int64) {
		if n < 3 {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	if _, err := c.BlockNumber(context.Background()); err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if c := count.Load(); c != 3 {
		t.Fatalf("request count = %d, want 3", c)
	}
}

// flakeyTransport returns errors for the first `failures` calls, then
// delegates to a wrapped RoundTripper.
type flakeyTransport struct {
	failures int
	calls    atomic.Int64
	inner    http.RoundTripper
}

func (f *flakeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	n := f.calls.Add(1)
	if int(n) <= f.failures {
		return nil, fmt.Errorf("simulated transport failure %d", n)
	}
	return f.inner.RoundTrip(req)
}

func TestRetryOnTransportError(t *testing.T) {
	srv, _ := countingServer(t, func(w http.ResponseWriter, _ *http.Request, _ int64) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
	})
	flakey := &flakeyTransport{failures: 2, inner: http.DefaultTransport}
	hc := &http.Client{Transport: flakey}
	c := NewHTTPClient(srv.URL, hc, fastPolicy)
	if _, err := c.BlockNumber(context.Background()); err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if got := flakey.calls.Load(); got != 3 {
		t.Fatalf("transport call count = %d, want 3", got)
	}
}

func TestNoRetryOn400(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, _ int64) {
		http.Error(w, "bad request", http.StatusBadRequest)
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	_, err := c.BlockNumber(context.Background())
	if err == nil {
		t.Fatalf("want error, got nil")
	}
	if !strings.Contains(err.Error(), "http 400") {
		t.Fatalf("err = %v, want 'http 400'", err)
	}
	if c := count.Load(); c != 1 {
		t.Fatalf("request count = %d, want 1", c)
	}
}

func TestNoRetryOnRPCError(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, _ int64) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`))
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	_, err := c.BlockNumber(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want rpc error containing 'boom'", err)
	}
	if c := count.Load(); c != 1 {
		t.Fatalf("request count = %d, want 1", c)
	}
}

func TestNoRetryOnDecodeError(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, _ int64) {
		_, _ = w.Write([]byte(`not json{`))
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	_, err := c.BlockNumber(context.Background())
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("err = %v, want decode error", err)
	}
	if c := count.Load(); c != 1 {
		t.Fatalf("request count = %d, want 1", c)
	}
}

func TestAttemptBudgetEnforced(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, _ int64) {
		http.Error(w, "always down", http.StatusServiceUnavailable)
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	_, err := c.BlockNumber(context.Background())
	if err == nil {
		t.Fatalf("want error after exhausted budget, got nil")
	}
	if !strings.Contains(err.Error(), "http 503") {
		t.Fatalf("err = %v, want underlying 'http 503' error string", err)
	}
	var te *transientError
	if errors.As(err, &te) {
		t.Fatalf("returned error still wraps *transientError; classification marker leaked")
	}
	if got := count.Load(); got != int64(fastPolicy.MaxAttempts) {
		t.Fatalf("request count = %d, want %d", got, fastPolicy.MaxAttempts)
	}
}

func TestContextCancellationAbortsBackoff(t *testing.T) {
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, _ int64) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	})
	longPolicy := RetryPolicy{Base: time.Hour, MaxDelay: time.Hour, MaxAttempts: 5}
	c := NewHTTPClient(srv.URL, nil, longPolicy)
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel shortly after the first attempt is in flight or its backoff has
	// started. With a 1h base the timer will not fire on its own.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.BlockNumber(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("returned in %v, want <2s — backoff was not aborted", elapsed)
	}
	if got := count.Load(); got > 1 {
		t.Fatalf("request count = %d, want 1 (no requests beyond first)", got)
	}
}

func TestBackoffCapBounded(t *testing.T) {
	policy := RetryPolicy{
		Base:        10 * time.Millisecond,
		MaxDelay:    500 * time.Millisecond,
		MaxAttempts: 8,
	}
	const samples = 200
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		want := backoffCap(policy, attempt)
		for i := 0; i < samples; i++ {
			cap := backoffCap(policy, attempt)
			if cap != want {
				t.Fatalf("backoffCap(attempt=%d) inconsistent: got %v then %v", attempt, want, cap)
			}
			if cap < 0 {
				t.Fatalf("backoffCap(attempt=%d) = %v, want >= 0", attempt, cap)
			}
			if cap > policy.MaxDelay {
				t.Fatalf("backoffCap(attempt=%d) = %v, want <= MaxDelay=%v", attempt, cap, policy.MaxDelay)
			}
		}
	}
}

func TestBackoffCapHandlesOverflow(t *testing.T) {
	// Base * 2^shift could overflow int64 for pathological inputs; backoffCap
	// must fall back to MaxDelay rather than producing a negative duration.
	policy := RetryPolicy{
		Base:        time.Hour,
		MaxDelay:    time.Minute,
		MaxAttempts: 10,
	}
	for attempt := 1; attempt <= 35; attempt++ {
		got := backoffCap(policy, attempt)
		if got < 0 || got > policy.MaxDelay {
			t.Fatalf("backoffCap(attempt=%d) = %v, out of bounds [0, %v]", attempt, got, policy.MaxDelay)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"-3", 0},
		{"not-a-number", 0},
		{"7", 7 * time.Second},
		{"  4 ", 4 * time.Second},
		// HTTP-date 90s in the future.
		{now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		// HTTP-date in the past collapses to 0.
		{now.Add(-time.Hour).UTC().Format(http.TimeFormat), 0},
	}
	for _, tc := range cases {
		got := parseRetryAfter(tc.in, now)
		if got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestRetryAfterHonoredOn429(t *testing.T) {
	// Server sends 429 with Retry-After once, then succeeds. The override
	// delay is capped by MaxDelay (fastPolicy's MaxDelay is 100µs), so the
	// test finishes well under the 1s the header asks for.
	srv, count := countingServer(t, func(w http.ResponseWriter, _ *http.Request, n int64) {
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
	})
	c := NewHTTPClient(srv.URL, nil, fastPolicy)
	start := time.Now()
	if _, err := c.BlockNumber(context.Background()); err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("BlockNumber took %v; Retry-After override exceeded MaxDelay cap", elapsed)
	}
	if got := count.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}

func TestNextDelayHonorsOverride(t *testing.T) {
	policy := RetryPolicy{Base: time.Millisecond, MaxDelay: time.Second, MaxAttempts: 3}
	override := 250 * time.Millisecond
	got := nextDelay(policy, 1, &transientError{err: errors.New("x"), overrideDelay: override})
	if got != override {
		t.Errorf("nextDelay with override=%v returned %v, want %v", override, got, override)
	}
	// Override above MaxDelay is capped.
	got = nextDelay(policy, 1, &transientError{err: errors.New("x"), overrideDelay: 10 * time.Second})
	if got != policy.MaxDelay {
		t.Errorf("nextDelay with override>MaxDelay returned %v, want %v", got, policy.MaxDelay)
	}
}


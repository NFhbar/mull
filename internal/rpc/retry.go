package rpc

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"
)

type RetryPolicy struct {
	Base        time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Base:        500 * time.Millisecond,
		MaxDelay:    30 * time.Second,
		MaxAttempts: 5,
	}
}

func applyRetryDefaults(p *RetryPolicy) {
	d := DefaultRetryPolicy()
	if p.Base <= 0 {
		p.Base = d.Base
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = d.MaxDelay
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}
}

// transientError marks an error as eligible for retry. overrideDelay, when
// positive, signals the retry loop to honor a server-provided wait (e.g. an
// HTTP Retry-After header) for the next attempt in lieu of jittered backoff.
type transientError struct {
	err           error
	overrideDelay time.Duration
}

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

func isTransient(err error) bool {
	var te *transientError
	return errors.As(err, &te)
}

// backoffCap returns the upper bound (inclusive) on the jittered delay for the
// given attempt under the policy. Guards against overflow when Base<<shift
// would exceed int64.
func backoffCap(policy RetryPolicy, attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	// Defensive cap for direct-construction callers that bypass config
	// validation (which already constrains MaxAttempts to a small value);
	// the shifted-overflow check below handles all realistic policies.
	if shift > 30 {
		shift = 30
	}
	shifted := policy.Base << shift
	if shifted <= 0 || shifted > policy.MaxDelay {
		return policy.MaxDelay
	}
	return shifted
}

// retry runs fn with bounded exponential-backoff retries on transient errors.
// Caller must pass a defaulted policy (see NewHTTPClient / applyRetryDefaults).
func retry(ctx context.Context, policy RetryPolicy, fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if !isTransient(err) {
			return err
		}
		lastErr = err
		if attempt == policy.MaxAttempts {
			break
		}
		delay := nextDelay(policy, attempt, err)
		slog.Default().DebugContext(ctx, "rpc retry", "attempt", attempt, "delay_ms", delay.Milliseconds())
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	var te *transientError
	if errors.As(lastErr, &te) {
		return te.err
	}
	return lastErr
}

// nextDelay returns the wait before the next retry attempt. When the error
// carries a server-provided override (e.g. Retry-After), it is honored up to
// policy.MaxDelay; otherwise we draw a jittered sample from [0, backoffCap].
func nextDelay(policy RetryPolicy, attempt int, err error) time.Duration {
	var te *transientError
	if errors.As(err, &te) && te.overrideDelay > 0 {
		delay := te.overrideDelay
		if delay > policy.MaxDelay {
			delay = policy.MaxDelay
		}
		return delay
	}
	capDelay := backoffCap(policy, attempt)
	return time.Duration(rand.Int64N(int64(capDelay) + 1))
}

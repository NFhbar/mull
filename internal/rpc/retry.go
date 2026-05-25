package rpc

import (
	"context"
	"errors"
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
	if p.Base == 0 {
		p.Base = d.Base
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = d.MaxDelay
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = d.MaxAttempts
	}
}

type transientError struct {
	err error
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
	if shift > 30 {
		shift = 30
	}
	shifted := policy.Base << shift
	if shifted <= 0 || shifted > policy.MaxDelay {
		return policy.MaxDelay
	}
	return shifted
}

func retry(ctx context.Context, policy RetryPolicy, fn func(ctx context.Context) error) error {
	applyRetryDefaults(&policy)
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
		capDelay := backoffCap(policy, attempt)
		delay := time.Duration(rand.Int64N(int64(capDelay) + 1))
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

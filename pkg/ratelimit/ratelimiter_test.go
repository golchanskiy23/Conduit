package ratelimit

import (
	"conduit/pkg/retry"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errTransient = errors.New("transient error")

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	t.Parallel()

	calls := atomic.Int32{}
	err := retry.Do(context.Background(), func(_ context.Context) error {
		calls.Add(1)
		return nil
	}, retry.Config{MaxAttempts: 3, InitialDelay: time.Millisecond})

	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
}

func TestDo_SuccessAfterRetries(t *testing.T) {
	t.Parallel()

	calls := atomic.Int32{}
	err := retry.Do(context.Background(), func(_ context.Context) error {
		if calls.Add(1) < 3 {
			return errTransient
		}
		return nil
	}, retry.Config{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
	})

	if err != nil {
		t.Fatalf("expected nil after retries, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 calls, got %d", calls.Load())
	}
}

func TestDo_ExhaustsAttempts(t *testing.T) {
	t.Parallel()

	calls := atomic.Int32{}
	err := retry.Do(context.Background(), func(_ context.Context) error {
		calls.Add(1)
		return errTransient
	}, retry.Config{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	})

	if !errors.Is(err, errTransient) {
		t.Fatalf("expected errTransient, got %v", err)
	}
	if calls.Load() != 5 {
		t.Fatalf("expected 5 calls, got %d", calls.Load())
	}
}

func TestDo_ContextCancelledBetweenRetries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	calls := atomic.Int32{}
	err := retry.Do(ctx, func(_ context.Context) error {
		if calls.Add(1) == 1 {
			cancel()
		}
		return errTransient
	}, retry.Config{
		MaxAttempts:  10,
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   1.0,
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call before cancel, got %d", calls.Load())
	}
}

func TestDo_ContextAlreadyCancelledBeforeStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := atomic.Int32{}
	err := retry.Do(ctx, func(_ context.Context) error {
		calls.Add(1)
		return nil
	}, retry.Config{MaxAttempts: 3})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("expected 0 calls, got %d", calls.Load())
	}
}

func TestDo_MaxAttemptsZeroOrNegative(t *testing.T) {
	t.Parallel()

	calls := atomic.Int32{}
	err := retry.Do(context.Background(), func(_ context.Context) error {
		calls.Add(1)
		return errTransient
	}, retry.Config{MaxAttempts: 0, InitialDelay: time.Millisecond})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
}

func TestDo_BackoffDoesNotExceedMaxDelay(t *testing.T) {
	t.Parallel()

	maxDelay := 5 * time.Millisecond
	cfg := retry.Config{
		MaxAttempts:  6,
		InitialDelay: time.Millisecond,
		MaxDelay:     maxDelay,
		Multiplier:   10.0,
		Jitter:       0,
	}

	start := time.Now()
	retry.Do(context.Background(), func(_ context.Context) error {
		return errTransient
	}, cfg)
	elapsed := time.Since(start)

	upperBound := time.Duration(cfg.MaxAttempts) * maxDelay * 3
	if elapsed > upperBound {
		t.Fatalf("elapsed %v exceeds upper bound %v — backoff вышел за MaxDelay", elapsed, upperBound)
	}
}

func TestDo_NoDelayAfterLastAttempt(t *testing.T) {
	t.Parallel()

	cfg := retry.Config{
		MaxAttempts:  2,
		InitialDelay: 500 * time.Millisecond,
		Multiplier:   1.0,
		Jitter:       0,
	}

	start := time.Now()
	retry.Do(context.Background(), func(_ context.Context) error {
		return errTransient
	}, cfg)
	elapsed := time.Since(start)

	if elapsed > 750*time.Millisecond {
		t.Fatalf("elapsed %v — похоже делает задержку после последней попытки", elapsed)
	}
}
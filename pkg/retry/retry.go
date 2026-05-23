package retry

import (
	"context"
	"math/rand"
	"time"
)

type Config struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter float64
}

var Default = Config{
	MaxAttempts:  3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	Multiplier:   2.0,
	Jitter:       0.2,
}


func Do(ctx context.Context, fn func(context.Context) error, cfg Config) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}

	var lastErr error
	delay := cfg.InitialDelay

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.MaxAttempts-1 {
			break
		}

		sleep := jittered(delay, cfg.Jitter)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		delay = time.Duration(float64(delay) * cfg.Multiplier)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}

	return lastErr
}

func jittered(d time.Duration, jitter float64) time.Duration {
	if jitter == 0 {
		return d
	}

	delta := float64(d) * jitter * (rand.Float64()*2 - 1)
	result := time.Duration(float64(d) + delta)
	if result < 0 {
		return 0
	}
	return result
}
package scheduler

import (
	"context"
	"conduit/internal/config"
	"conduit/internal/ds/queue/heap"
	"conduit/internal/pool"
)

type schedulerOptions struct {
	execute func(context.Context, *heap.Item) error
	cfg     config.WorkerPoolConfig
	onError func(string, error)
	pool    pool.WorkerPooler
}

type Option func(*schedulerOptions)

func WithTaskExecutor(fn func(context.Context, *heap.Item) error) Option {
	return func(s *schedulerOptions) {
		s.execute = fn
	}
}

func WithPoolConfig(cfg config.WorkerPoolConfig) Option {
	return func(s *schedulerOptions) {
		s.cfg = cfg
	}
}

func WithTaskOnError(fn func(string, error)) Option {
	return func(s *schedulerOptions) {
		s.onError = fn
	}
}

func WithPool(wp pool.WorkerPooler) Option {
	return func(s *schedulerOptions) {
		s.pool = wp
	}
}
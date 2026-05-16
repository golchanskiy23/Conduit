package scheduler

import (
	"context"
	"conduit/config"
)

type schedulerOptions struct {
	execute func(context.Context, *Item) error
	cfg     config.WorkerPoolConfig
	onError func(string, error)
	pool    WorkerPool
}

type Option func(*schedulerOptions)

func WithTaskExecutor(fn func(context.Context, *Item) error) Option {
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

func WithPool(wp WorkerPool) Option {
	return func(s *schedulerOptions) {
		s.pool = wp
	}
}
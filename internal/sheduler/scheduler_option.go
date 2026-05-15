package sheduler

import(
	"context"
)

type schedulerOptions struct{
	execute func(context.Context, *Item) error
	cfg WorkerPoolConfig
	onError func(string, error)
}

type Option func(*schedulerOptions)

func WithExecutor(fn func(context.Context, *Item) error) Option {
    return func(s *schedulerOptions) {
        s.execute = fn
    }
}

func WithPoolConfig(cfg WorkerPoolConfig) Option {
    return func(s *schedulerOptions) {
        s.cfg = cfg
    }
}

func WithOnError(fn func(string, error)) Option {
    return func(s *schedulerOptions) {
        s.onError = fn
    }
}

// для тестов — подменяем пул моком
/*func WithPool(wp workerPoolInterface) Option {
    return func(s *Scheduler) {
        s.wp = wp
    }
}*/
package scheduler

import "context"

type workerPoolOptions struct{
	execute func(context.Context, *Item) error
	onDone func(string)
	onError func(string, error)
}

type workerOption func(*workerPoolOptions)

func WithExecutor(fn func(context.Context, *Item) error) workerOption {
    return func(o *workerPoolOptions) {
        o.execute = fn
    }
}

func WithOnDone(fn func(string)) workerOption {
    return func(o *workerPoolOptions) {
        o.onDone = fn
    }
}

func WithOnError(fn func(string, error)) workerOption {
    return func(o *workerPoolOptions) {
        o.onError = fn
    }
}
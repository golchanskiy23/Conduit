package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"conduit/config"
	"log"
)

type WorkerPool interface {
	TryExecute(job *Item) bool
	Shutdown(ctx context.Context) error
}

type workerPool struct {
	jobs    chan *Item
	wg      sync.WaitGroup
	cfg     config.WorkerPoolConfig
	closed  atomic.Bool
	execute func(context.Context, *Item) error
	onDone  func(string)
	onError func(string, error)
}

func newWorkerPool(cfg config.WorkerPoolConfig, options ...workerOption) *workerPool {
	opts := &workerPoolOptions{
        onError: func(id string, err error) {
            log.Printf("job %s error: %v", id, err)
        },
    }

	for _, opt := range options{
		opt(opts)
	}

	return &workerPool{
        jobs:    make(chan *Item, cfg.BufferSize),
        cfg:     cfg,
        execute: opts.execute,
        onDone:  opts.onDone,
        onError: opts.onError,
    }
}

func (pool *workerPool) Start(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		pool.wg.Add(1)
		go pool.worker(ctx)
	}
}

func (pool *workerPool) worker(ctx context.Context) {
	defer pool.wg.Done()
	for job := range pool.jobs {
		func() {
			jobCtx, cancel := context.WithTimeout(ctx, pool.cfg.JobTimeout)
			defer cancel()
			if err := pool.execute(jobCtx, job); err != nil {
				pool.onError(job.JobID, err)
				return
			}
			pool.onDone(job.JobID)
		}()
	}
}

func (pool *workerPool) TryExecute(job *Item) bool {
	if pool.closed.Load() {
		return false
	}
	select {
	case pool.jobs <- job:
		return true
	default:
		return false
	}
}

func (pool *workerPool) Shutdown(ctx context.Context) error {
	pool.closed.Store(true)
	close(pool.jobs)

	done := make(chan struct{})
	go func() {
		pool.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker pool shutdown timeout: %w", ctx.Err())
	}
}
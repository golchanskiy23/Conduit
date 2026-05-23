package pool

import (
	"conduit/internal/config"
	"conduit/internal/ds/queue/heap"
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type WorkerPooler interface {
	TryExecute(job *heap.Item) bool
	Shutdown(ctx context.Context) error
	Start(context.Context, int)
}

type WorkerPool struct {
	jobs    chan *heap.Item
	wg      *sync.WaitGroup
	cfg     config.WorkerPoolConfig
	mu 		sync.RWMutex
	closed  atomic.Bool
	execute func(context.Context, *heap.Item) error
	onDone  func(string)
	onError func(string, error)
}

type JobTyper interface{
	Handles(jobType string) bool 
	Execute(ctx context.Context, item *heap.Item) error
}

func NewWorkerPool(cfg config.WorkerPoolConfig, worker JobTyper, options ...workerOption) *WorkerPool {
	opts := &workerPoolOptions{
        onError: func(id string, err error) {
            log.Printf("job %s error: %v", id, err)
        },
    }

	for _, opt := range options{
		opt(opts)
	}

	return &WorkerPool{
        jobs:    make(chan *heap.Item, cfg.BufferSize),
        cfg:     cfg,
		wg:      &sync.WaitGroup{},
        execute: opts.execute,
        onDone:  opts.onDone,
        onError: opts.onError,
    }
}

func (pool *WorkerPool) Start(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		pool.wg.Add(1)
		go pool.worker(ctx)
	}
}

func (pool *WorkerPool) worker(ctx context.Context) {
	defer pool.wg.Done()
	for job := range pool.jobs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					pool.onError(job.JobID, fmt.Errorf("panic: %v", r))
				}
			}()
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

func (pool *WorkerPool) TryExecute(job *heap.Item) bool {
	pool.mu.RLock()
    defer pool.mu.RUnlock()

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

func (pool *WorkerPool) Shutdown(ctx context.Context) error {	
	pool.mu.Lock()
	if pool.closed.Swap(true){
		pool.mu.Unlock()
		return nil
	}

	close(pool.jobs)
	pool.mu.Unlock()

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

func CreatePools(cfg config.WorkerPoolConfig , types []JobTyper, options [][]workerOption) []WorkerPooler{
	for _, val 

	return ans
}
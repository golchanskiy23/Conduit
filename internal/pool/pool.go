package pool

import (
	"conduit/internal/config"
	"conduit/internal/ds/queue/heap"
	"conduit/pkg/retry"
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type WorkerPooler interface {
	TryExecute(job *heap.Item) bool
	Shutdown(ctx context.Context) error
	Start(context.Context)
	Worker() Worker
	SetOnDone(func(string))
}

type WorkerPool struct {
	jobs    chan *heap.Item
	wg      *sync.WaitGroup
	cfg     config.WorkerPoolConfig
	mu      sync.RWMutex
	worker  Worker
	closed  atomic.Bool
	onDone  func(string)
	onError func(string, error)
	// retryCfg — если не nil, каждый job выполняется через retry.Do.
	retryCfg *retry.Config
}

func NewWorkerPool(cfg config.WorkerPoolConfig, worker Worker, options ...workerOption) *WorkerPool {
	p := &WorkerPool{
		jobs:   make(chan *heap.Item, cfg.BufferSize),
		cfg:    cfg,
		wg:     &sync.WaitGroup{},
		worker: worker,
		onDone: func(string) {},
		onError: func(id string, err error) {
			log.Printf("job %s error: %v", id, err)
		},
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func (pool *WorkerPool) Worker() Worker {
	return pool.worker
}

func (pool *WorkerPool) SetOnDone(fn func(string)) {
	pool.onDone = fn
}

func (pool *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < pool.cfg.WorkersNum; i++ {
		pool.wg.Add(1)
		go pool.run(ctx)
	}
}

func (pool *WorkerPool) run(ctx context.Context) {
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

			var err error
			if pool.retryCfg != nil {
				err = retry.Do(jobCtx, func(ctx context.Context) error {
					return pool.worker.Execute(ctx, job)
				}, *pool.retryCfg)
			} else {
				err = pool.worker.Execute(jobCtx, job)
			}

			if err != nil {
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
	if pool.closed.Swap(true) {
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
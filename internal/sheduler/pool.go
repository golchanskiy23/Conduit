package sheduler

import (
	"context"
	"sync"
	"time"
	"fmt"
)

type WorkerPoolConfig struct {
    BufferSize int
    JobTimeout time.Duration
}

// добавить интерфейс, чтобы в пушить любой пул в scheduer constructor
type workerPool struct {
    jobs    chan *Item
    wg      sync.WaitGroup
    cfg     WorkerPoolConfig
    onDone  func(string)
    onError func(string, error)
    execute func(context.Context, *Item) error
}

func newWorkerPool(cfg WorkerPoolConfig, execute func(context.Context, *Item) error, onDone func(string), onError func(string, error)) *workerPool {
    return &workerPool{
        jobs:    make(chan *Item, cfg.BufferSize),
        cfg:     cfg,
        execute: execute,
        onDone:  onDone,
        onError: onError,
    }
}

func (pool *workerPool) Start(ctx context.Context, n int) {
    for i := 0; i < n; i++ {
        pool.wg.Add(1)
        go pool.worker(ctx)
    }
}

func (pool *workerPool) worker(ctx context.Context){
	defer pool.wg.Done()
	for job := range pool.jobs{
		func() {
            jobCtx, cancel := context.WithTimeout(ctx, pool.cfg.JobTimeout)
            defer cancel()
            if err := pool.execute(jobCtx, job); err != nil {
                pool.onError(job.JobID, err)
            }
            pool.onDone(job.JobID)
        }()
	}
}

func (pool *workerPool) Execute(job *Item){
	pool.jobs <- job
}

func (pool *workerPool) Shutdown(ctx context.Context) error{
	close(pool.jobs)
	done := make(chan struct{})

	// зачем передавать параметры, если анонимной функции видно всё в этой функции?
	go func(){
		pool.wg.Wait()
		close(done)
	}()

	select{
	case <-done:
		return nil
	case <- ctx.Done():
		return fmt.Errorf("worker pool timeout shutdown: %w", ctx.Err())
	}
}
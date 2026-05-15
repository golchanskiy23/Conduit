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
type WorkerPool struct{
	Jobs chan *Item
	Wg sync.WaitGroup
	Cfg WorkerPoolConfig

	onDone func(string)
    onError func(string, error)
	toExecution func(context.Context, *Item) error
}

func newWorkerPool(cfg WorkerPoolConfig, execute func(context.Context, *Item) error, onDone func(string), onError func(string, error)) *WorkerPool {
    return &WorkerPool{
        Jobs:    make(chan *Item, cfg.BufferSize),
        Cfg:     cfg,
        toExecution: execute,
        onDone:  onDone,
        onError: onError,
    }
}

func (pool *WorkerPool) Worker(ctx context.Context){
	defer pool.Wg.Done()
	for job := range pool.Jobs{
		func() {
            jobCtx, cancel := context.WithTimeout(ctx, pool.Cfg.JobTimeout)
            defer cancel()
            if err := pool.toExecution(jobCtx, job); err != nil {
                pool.onError(job.JobID, err)
            }
            pool.onDone(job.JobID)
        }()
	}
}

func (pool *WorkerPool) Execute(job *Item){
	pool.Jobs <- job
}

func (pool *WorkerPool) Shutdown(ctx context.Context) error{
	close(pool.Jobs)
	done := make(chan struct{})

	// зачем передавать параметры, если анонимной функции видно всё в этой функции?
	go func(){
		pool.Wg.Wait()
		close(done)
	}()

	select{
	case <-done:
		return nil
	case <- ctx.Done():
		return fmt.Errorf("worker pool timeout shutdown: %w", ctx.Err())
	}
}
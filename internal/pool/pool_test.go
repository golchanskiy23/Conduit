package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"conduit/internal/config"
	"conduit/internal/ds/queue/heap"
)

func testPoolCfg() config.WorkerPoolConfig {
	return config.WorkerPoolConfig{
		BufferSize: 10,
		JobTimeout: time.Second,
	}
}

func TestWorkerPool_ExecutesJob(t *testing.T) {
	var executed atomic.Int32
	done := make(chan string, 1)

	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error {
			executed.Add(1)
			return nil
		}),
		WithOnDone(func(id string) { done <- id }),
	)
	pool.Start(context.Background(), 2)

	job := &heap.Item{JobID: "job1"}
	if !pool.TryExecute(job) {
		t.Fatal("TryExecute returned false")
	}

	select {
	case id := <-done:
		if id != "job1" {
			t.Errorf("expected job1, got %s", id)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for job completion")
	}

	if executed.Load() != 1 {
		t.Errorf("expected 1 execution, got %d", executed.Load())
	}
}

func TestWorkerPool_OnErrorCalledOnFailure(t *testing.T) {
	errCh := make(chan error, 1)
	doneCh := make(chan string, 1)

	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error {
			return errors.New("job failed")
		}),
		WithOnDone(func(id string) { doneCh <- id }),
		WithOnError(func(id string, err error) { errCh <- err }),
	)
	pool.Start(context.Background(), 1)

	pool.TryExecute(&heap.Item{JobID: "failing"})

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error callback")
	}

	select {
	case id := <-doneCh:
		t.Errorf("onDone should not be called on error, got %s", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWorkerPool_TryExecuteReturnsFalseWhenFull(t *testing.T) {
	cfg := config.WorkerPoolConfig{
		BufferSize: 1,
		JobTimeout: time.Second,
	}

	pool := NewWorkerPool(
		cfg,
		WithExecutor(func(ctx context.Context, item *heap.Item) error { return nil }),
		WithOnDone(func(id string) {}),
	)

	pool.TryExecute(&heap.Item{JobID: "fill"})

	ok := pool.TryExecute(&heap.Item{JobID: "overflow"})
	if ok {
		t.Error("expected TryExecute to return false when buffer full")
	}
}

func TestWorkerPool_TryExecuteReturnsFalseAfterShutdown(t *testing.T) {
	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error { return nil }),
		WithOnDone(func(id string) {}),
	)
	pool.Start(context.Background(), 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pool.Shutdown(ctx)

	ok := pool.TryExecute(&heap.Item{JobID: "after-shutdown"})
	if ok {
		t.Error("expected TryExecute to return false after shutdown")
	}
}

func TestWorkerPool_ShutdownWaitsForWorkers(t *testing.T) {
	var inProgress atomic.Bool
	started := make(chan struct{})

	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error {
			inProgress.Store(true)
			close(started)
			time.Sleep(100 * time.Millisecond)
			inProgress.Store(false)
			return nil
		}),
		WithOnDone(func(id string) {}),
	)
	pool.Start(context.Background(), 1)
	pool.TryExecute(&heap.Item{JobID: "slow"})

	<-started

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pool.Shutdown(ctx)

	if inProgress.Load() {
		t.Error("shutdown returned before job finished")
	}
}

func TestWorkerPool_ShutdownTimeout(t *testing.T) {
	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error {
			time.Sleep(time.Second)
			return nil
		}),
		WithOnDone(func(id string) {}),
	)
	pool.Start(context.Background(), 1)
	pool.TryExecute(&heap.Item{JobID: "slow"})
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := pool.Shutdown(ctx)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestWorkerPool_ShutdownIdempotent(t *testing.T) {
	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error { return nil }),
		WithOnDone(func(id string) {}),
	)
	pool.Start(context.Background(), 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pool.Shutdown(ctx)
	pool.Shutdown(ctx)
}

func TestWorkerPool_PanicRecovery(t *testing.T) {
	errCh := make(chan error, 1)

	pool := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error {
			panic("unexpected panic")
		}),
		WithOnDone(func(id string) {}),
		WithOnError(func(id string, err error) { errCh <- err }),
	)
	pool.Start(context.Background(), 1)
	pool.TryExecute(&heap.Item{JobID: "panicking"})

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected panic error")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout — panic not recovered")
	}

	doneCh := make(chan string, 1)
	pool2 := NewWorkerPool(
		testPoolCfg(),
		WithExecutor(func(ctx context.Context, item *heap.Item) error { return nil }),
		WithOnDone(func(id string) { doneCh <- id }),
	)
	pool2.Start(context.Background(), 1)
	pool2.TryExecute(&heap.Item{JobID: "after-panic"})

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("worker stopped after panic")
	}
}

func TestWorkerPool_ConcurrentTryExecute(t *testing.T) {
	var count atomic.Int32
	var wg sync.WaitGroup

	pool := NewWorkerPool(
		config.WorkerPoolConfig{BufferSize: 200, JobTimeout: time.Second},
		WithExecutor(func(ctx context.Context, item *heap.Item) error {
			count.Add(1)
			return nil
		}),
		WithOnDone(func(id string) { wg.Done() }),
	)
	pool.Start(context.Background(), 4)

	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(n int) {
			pool.TryExecute(&heap.Item{JobID: string(rune('a' + n%26))})
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for concurrent jobs")
	}
}
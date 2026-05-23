package scheduler

import (
	"conduit/internal/config"
	"conduit/internal/ds/graph"
	"conduit/internal/ds/queue/heap"
	"conduit/internal/pool"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testWorker struct {
	jobType string
	fn      func(*heap.Item)
}

func (w *testWorker) Handles(jobType string) bool {
	return w.jobType == "*" || w.jobType == jobType
}

func (w *testWorker) Execute(ctx context.Context, item *heap.Item) error {
	if w.fn != nil {
		w.fn(item)
	}
	return nil
}

type mockPool struct {
    mu       sync.Mutex
    executed []*heap.Item
    closed   bool
    worker   *testWorker
    onDone   func(string)
    onExec   func(*heap.Item)
}

func newMockPool(jobType string) *mockPool {
    return &mockPool{worker: &testWorker{jobType: jobType}}
}

func (m *mockPool) Worker() pool.Worker { return m.worker }

func (m *mockPool) SetOnDone(fn func(string)) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.onDone = fn
}

func (m *mockPool) Start(ctx context.Context) {}

func (m *mockPool) TryExecute(job *heap.Item) bool {
    m.mu.Lock()
    if m.closed {
        m.mu.Unlock()
        return false
    }
    m.executed = append(m.executed, job)
    onExec := m.onExec
    m.mu.Unlock()

    if onExec != nil {
        go onExec(job)
    }
    return true
}

func (m *mockPool) Shutdown(_ context.Context) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.closed = true
    return nil
}

func (m *mockPool) setOnExec(fn func(*heap.Item)) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.onExec = fn
}

func (m *mockPool) getExecuted() []*heap.Item {
    m.mu.Lock()
    defer m.mu.Unlock()
    cp := make([]*heap.Item, len(m.executed))
    copy(cp, m.executed)
    return cp
}

type limitedMockPool struct {
	mu       sync.Mutex
	executed []*heap.Item
	limited  bool
	count    int
	limit    int
	worker   pool.Worker
}

func (l *limitedMockPool) Worker() pool.Worker        { return l.worker }
func (l *limitedMockPool) SetOnDone(fn func(string))  {}
func (l *limitedMockPool) Start(ctx context.Context) {}

func (l *limitedMockPool) TryExecute(job *heap.Item) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limited && l.count >= l.limit {
		return false
	}
	l.executed = append(l.executed, job)
	l.count++
	return true
}

func (l *limitedMockPool) removeLimit(wake func()) {
	l.mu.Lock()
	l.limited = false
	l.mu.Unlock()
	if wake != nil {
		wake()
	}
}

func (l *limitedMockPool) Shutdown(_ context.Context) error { return nil }

func (l *limitedMockPool) getAll() []*heap.Item {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]*heap.Item, len(l.executed))
	copy(cp, l.executed)
	return cp
}

func newTestScheduler(p pool.WorkerPooler) *Scheduler {
	cfg := &config.Config{}
	s := NewScheduler(cfg)
	s.Register(p)
	return s
}

func newTestItem(id string) *heap.Item {
	return &heap.Item{JobID: id, JobType: "test.job"}
}

func TestScheduler_SubmitEnqueuesImmediately(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(newTestItem("job1"), nil)
	time.Sleep(50 * time.Millisecond)

	if len(mock.getExecuted()) == 0 {
		t.Error("expected job to be executed")
	}
}

func TestScheduler_SubmitDelayed(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	item := newTestItem("delayed")
	item.RunAt = time.Now().Add(100 * time.Millisecond)
	s.Submit(item, nil)

	time.Sleep(20 * time.Millisecond)
	if len(mock.getExecuted()) > 0 {
		t.Error("delayed job should not execute immediately")
	}

	time.Sleep(150 * time.Millisecond)
	if len(mock.getExecuted()) == 0 {
		t.Error("delayed job should have executed after RunAt")
	}
}

func TestScheduler_SubmitDuplicateReturnsError(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	s.Submit(newTestItem("dup"), nil)
	err := s.Submit(newTestItem("dup"), nil)
	if !errors.Is(err, graph.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestScheduler_SubmitUnknownDepReturnsError(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	err := s.Submit(newTestItem("B"), []string{"A"})
	if err == nil {
		t.Error("expected error for unknown dependency")
	}
}

func TestScheduler_SubmitNoWorkerReturnsError(t *testing.T) {
	mock := newMockPool("payment.charge")
	s := newTestScheduler(mock)

	err := s.Submit(&heap.Item{JobID: "x", JobType: "email.send"}, nil)
	if !errors.Is(err, ErrNoSuchWorker) {
		t.Errorf("expected ErrNoWorker, got %v", err)
	}
}

func TestScheduler_SubmitWithDepsNotEnqueuedImmediately(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(newTestItem("A"), nil)
	s.Submit(newTestItem("B"), []string{"A"})

	time.Sleep(50 * time.Millisecond)

	for _, item := range mock.getExecuted() {
		if item.JobID == "B" {
			t.Error("B should not execute before A completes")
		}
	}
}

func TestScheduler_OnDoneUnlocksDependents(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	aDone := make(chan struct{})
	mock.setOnExec(func(item *heap.Item) {
		if item.JobID == "A" {
			close(aDone)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(newTestItem("A"), nil)
	s.Submit(newTestItem("B"), []string{"A"})

	select {
	case <-aDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for A")
	}

	s.OnDone("A")
	time.Sleep(50 * time.Millisecond)

	found := false
	for _, item := range mock.getExecuted() {
		if item.JobID == "B" {
			found = true
		}
	}
	if !found {
		t.Error("B should be enqueued after A completes")
	}
}

func TestScheduler_ChainABC(t *testing.T) {
	var order []string
	var mu sync.Mutex
	var s *Scheduler

	mock := newMockPool("*")
	cfg := &config.Config{}
	s = NewScheduler(cfg)
	mock.setOnExec(func(item *heap.Item) {
		mu.Lock()
		order = append(order, item.JobID)
		mu.Unlock()
		s.OnDone(item.JobID)
	})
	s.Register(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(newTestItem("A"), nil)
	s.Submit(newTestItem("B"), []string{"A"})
	s.Submit(newTestItem("C"), []string{"B"})

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Fatalf("expected 3 jobs executed, got %d: %v", len(order), order)
	}
	if order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Errorf("expected A→B→C, got %v", order)
	}
}

func TestScheduler_DiamondDependency(t *testing.T) {
	var count atomic.Int32
	var s *Scheduler

	mock := newMockPool("*")
	cfg := &config.Config{}
	s = NewScheduler(cfg)
	mock.setOnExec(func(item *heap.Item) {
		count.Add(1)
		s.OnDone(item.JobID)
	})
	s.Register(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(newTestItem("A"), nil)
	s.Submit(newTestItem("B"), []string{"A"})
	s.Submit(newTestItem("C"), []string{"A"})
	s.Submit(newTestItem("D"), []string{"B", "C"})

	time.Sleep(300 * time.Millisecond)

	if count.Load() != 4 {
		t.Errorf("expected 4 executions, got %d", count.Load())
	}
}

func TestScheduler_WaitReturnsAfterContextCancel(t *testing.T) {
	mock := newMockPool("*")
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	cancel()

	done := make(chan struct{})
	go func() {
		s.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait() did not return after context cancel")
	}
}

func TestScheduler_OverflowRequeued(t *testing.T) {
	limited := &limitedMockPool{
		limited: true,
		limit:   1,
		worker:  &testWorker{jobType: "*"},
	}
	s := newTestScheduler(limited)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(newTestItem("job1"), nil)
	s.Submit(newTestItem("job2"), nil)

	time.Sleep(150 * time.Millisecond)

	limited.removeLimit(s.Wake)
	time.Sleep(200 * time.Millisecond)

	if len(limited.getAll()) < 2 {
		t.Errorf("expected both jobs eventually executed, got %d", len(limited.getAll()))
	}
}

func TestScheduler_DispatchesToCorrectPool(t *testing.T) {
	paymentPool := newMockPool("payment.charge")
	emailPool := newMockPool("email.send")
	defaultPool := newMockPool("*")

	cfg := &config.Config{}
	s := NewScheduler(cfg)
	s.Register(paymentPool, emailPool, defaultPool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&heap.Item{JobID: "p1", JobType: "payment.charge"}, nil)
	s.Submit(&heap.Item{JobID: "e1", JobType: "email.send"}, nil)
	time.Sleep(50 * time.Millisecond)

	if len(paymentPool.getExecuted()) != 1 {
		t.Errorf("expected 1 job in payment pool, got %d", len(paymentPool.getExecuted()))
	}
	if len(emailPool.getExecuted()) != 1 {
		t.Errorf("expected 1 job in email pool, got %d", len(emailPool.getExecuted()))
	}
}

func TestScheduler_FallsBackToDefaultPool(t *testing.T) {
	paymentPool := newMockPool("payment.charge")
	defaultPool := newMockPool("*")

	cfg := &config.Config{}
	s := NewScheduler(cfg)
	s.Register(paymentPool, defaultPool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&heap.Item{JobID: "r1", JobType: "report.generate"}, nil)
	time.Sleep(50 * time.Millisecond)

	if len(defaultPool.getExecuted()) != 1 {
		t.Errorf("expected job in default pool, got %d", len(defaultPool.getExecuted()))
	}
}
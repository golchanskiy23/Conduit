package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"conduit/config"
)

type mockPool struct {
	mu       sync.Mutex
	executed []*Item
	closed   bool
	onExec   func(*Item)
}

func (m *mockPool) TryExecute(job *Item) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.executed = append(m.executed, job)
	if m.onExec != nil {
		go m.onExec(job)
	}
	return true
}

func (m *mockPool) Shutdown(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockPool) getExecuted() []*Item {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*Item, len(m.executed))
	copy(cp, m.executed)
	return cp
}

type limitedMockPool struct {
	mu       sync.Mutex
	executed []*Item
	limited  bool
	count    int
	limit    int
	onAccept func() // вызывается при успешном TryExecute
}

func (l *limitedMockPool) TryExecute(job *Item) bool {
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

func (l *limitedMockPool) getAll() []*Item {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]*Item, len(l.executed))
	copy(cp, l.executed)
	return cp
}

func newTestScheduler(pool WorkerPool) *Scheduler {
	cfg := &config.Config{
		PoolCfg:    config.WorkerPoolConfig{BufferSize: 10, JobTimeout: time.Second},
		WorkersNum: 2,
	}
	return NewScheduler(
		cfg,
		WithTaskExecutor(func(ctx context.Context, item *Item) error { return nil }),
		WithPool(pool),
	)
}

func TestScheduler_SubmitEnqueuesImmediately(t *testing.T) {
	mock := &mockPool{}
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{JobID: "job1", Priority: PriorityNormal}, nil)

	time.Sleep(50 * time.Millisecond)

	if len(mock.getExecuted()) == 0 {
		t.Error("expected job to be in executed")
	}
}

func TestScheduler_SubmitDelayed(t *testing.T) {
	mock := &mockPool{}
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{
		JobID:    "delayed",
		Priority: PriorityNormal,
		RunAt:    time.Now().Add(100 * time.Millisecond),
	}, nil)

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
	mock := &mockPool{}
	s := newTestScheduler(mock)

	s.Submit(&Item{JobID: "dup"}, nil)

	err := s.Submit(&Item{JobID: "dup"}, nil)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestScheduler_SubmitUnknownDepReturnsError(t *testing.T) {
	mock := &mockPool{}
	s := newTestScheduler(mock)

	err := s.Submit(&Item{JobID: "B"}, []string{"A"})
	if err == nil {
		t.Error("expected error for unknown dependency")
	}
}

func TestScheduler_SubmitWithDepsNotEnqueuedImmediately(t *testing.T) {
	mock := &mockPool{}
	s := newTestScheduler(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{JobID: "A"}, nil)
	s.Submit(&Item{JobID: "B"}, []string{"A"})

	time.Sleep(50 * time.Millisecond)

	for _, item := range mock.getExecuted() {
		if item.JobID == "B" {
			t.Error("B should not execute before A completes")
		}
	}
}

func TestScheduler_OnDoneUnlocksDependents(t *testing.T) {
	mock := &mockPool{}
	s := newTestScheduler(mock)

	aDone := make(chan struct{})
	mock.onExec = func(item *Item) {
		if item.JobID == "A" {
			close(aDone)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{JobID: "A"}, nil)
	s.Submit(&Item{JobID: "B"}, []string{"A"})

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

	mock := &mockPool{}
	s := newTestScheduler(mock)

	mock.onExec = func(item *Item) {
		mu.Lock()
		order = append(order, item.JobID)
		mu.Unlock()
		s.OnDone(item.JobID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{JobID: "A"}, nil)
	s.Submit(&Item{JobID: "B"}, []string{"A"})
	s.Submit(&Item{JobID: "C"}, []string{"B"})

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Fatalf("expected 3 jobs executed, got %d: %v", len(order), order)
	}
	if order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Errorf("expected A→B→C order, got %v", order)
	}
}

func TestScheduler_DiamondDependency(t *testing.T) {
	var count atomic.Int32
	mock := &mockPool{}
	s := newTestScheduler(mock)

	mock.onExec = func(item *Item) {
		count.Add(1)
		s.OnDone(item.JobID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{JobID: "A"}, nil)
	s.Submit(&Item{JobID: "B"}, []string{"A"})
	s.Submit(&Item{JobID: "C"}, []string{"A"})
	s.Submit(&Item{JobID: "D"}, []string{"B", "C"})

	time.Sleep(300 * time.Millisecond)

	if count.Load() != 4 {
		t.Errorf("expected 4 executions, got %d", count.Load())
	}
}

func TestScheduler_WaitReturnsAfterContextCancel(t *testing.T) {
	mock := &mockPool{}
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
	limitedPool := &limitedMockPool{limited: true, limit: 1}
	s := newTestScheduler(limitedPool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	s.Submit(&Item{JobID: "job1"}, nil)
	s.Submit(&Item{JobID: "job2"}, nil)

	time.Sleep(150 * time.Millisecond)

	limitedPool.removeLimit(s.Wake)
	time.Sleep(200 * time.Millisecond)

	if len(limitedPool.getAll()) < 2 {
		t.Errorf("expected both jobs eventually executed, got %d", len(limitedPool.getAll()))
	}
}
package delayed

import (
	"conduit/internal/ds/queue"
	"conduit/internal/ds/queue/heap"
	"testing"
	"time"
)

func TestDelayedQueue_AddAndPoll(t *testing.T) {
	dq := NewDelayedQueue()

	now := time.Now()
	past := &heap.Item{JobID: "past", RunAt: now.Add(-time.Second)}
	future := &heap.Item{JobID: "future", RunAt: now.Add(time.Hour)}

	dq.Add(past)
	dq.Add(future)

	ready := dq.Poll(now)
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready item, got %d", len(ready))
	}
	if ready[0].JobID != "past" {
		t.Errorf("expected 'past', got %s", ready[0].JobID)
	}
}

func TestDelayedQueue_PollEmpty(t *testing.T) {
	dq := NewDelayedQueue()
	ready := dq.Poll(time.Now())
	if len(ready) != 0 {
		t.Errorf("expected empty slice, got %d items", len(ready))
	}
}

func TestDelayedQueue_PollOrder(t *testing.T) {
	dq := NewDelayedQueue()
	now := time.Now()

	dq.Add(&heap.Item{JobID: "third", RunAt: now.Add(-time.Second)})
	dq.Add(&heap.Item{JobID: "first", RunAt: now.Add(-3 * time.Second)})
	dq.Add(&heap.Item{JobID: "second", RunAt: now.Add(-2 * time.Second)})

	ready := dq.Poll(now)
	if len(ready) != 3 {
		t.Fatalf("expected 3 items, got %d", len(ready))
	}

	expected := []string{"first", "second", "third"}
	for i, item := range ready {
		if item.JobID != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], item.JobID)
		}
	}
}

func TestDelayedQueue_PollDoesNotReturnFuture(t *testing.T) {
	dq := NewDelayedQueue()
	dq.Add(&heap.Item{JobID: "future", RunAt: time.Now().Add(time.Hour)})

	ready := dq.Poll(time.Now())
	if len(ready) != 0 {
		t.Errorf("expected 0 items, got %d", len(ready))
	}
}

func TestDelayedQueue_Next(t *testing.T) {
	dq := NewDelayedQueue()

	_, err := dq.Next()
	if err != queue.ErrEmptyQueue {
		t.Errorf("expected ErrEmptyQueue, got %v", err)
	}

	runAt := time.Now().Add(time.Minute)
	dq.Add(&heap.Item{JobID: "job", RunAt: runAt})

	next, err := dq.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !next.Equal(runAt) {
		t.Errorf("expected %v, got %v", runAt, next)
	}
}

func TestDelayedQueue_NextReturnsEarliest(t *testing.T) {
	dq := NewDelayedQueue()
	now := time.Now()

	dq.Add(&heap.Item{JobID: "later", RunAt: now.Add(2 * time.Minute)})
	dq.Add(&heap.Item{JobID: "earlier", RunAt: now.Add(time.Minute)})

	next, err := dq.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !next.Equal(now.Add(time.Minute)) {
		t.Errorf("expected earliest RunAt, got %v", next)
	}
}

func TestDelayedQueue_ConcurrentAddPoll(t *testing.T) {
	dq := NewDelayedQueue()
	done := make(chan struct{})

	go func() {
		for i := 0; i < 100; i++ {
			dq.Add(&heap.Item{JobID: "job", RunAt: time.Now().Add(-time.Millisecond)})
		}
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			dq.Poll(time.Now())
		}
	}
}
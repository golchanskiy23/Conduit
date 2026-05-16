package scheduler

import (
	"testing"
	"time"
)

func makeItem(id string, priority Priority) *Item {
	return &Item{
		JobID:      id,
		Priority:   priority,
		RunAt:      time.Now().Add(time.Minute),
		EnqueuedAt: time.Now(),
	}
}

func TestPriorityQueue_PushPop(t *testing.T) {
	pq := NewPriorityQueue()

	pq.Push(makeItem("low", PriorityLow))
	pq.Push(makeItem("high", PriorityHigh))
	pq.Push(makeItem("normal", PriorityNormal))

	item, err := pq.Pop()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item.JobID != "high" {
		t.Errorf("expected 'high' first, got %s", item.JobID)
	}

	item, err = pq.Pop()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item.JobID != "normal" {
		t.Errorf("expected 'normal' second, got %s", item.JobID)
	}
}

func TestPriorityQueue_PopEmpty(t *testing.T) {
	pq := NewPriorityQueue()

	_, err := pq.Pop()
	if err != ErrEmptyQueue {
		t.Errorf("expected ErrEmptyQueue, got %v", err)
	}
}

func TestPriorityQueue_SamePriorityFIFO(t *testing.T) {
	pq := NewPriorityQueue()

	now := time.Now()
	pq.Push(&Item{
		JobID:      "first",
		Priority:   PriorityNormal,
		RunAt:      now.Add(time.Minute),
		EnqueuedAt: now,
	})
	pq.Push(&Item{
		JobID:      "second",
		Priority:   PriorityNormal,
		RunAt:      now.Add(time.Minute),
		EnqueuedAt: now.Add(time.Millisecond),
	})

	item, _ := pq.Pop()
	if item.JobID != "first" {
		t.Errorf("expected FIFO order, got %s first", item.JobID)
	}
}

func TestPriorityQueue_Update(t *testing.T) {
	pq := NewPriorityQueue()

	pq.Push(makeItem("low", PriorityLow))
	normal := makeItem("normal", PriorityNormal)
	pq.Push(normal)

	pq.Update(normal, PriorityCritical)

	item, _ := pq.Pop()
	if item.JobID != "normal" {
		t.Errorf("expected updated item first, got %s", item.JobID)
	}
}

func TestPriorityQueue_Len(t *testing.T) {
	pq := NewPriorityQueue()

	if pq.Len() != 0 {
		t.Errorf("expected 0, got %d", pq.Len())
	}

	pq.Push(makeItem("a", PriorityLow))
	pq.Push(makeItem("b", PriorityHigh))

	if pq.Len() != 2 {
		t.Errorf("expected 2, got %d", pq.Len())
	}

	pq.Pop()

	if pq.Len() != 1 {
		t.Errorf("expected 1, got %d", pq.Len())
	}
}

func TestPriorityQueue_CriticalBeforeLow(t *testing.T) {
	pq := NewPriorityQueue()

	pq.Push(makeItem("low", PriorityLow))
	pq.Push(makeItem("critical", PriorityCritical))
	pq.Push(makeItem("normal", PriorityNormal))
	pq.Push(makeItem("high", PriorityHigh))

	expected := []string{"critical", "high", "normal", "low"}
	for _, exp := range expected {
		item, err := pq.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if item.JobID != exp {
			t.Errorf("expected %s, got %s", exp, item.JobID)
		}
	}
}

func TestPriorityQueue_ConcurrentPushPop(t *testing.T) {
	pq := NewPriorityQueue()

	for i := 0; i < 100; i++ {
		go func(n int) {
			pq.Push(&Item{
				JobID:      string(rune('a' + n%26)),
				Priority:   PriorityNormal,
				RunAt:      time.Now().Add(time.Minute),
				EnqueuedAt: time.Now(),
			})
		}(i)
	}

	for i := 0; i < 50; i++ {
		pq.Pop()
	}
}
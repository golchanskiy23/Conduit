package delayed

import (
	"conduit/internal/ds/queue"
	minHeap "conduit/internal/ds/queue/heap"
	"container/heap"
	"sync"
	"time"
)

type delayedMinHeap []*minHeap.Item

func (h delayedMinHeap) Less(i, j int) bool { return h[i].RunAt.Before(h[j].RunAt) }
func (h delayedMinHeap) Len() int           { return len(h) }
func (h delayedMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *delayedMinHeap) Push(x any) {
	val, ok := x.(*minHeap.Item)
	if !ok {
		return
	}
	*h = append(*h, val)
}

func (h *delayedMinHeap) Pop() any {
	old := *h
	n := len(old)
	val := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return val
}

type DelayedQueue struct {
	mu   sync.Mutex
	heap delayedMinHeap
}

func NewDelayedQueue() *DelayedQueue {
	dq := &DelayedQueue{}
	heap.Init(&dq.heap)
	return dq
}

func (dq *DelayedQueue) Add(item *minHeap.Item) {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	heap.Push(&dq.heap, item)
}

func (dq *DelayedQueue) Poll(now time.Time) []*minHeap.Item {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	var ready []*minHeap.Item
	for len(dq.heap) > 0 && !dq.heap[0].RunAt.After(now) {
		top, ok := heap.Pop(&dq.heap).(*minHeap.Item)
		if !ok {
			continue
		}
		ready = append(ready, top)
	}
	return ready
}

func (dq *DelayedQueue) Next() (time.Time, error) {
    dq.mu.Lock()
    defer dq.mu.Unlock()

    if len(dq.heap) == 0 {
        return time.Time{}, queue.ErrEmptyQueue
    }
    return dq.heap[0].RunAt, nil
}
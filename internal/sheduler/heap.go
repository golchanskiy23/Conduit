package sheduler

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

type Item struct {
	JobID      string
	Priority   int
	EnqueuedAt time.Time
	idx        int
}

type MinHeap []*Item

func (h MinHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority < h[j].Priority
	}

	return h[i].EnqueuedAt.Before(h[j].EnqueuedAt)
}

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx, h[j].idx = i, j
}

func (h *MinHeap) Push(x any) {
	val, ok := x.(*Item)
	if !ok {
		return
	}

	val.idx = len(*h)
	*h = append(*h, val)
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	val := old[n-1]
	old[n-1] = nil
	val.idx = -1
	*h = old[:n-1]
	return val
}

type PriorityQueue struct {
	mu   sync.Mutex
	heap MinHeap
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	heap.Init(&pq.heap)
	return pq
}

func (h *PriorityQueue) Push(jobID string, priority int) *Item {
	h.mu.Lock()
	defer h.mu.Unlock()

	val := &Item{
		JobID:      jobID,
		Priority:   priority,
		EnqueuedAt: time.Now(),
	}

	heap.Push(&h.heap, val)
	return val
}

var ErrEmptyQueue = errors.New("priority queue is empty")
var ErrConversation = errors.New("error in type conversation")

func (h *PriorityQueue) Pop() (*Item, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.heap) == 0 {
		return nil, ErrEmptyQueue
	}

	val, ok := heap.Pop(&h.heap).(*Item)
	if !ok {
		return nil, ErrConversation
	}

	return val, nil
}

func (h *PriorityQueue) Update(item *Item, newPriority int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	item.Priority = newPriority
	heap.Fix(&h.heap, item.idx)
}

func (h *PriorityQueue) Len() int{
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.heap)
}
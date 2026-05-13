package sheduler

import (
	"sync"
	"time"
	"container/heap"
)

type DelayedItem struct {
	Item *Item
	RunAt time.Time
	idx int // internal definition in delayed queue
}

type DelayedMinHeap []*DelayedItem

func (h DelayedMinHeap) Less(i, j int) bool {
	return h[i].RunAt.Before(h[j].RunAt)	
}

func (h DelayedMinHeap) Len() int {
	return len(h)
}

func (h DelayedMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx, h[j].idx = i, j
}

func (h *DelayedMinHeap) Push(x any) {
	val, ok := x.(*DelayedItem)
	if !ok {
		return
	}

	val.idx = len(*h)
	*h = append(*h, val)
}

func (h *DelayedMinHeap) Pop() any {
	old := *h
	n := len(old)
	val := old[n-1]
	old[n-1] = nil
	val.idx = -1
	*h = old[:n-1]
	return val
}

type DelayedQueue struct {
	mu   sync.Mutex
	heap DelayedMinHeap
}

func (dq *DelayedQueue) Poll(now time.Time) []*DelayedItem{
	dq.mu.Lock()
	defer dq.mu.Unlock()

	var ans []*DelayedItem
	for len(dq.heap) > 0 && !dq.heap[0].RunAt.After(now){
		top := heap.Pop(&dq.heap).(*DelayedItem)
		ans = append(ans, top)
	}

	return ans
}

func (dq *DelayedQueue) Next() (time.Duration, error){
	dq.mu.Lock()
	defer dq.mu.Unlock()
	
	if len(dq.heap) == 0{
		return -1, ErrEmptyQueue
	}
	return time.Until(dq.heap[0].RunAt), nil
}

func (dq *DelayedQueue) Add(item *DelayedItem){
	dq.mu.Lock()
	defer dq.mu.Unlock()
	heap.Push(&dq.heap, item)
}
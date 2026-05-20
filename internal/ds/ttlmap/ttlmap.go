package ttlmap

import (
	priorityqueue "conduit/internal/ds/queue/heap"
	"container/heap"
	"conduit/internal/ds/queue/expiring"
	"sync"
	"time"
)

type jobEntry struct{
	item *priorityqueue.Item
	expiredAt time.Time
}

type TTLMap struct{
	TTLShutdown time.Duration
	mu sync.Mutex
	entryMap map[string]*jobEntry
	indexMap map[string]*expiring.ExpiryItem
	expiryHeap expiring.ExpiryHeap
	wake chan struct{}
	errCh chan struct{}
	once sync.Once
}

func New(t time.Duration) *TTLMap{
	m := &TTLMap{
		TTLShutdown: t,
		mu: sync.Mutex{},
		entryMap: make(map[string]*jobEntry),
		indexMap: make(map[string]*expiring.ExpiryItem),
		expiryHeap: expiring.ExpiryHeap{},
		wake: make(chan struct{}, 1),
		once: sync.Once{},
	}

	heap.Init(&m.expiryHeap)
	go m.cleanup()
	return m
}

func (m *TTLMap) SetIfAbsent(key string, job *priorityqueue.Item) (*priorityqueue.Item, bool){
	m.mu.Lock()
	defer m.mu.Unlock()

	if item, ok := m.entryMap[key]; ok{
		if !time.Now().After(item.expiredAt){
			return item.item, false
		}

		idx := m.indexMap[key]
		heap.Remove(&m.expiryHeap, idx.Index)
		delete(m.indexMap, key)
	}

	expiresAt := time.Now().Add(m.TTLShutdown)
    m.entryMap[key] = &jobEntry{item: job, expiredAt: expiresAt}
    item := &expiring.ExpiryItem{Key: key, ExpiredAt: expiresAt}
    heap.Push(&m.expiryHeap, item)
    m.indexMap[key] = item

    select {
    case m.wake <- struct{}{}:
    default:
    }

    return job, true
}

func (m *TTLMap) Delete(key string){
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.entryMap[key]; !ok{
		return
	}
	delete(m.entryMap, key)
	idx := m.indexMap[key]
	heap.Remove(&m.expiryHeap, idx.Index)
	delete(m.indexMap, key)
}

func (m *TTLMap) Close(){
	m.once.Do(func(){close(m.errCh)})
}

func (m *TTLMap) cleanup(){
	for{
		m.mu.Lock()
		hasItems := len(m.expiryHeap) > 0
		m.mu.Unlock()

		if !hasItems{
			select{
			case <-m.wake:
				continue	
			case <-m.errCh:
				return
			}
		} else{
			m.mu.Lock()
			nextExpired := m.expiryHeap[0].ExpiredAt
			m.mu.Unlock()
			
			timer := time.NewTimer(time.Until(nextExpired))
			select{
			case <-timer.C:
				m.mu.Lock()
				curr := time.Now()
				for len(m.expiryHeap) > 0 && curr.After(m.expiryHeap[0].ExpiredAt){
					item := heap.Pop(&m.expiryHeap).(*expiring.ExpiryItem)
					delete(m.indexMap, item.Key)
					delete(m.entryMap, item.Key)
				}
				m.mu.Unlock()
			case <-m.errCh:
				timer.Stop()
				return
			case <-m.wake:
				timer.Stop()
			}
		}
	}
}
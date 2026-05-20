package ttlmap

import (
	priorityqueue "conduit/internal/ds/queue/heap"
	"container/heap"
	"sync"
	"time"
)

type expiryItem struct{
	expiredAt time.Time
	key string
	index int
}

type ExpiryHeap []*expiryItem

func (h ExpiryHeap) Len() int{return 0}

func (h ExpiryHeap) Less(i,j int) bool{return false}

func (h ExpiryHeap) Swap(i,j int) {}

func (h *ExpiryHeap) Push(val any){}

func (h *ExpiryHeap) Pop() any{return nil}


type jobEntry struct{
	item *priorityqueue.Item
	expiredAt time.Time
}

type TTLMap struct{
	TTLShutdown time.Duration
	mu sync.Mutex
	entryMap map[string]*jobEntry
	indexMap map[string]*expiryItem
	expiryHeap ExpiryHeap
	wake chan struct{}
	errCh chan struct{}
	once sync.Once
}

func New(t time.Duration) *TTLMap{
	m := &TTLMap{
		TTLShutdown: t,
		mu: sync.Mutex{},
		entryMap: make(map[string]*jobEntry),
		indexMap: make(map[string]*expiryItem),
		expiryHeap: ExpiryHeap{},
		wake: make(chan struct{}, 1),
		once: sync.Once{},
	}

	heap.Init(&m.expiryHeap)
	go m.cleanup()
	return m
}

func (m *TTLMap) Set(key string, item *priorityqueue.Item){
	expiredAt := time.Now().Add(m.TTLShutdown)

	m.mu.Lock()
	m.entryMap[key] = &jobEntry{item: item, expiredAt: expiredAt}
	
	if existingItem, existing := m.indexMap[key]; existing{
		existingItem.expiredAt = expiredAt
		heap.Fix(&m.expiryHeap, existingItem.index)
	} else{
		expItem := &expiryItem{expiredAt: expiredAt, key: key}
		m.expiryHeap.Push(expItem)
		m.indexMap[key] = expItem
	}
	m.mu.Unlock()

	select{
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *TTLMap) Delete(key string){
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.entryMap[key]; !ok{
		return
	}
	delete(m.entryMap, key)
	idx := m.indexMap[key]
	heap.Remove(&m.expiryHeap, idx.index)
	delete(m.indexMap, key)
}

func (m *TTLMap) Get(key string)(*priorityqueue.Item, bool){
	m.mu.Lock()
	el, ok := m.entryMap[key] 
	m.mu.Unlock()

	if !ok{
		return nil, false
	}

	if time.Now().After(el.expiredAt){
		m.Delete(key)
		return nil, false
	}

	return el.item, true
}

func (m *TTLMap) Close(){
	m.once.Do(func(){close(m.wake)})
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
			nextExpired := m.expiryHeap[0].expiredAt
			timer := time.NewTimer(time.Until(nextExpired))
			select{
			case <-timer.C:
				m.mu.Lock()
				curr := time.Now()
				for len(m.expiryHeap) > 0 && curr.After(m.expiryHeap[0].expiredAt){
					item := heap.Pop(&m.expiryHeap).(*expiryItem)
					delete(m.indexMap, item.key)
					delete(m.entryMap, item.key)
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
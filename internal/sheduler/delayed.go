package sheduler

import (
	"sync"
	"time"
)

type DelayedItem struct {
	Item *Item
	RunAt time.Time
	idx int // internal definition in delayed queue
}

type DelayedQueue struct {
	mu   sync.Mutex // нужен ли, если блокировка на уровне выше - ?
	jobs []DelayedItem
}

func (dq *DelayedQueue) Poll(time time.Time) []DelayedItem{
	return nil
}

func (dq *DelayedQueue) Next() (time.Duration, error){
	return 0, nil
}
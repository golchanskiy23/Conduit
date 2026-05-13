package sheduler

import (
	"context"
	"sync"
	"time"
)

type Scheduler struct {
	pq             *PriorityQueue
	delayed        DelayedQueue // interface stub
	dependecyGraph DAG          // interface stub
	wp             *WorkerPool

	mu          sync.Mutex
	wakeChannel chan struct{}
}

func NewScheduler(pool *WorkerPool) *Scheduler{
	return &Scheduler{
		pq: &PriorityQueue{},
		delayed: DelayedQueue{},
		dependecyGraph: DAG{},
		wp: &WorkerPool{},
		mu: sync.Mutex{},
		wakeChannel: make(chan struct{}),
	}
}

// возвращать ошибку?
func (s *Scheduler) Run(ctx context.Context) {
	for{
		// забираем из delayed слайс задач -> помещаем в priority
		jobs := s.delayed.Poll(time.Now())
		for _, job := range jobs {
			s.pq.Push(job.Item.JobID, job.Item.Priority)
		}

		// из priority помещаем в worker pool
		for s.pq.Len() > 0 {
			item, err := s.pq.Pop()
			if err != nil {
				continue
			}
			s.wp.Execute(item)
		}

		// смотрим время до следуюЗщей задачи в delayed: если очередь пуста -
		// дефолтное время, запускаем блокируемый select по таймеру
		// (дедлайн - время до следующей задачи):
		// обрабатываем завершение контекста, окончание таймера и добавление задачи с более ранним временем
		next, err := s.delayed.Next()
		if err != nil {
			// default time
			next = time.Minute
		}

		timer := time.NewTimer(next)

		select {
			case <-ctx.Done():
				// context cancelled output
				timer.Stop()
				s.wp.Shutdown()
				return 

			case <- timer.C: // истечение таймера
			case <- s.wakeChannel:
				// добавить задачу с более ранним временем
				timer.Stop()

			// утечка памяти - при каждом вызове создаётся новый таймер
			// case <- time.After(next):
		}
	}
}

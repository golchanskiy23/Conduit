package sheduler

import (
	"context"
	"sync"
	"time"
)

type schedulerOptions struct{
	execute func(context.Context, *Item) error
	cfg WorkerPoolConfig
	onError func(string, error)
}

type Scheduler struct {
	pq             *PriorityQueue
	delayed        *DelayedQueue
	dependecyGraph *DAG
	wp             *WorkerPool
	registry map[string]*Item

	mu          sync.Mutex
	wakeChannel chan struct{}
	done        chan error
}

func NewScheduler(options ...Option) *Scheduler{
	s := &Scheduler{
		pq: NewPriorityQueue(),
		delayed: NewDelayedQueue(),
		dependecyGraph: NewDAG(),

		registry: make(map[string]*Item),
		mu: sync.Mutex{},
		wakeChannel: make(chan struct{}, 1),
	}

	so := &schedulerOptions{}
	for _, opt := range options{
		opt(so)
	}

	s.wp = newWorkerPool(cfg.Pool, execute, s.OnDone, s.onError)

	return s
}

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

func (s *Scheduler) enqueue(job *Item, t time.Time){
	if t.After(time.Now()){
		delayedItem := &DelayedItem{
			Item: job,
			RunAt: t,
		}
		s.delayed.Add(delayedItem)

		select{
		// канал блокируется, сигнал Run() пересчитать Next()
		case s.wakeChannel <- struct{}{}:
		default:
		}
	} else{
		s.pq.Push(job.JobID, job.Priority)
	}
}

func (s *Scheduler) Submit(job *Item, items []string, RunAt time.Time) error{
	s.mu.Lock()
	defer s.mu.Unlock()

	s.registry[job.JobID] = job

	if err := s.dependecyGraph.Add(job.JobID, items); err != nil{
		return err
	}

	if len(items) == 0{
		s.enqueue(job, RunAt)
	}

	return nil
}

func (s *Scheduler) OnDone(id string){
	s.mu.Lock()
    defer s.mu.Unlock()

    unlocked := s.dependecyGraph.OnComplete(id)
    for _, jobID := range unlocked {
        job := s.registry[jobID]
		// как-то перелать RunAt
        s.enqueue(job, job.EnqueuedAt)
    }

    delete(s.registry, id) 
}
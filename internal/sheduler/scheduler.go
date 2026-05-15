package sheduler

import (
	"context"
	"sync"
	"time"
	cfg "conduit/config"
)

type Scheduler struct {
	pq             *PriorityQueue
	delayed        *DelayedQueue
	dependecyGraph *DAG
	Pool             *workerPool
	registry map[string]*Item
	cfg *cfg.Config

	mu          sync.Mutex
	wakeChannel chan struct{}
	done        chan error
}

func (s *Scheduler) execute(ctx context.Context, item *Item) error{
	return nil
}

func (s *Scheduler) onError(string, error){

}

// как передавать cfg в NewScheduler?
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

	s.Pool = newWorkerPool(cfg.Pool, s.execute, s.OnDone, s.onError)

	return s
}

func (s *Scheduler) Wait() error {
    return <- s.done // chan error, закрывается в Run перед return
}

func (s *Scheduler) Run(ctx context.Context) {
	for{
		// забираем из delayed слайс задач -> помещаем в priority
		jobs := s.delayed.Poll(time.Now())
		for _, job := range jobs {
			s.pq.Push(job.JobID, job.Priority)
		}

		// из priority помещаем в worker pool
		for s.pq.Len() > 0 {
			item, err := s.pq.Pop()
			if err != nil {
				continue
			}
			s.Pool.Execute(item)
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
				// нужно ли передавать если context завершился
				s.Pool.Shutdown(ctx)
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

func (s *Scheduler) enqueue(job *Item) {
    if job.RunAt.After(time.Now()) {
        s.delayed.Add(job)
        select {
        case s.wakeChannel <- struct{}{}:
        default:
        }
    } else {
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
		s.enqueue(job)
	}

	return nil
}

func (s *Scheduler) OnDone(id string){
	s.mu.Lock()
    defer s.mu.Unlock()

    unlocked := s.dependecyGraph.OnComplete(id)
    for _, jobID := range unlocked {
        job := s.registry[jobID]
        s.enqueue(job)
    }

    delete(s.registry, id) 
}
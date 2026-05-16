package scheduler

import (
	"context"
    "conduit/config"
	"log"
	"sync"
	"time"
)

type Scheduler struct {
	pq      *PriorityQueue
	delayed *DelayedQueue
	dag     *DAG
	pool    WorkerPool

	registry    map[string]*Item
	mu          sync.Mutex
	wakeChannel chan struct{}
	done        chan error
}

func NewScheduler(cfg *config.Config, options ...Option) *Scheduler {
	so := &schedulerOptions{
		onError: func(id string, err error) {
			log.Printf("job %s error: %v", id, err)
		},
	}
	for _, opt := range options {
		opt(so)
	}

	s := &Scheduler{
		pq:          NewPriorityQueue(),
		delayed:     NewDelayedQueue(),
		dag:         NewDAG(),
		registry:    make(map[string]*Item),
		wakeChannel: make(chan struct{}, 1),
		done:        make(chan error),
	}

	if so.pool != nil {
		s.pool = so.pool
	} else {
		s.pool = newWorkerPool(so.cfg,
        
        )
	}

	return s
}

func (s *Scheduler) Start(ctx context.Context, n int) {
	if wp, ok := s.pool.(*workerPool); ok {
		wp.Start(ctx, n)
	}
}

func (s *Scheduler) Wait() error {
	return <-s.done
}

func (s *Scheduler) Run(ctx context.Context) {
	defer close(s.done)

	for {
		for _, job := range s.delayed.Poll(time.Now()) {
			s.pq.Push(job.JobID, job.Priority)
		}

		s.mu.Lock()
		var batch []*Item
		for s.pq.Len() > 0 {
			item, err := s.pq.Pop()
			if err != nil {
				break
			}
			batch = append(batch, item)
		}
		s.mu.Unlock()

		var overflow []*Item
		for _, item := range batch {
			if !s.pool.TryExecute(item) {
				overflow = append(overflow, item)
			}
		}

		if len(overflow) > 0 {
			s.mu.Lock()
			for _, item := range overflow {
				s.pq.Push(item.JobID, item.Priority)
			}
			s.mu.Unlock()
		}

		nextAt, err := s.delayed.Next()
        if err != nil {
            nextAt = time.Now().Add(time.Minute)
        }
        timer := time.NewTimer(time.Until(nextAt))
		
        select {
		case <-ctx.Done():
			timer.Stop()
			s.pool.Shutdown(ctx)
			return
		case <-timer.C:
		case <-s.wakeChannel:
			timer.Stop()
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

func (s *Scheduler) Submit(job *Item, deps []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.registry[job.JobID] = job

	if err := s.dag.Add(job.JobID, deps); err != nil {
		delete(s.registry, job.JobID)
		return err
	}

	if len(deps) == 0 {
		s.enqueue(job)
	}

	return nil
}

func (s *Scheduler) onDone(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	unlocked := s.dag.OnComplete(id)
	for _, jobID := range unlocked {
		job := s.registry[jobID]
		s.enqueue(job)
	}
	delete(s.registry, id)
}
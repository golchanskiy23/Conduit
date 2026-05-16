package scheduler

import (
	"context"
    "conduit/config"
	"log"
	"sync"
	"time"
	"fmt"
)

type Scheduler struct {
	pq      *PriorityQueue
	delayed *DelayedQueue
	dag     *DAG
	pool    WorkerPool

	registry    map[string]*Item
	mu          sync.Mutex
	wakeChannel chan struct{}
	done        chan struct{}
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
		done:        make(chan struct{}),
	}

	if so.pool != nil {
		s.pool = so.pool
	} else {
		if so.execute == nil {
			panic("scheduler: WithTaskExecutor is required")
		}
		s.pool = newWorkerPool(cfg.PoolCfg,
			WithExecutor(so.execute),
			WithOnDone(s.OnDone),
			WithOnError(so.onError),
		)
	}	

	return  s
}

func (s *Scheduler) Start(ctx context.Context, n int) {
	if wp, ok := s.pool.(*workerPool); ok {
		wp.Start(ctx, n)
	}
}

func (s *Scheduler) Wait() {
	<-s.done
}

func (s *Scheduler) Run(ctx context.Context) {
	defer close(s.done)

	for {
		for _, job := range s.delayed.Poll(time.Now()) {
			s.pq.Push(job)
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
				s.pq.Push(item)
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
			if err := s.pool.Shutdown(ctx); err != nil {
				log.Printf("pool shutdown: %v", err)
			}
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
    } else {
        s.pq.Push(job)
    }
    select {
    case s.wakeChannel <- struct{}{}:
    default:
    }
}

func (s *Scheduler) Submit(job *Item, deps []string) error {
	s.mu.Lock()

	if _, ok := s.registry[job.JobID]; ok {
        s.mu.Unlock()
        return fmt.Errorf("%w: %s", ErrAlreadyExists, job.JobID)
    }

	s.registry[job.JobID] = job

	if err := s.dag.Add(job.JobID, deps); err != nil {
		delete(s.registry, job.JobID)
        s.mu.Unlock()
		return err
	}

	shouldEnqueue := len(deps) == 0

    s.mu.Unlock()

    if shouldEnqueue {
        s.enqueue(job)
    }

    return nil
}

func (s *Scheduler) OnDone(id string) {
	s.mu.Lock()

	unlocked := s.dag.OnComplete(id)

    var jobs []*Item
    for _, id := range unlocked{
        job, ok := s.registry[id]
        if ok {
            jobs = append(jobs, job)
        }
    }

    delete(s.registry, id)
    s.mu.Unlock()

	for _, job := range jobs {
		s.enqueue(job)
	}
}

func (s *Scheduler) Wake() {
	select {
	case s.wakeChannel <- struct{}{}:
	default:
	}
}
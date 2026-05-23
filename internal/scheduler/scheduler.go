package scheduler

import (
	"conduit/internal/config"
	"conduit/internal/ds/graph"
	"conduit/internal/ds/queue/delayed"
	"conduit/internal/ds/queue/heap"
	"conduit/internal/pool"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Scheduler struct {
	pq      *heap.PriorityQueue
	delayed *delayed.DelayedQueue
	dag     *graph.DAG
	pools    []pool.WorkerPooler
	onError func(string, error)

	registry    map[string]*heap.Item
	mu          sync.Mutex
	wakeChannel chan struct{}
	done        chan struct{}
}

func NewScheduler(cfg *config.Config, options ...Option) *Scheduler {
	s := &Scheduler{
		pq:          heap.NewPriorityQueue(),
		delayed:     delayed.NewDelayedQueue(),
		dag:         graph.NewDAG(),
		registry:    make(map[string]*heap.Item),
		wakeChannel: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}

	for _, opt := range options {
		opt(s)
	}

	return  s
}

func (s *Scheduler) Start(ctx context.Context) {
	for _, p := range s.pools{
		p.Start(ctx)
	}
}

func (s *Scheduler) Wait() {
	<-s.done
}

func (s *Scheduler) dispatch(job *heap.Item) bool {
	for _, p := range s.pools {
		if p.Worker().Handles(job.JobType) {
			return p.TryExecute(job)
		}
	}
	s.onError(job.JobID, fmt.Errorf("%w: %s", ErrNoSuchWorker, job.JobType))
	return false
}

func (s *Scheduler) Run(ctx context.Context) {
	defer close(s.done)

	for {
		for _, job := range s.delayed.Poll(time.Now()) {
			s.pq.Push(job)
		}

		s.mu.Lock()
		var batch []*heap.Item
		for s.pq.Len() > 0 {
			item, err := s.pq.Pop()
			if err != nil {
				break
			}
			batch = append(batch, item)
		}
		s.mu.Unlock()

		var overflow []*heap.Item
		for _, item := range batch {
			if !s.dispatch(item) {
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
			for _, p := range s.pools{
				if err := p.Shutdown(ctx); err != nil {
					log.Printf("pool shutdown: %v", err)
				}
			}
			return
		case <-timer.C:
		case <-s.wakeChannel:
			timer.Stop()
		}
	}
}

func (s *Scheduler) enqueue(job *heap.Item) {
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

func (s *Scheduler) Submit(job *heap.Item, deps []string) error {
	s.mu.Lock()

	if _, ok := s.registry[job.JobID]; ok {
        s.mu.Unlock()
        return fmt.Errorf("%w: %s", graph.ErrAlreadyExists, job.JobID)
    }

	hasWorker := false
	for _, p := range s.pools {
    	if p.Worker().Handles(job.JobType) {
        	hasWorker = true
        	break
    	}
	}

	if !hasWorker {
    	s.mu.Unlock()
    	return fmt.Errorf("%w: %s", ErrNoSuchWorker, job.JobType)
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

    var jobs []*heap.Item
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

func (s *Scheduler) Register(pools ...pool.WorkerPooler){
	for _, p := range pools{
		p.SetOnDone(s.OnDone)
		s.pools = append(s.pools, p)
	}
}
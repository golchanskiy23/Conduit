package sheduler

import (
	"context"
	"sync"
	"time"
)

type WorkerPool struct{
	Jobs chan *Item
	Wg sync.WaitGroup

	// это какая-то функция выполнения воркера
	toExecution func(context.Context, *Item) error
}

func NewWorkerPool(size int) *WorkerPool{
	return &WorkerPool{
		Jobs: make(chan *Item, size),
	}
}

func (pool *WorkerPool) Worker(ctx context.Context){
	defer pool.Wg.Done()
	for job := range pool.Jobs{
		// потенциальное время работы одной job
		parent, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel() // освобождение ресурсов таймера
		err := pool.toExecution(parent, job) // может какой-то ответ
		if err != nil{
			// выводим ошибку или прокидываем выше
		}
	}
}

func (pool *WorkerPool) Execute(job *Item){
	pool.Jobs <- job
}

// возвращать ошибку - ?
func (pool *WorkerPool) Shutdown(){
	close(pool.Jobs)
	pool.Wg.Wait()
}
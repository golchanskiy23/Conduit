package main

import (
	"conduit/internal/sheduler"
	"context"
	"runtime"
)


func main(){
	// size брать из конфигурации
	// пересчитать позже размеры
	pool := sheduler.NewWorkerPool(100)
	ctx := context.Background()

	for i := 0; i < runtime.GOMAXPROCS(0); i++{
		pool.Wg.Add(1)
		go pool.Worker(ctx)
	}

	scheduler := sheduler.NewScheduler(pool)
	// при необходимости обернуть в ошибку и вернуть результат
	go scheduler.Run(ctx)
}
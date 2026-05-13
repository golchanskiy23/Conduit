package main

import (
	"conduit/internal/sheduler"
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)


func main(){
	// size брать из конфигурации
	// пересчитать позже размеры
	pool := sheduler.NewWorkerPool(100)
	// нужно отменить контекст при получении сигнала от ОС
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < runtime.GOMAXPROCS(0); i++{
		pool.Wg.Add(1)
		go pool.Worker(ctx)
	}

	scheduler := sheduler.NewScheduler(pool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func(){
		<-quit
		cancel()
	}()

	// при необходимости обернуть в ошибку и вернуть результат
	scheduler.Run(ctx)
}
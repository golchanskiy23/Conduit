package main

import (
	"conduit/internal/sheduler"
	"context"
	"http"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)


func main(){
	// size брать из конфигурации
	// пересчитать позже размеры
	scheduler := sheduler.NewScheduler()

	pool := sheduler.NewWorkerPool(100, scheduler.OnDone)
	scheduler.SetPool(pool)
	// нужно отменить контекст при получении сигнала от ОС
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < runtime.GOMAXPROCS(0); i++{
		pool.Wg.Add(1)
		go pool.Worker(ctx)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func(){
		<-quit
		cancel()
	}()

	// создаём хэндлер от шедулера
	// регистрируем ручки в хэндлер
	handler := http.NewHTTPHandler(scheduler)
	http.HandleFunc("/postJob", handler.EnqueueJob)

	// запускаем сервер
	go http.ListenAndServe(":8080", nil)

	// при необходимости обернуть в ошибку и вернуть результат
	scheduler.Run(ctx)

	// http-server graceful shutdown
}
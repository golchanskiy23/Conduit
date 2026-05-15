package main

import (
	"conduit/internal/sheduler"
	"context"
	"conduit/handler"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"errors"
	"log"
	cfg "conduit/config"
)


func main(){
	config := &cfg.Config{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// добавить параметры по функциональным опциям(включая запускаемую функцию)
	scheduler := sheduler.NewScheduler(cfg.Scheduler, executeJob)
	scheduler.Pool.Start(ctx, cfg.WorkerCount)

	mux := http.NewServeMux()
	handler := handler.NewHTTPHandler(scheduler)
	mux.HandleFunc("/postJob", handler.EnqueueJob)

	srv := &http.Server{
    	Addr:         ":" + cfg.Port,
    	Handler:      mux,
    	ReadTimeout:  5 * time.Second,
    	WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)

	go func() {
    	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
        	errCh <- err
    	}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
    	log.Println("shutdown signal received")
	case err := <-errCh:
    	log.Printf("http server error: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
    	log.Printf("http shutdown: %v", err)
	}

	cancel()

	if err := scheduler.Wait(); err != nil {
    	log.Printf("scheduler shutdown: %v", err)
	}

	signal.Stop(quit)
	close(quit)
}
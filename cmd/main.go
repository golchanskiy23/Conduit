package main

import (
	"conduit/config"
	"conduit/handler"
	"conduit/internal/scheduler"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func executeJob(ctx context.Context, item *scheduler.Item) error{
	return nil
}

func main(){
	cfg, err := config.NewConfig()
	if err != nil{
		log.Fatalf("error during get configuration: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := scheduler.NewScheduler(
		cfg,
		scheduler.WithTaskExecutor(executeJob),
		scheduler.WithPoolConfig(cfg.PoolCfg),
		scheduler.WithTaskOnError(func(id string, err error) {
			log.Printf("job %s failed: %v", id, err)
		}),
	)

	s.Start(ctx, cfg.WorkersNum)
	go s.Run(ctx)

	mux := http.NewServeMux()
	h := handler.NewHTTPHandler(s)
	mux.HandleFunc("/jobs", h.EnqueueJob)

	srv := &http.Server{
		Addr:         ":8080",
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

	/*if err := s.Wait(); err != nil {
		log.Printf("scheduler shutdown: %v", err)
	}*/
	s.Wait()

	signal.Stop(quit)
	close(quit)
}
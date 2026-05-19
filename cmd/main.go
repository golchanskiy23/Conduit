package main

import (
	"conduit/internal/config"
	"conduit/internal/ds/queue/heap"
	"conduit/internal/handler"
	"conduit/internal/scheduler"
	"conduit/internal/server"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func executeJob(ctx context.Context, item *heap.Item) error{
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

	srv := server.NewServer(mux,
		cfg,
		server.WithAddress(cfg.Server.Addr),
		server.WithReadTimeout(cfg.Server.ReadTimeout),
		server.WithWriteTimeout(cfg.Server.WriteTimeout),
		server.WithIdleTimeout(cfg.Server.IdleTimeout),
		server.WithShutdownTimeout(cfg.Server.ShutdownTimeout),
	)

	srv.Start()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {

	case <-quit:
		log.Println("shutdown signal received")

	case err := <-srv.Errors():

		if !errors.Is(err, context.Canceled) {
			log.Printf("server error: %v", err)
		}
	}

	signal.Stop(quit)

	cancel()

	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatalf("server shutdown error: %v", err)
	}

	s.Wait()
	log.Println("application succesfully stopped")
}
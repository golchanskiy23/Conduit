package main

import (
	"conduit/internal/config"
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"conduit/internal/app"
)

func main(){
	cfg, err := config.NewConfig()
	if err != nil{
		log.Fatalf("error during get configuration: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app := app.New(cfg)
	app.Start(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {

	case <-quit:
		log.Println("shutdown signal received")

	case err := <-app.Errors():

		if !errors.Is(err, context.Canceled) {
			log.Printf("server error: %v", err)
		}
	}

	signal.Stop(quit)

	cancel()

	if err := app.Shutdown(context.Background()); err != nil {
		log.Fatalf("server shutdown error: %v", err)
	}

	app.Wait()
	log.Println("application succesfully stopped")
}
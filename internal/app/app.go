package app

import (
	"conduit/internal/config"
	"conduit/internal/scheduler"
	"conduit/internal/server"
	"context"
)

type App struct{
	srv *server.Server
	scheduler *scheduler.Scheduler
}

func New(cfg *config.Config) *App{
	return nil
}

func (app *App) Start(ctx context.Context){
	app.scheduler.Start(ctx)
	go app.scheduler.Run(ctx)
	app.srv.Start()
}

func (app *App) Shutdown(ctx context.Context) error{
	return app.srv.Shutdown(ctx)
}

func (app *App) Errors() <-chan error{
	return app.srv.Errors()
}

func (app *App) Wait(){
	app.scheduler.Wait()
}
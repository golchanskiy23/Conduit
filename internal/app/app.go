package app

import (
	"conduit/internal/config"
	"context"
)

type App struct{

}

func New(cfg *config.Config) *App{
	return nil
}

func (app *App) Start(ctx context.Context){

}

func (app *App) Shutdown(ctx context.Context) error{
	return nil
}

func (app *App) Errors() <-chan error{
	return nil
}

func (app *App) Wait(){
	
}
package app

import (
	"conduit/internal/config"
	"conduit/internal/scheduler"
	"conduit/internal/server"
	"conduit/internal/handler"
	"context"
	"conduit/internal/pool"
	"log"
	"net/http"
)

type App struct{
	srv *server.Server
	scheduler *scheduler.Scheduler
}

var workers = map[string]pool.Worker{
	//"payment": PaymentWorker{},
	//"email":   EmailWorker{},
	//"report":  ReportWorker{},
	//"default": DefaultWorker{},
}

func createPools(cfg *config.Config, onError func(string, error)) []pool.WorkerPooler {
	pools := make([]pool.WorkerPooler, 0, len(cfg.PoolCfg))
	for _, pc := range cfg.PoolCfg {
		w, ok := workers[pc.Worker]
		if !ok {
			log.Printf("unknown worker type: %s, skipping", pc.Worker)
			continue
		}
		p := pool.NewWorkerPool(
			config.WorkerPoolConfig{
				BufferSize: pc.BufferSize,
				JobTimeout: pc.JobTimeout,
				WorkersNum: pc.WorkersNum,
			},
			w,
			pool.WithOnError(onError),
		)
		pools = append(pools, p)
	}
	return pools
}
func New(cfg *config.Config) *App{
	onError := func(id string, err error) {
		log.Printf("job %s failed: %v", id, err)
	}

	s := scheduler.NewScheduler(cfg,
		scheduler.WithTaskOnError(onError),
	)

	pools := createPools(cfg, onError)
	s.Register(pools...)

	mux := http.NewServeMux()
	h := handler.NewHTTPHandler(s, cfg.TTLMap.ExpiredTimeShutdown)
	mux.HandleFunc("/jobs", h.EnqueueJob)

	srv := server.NewServer(mux, cfg)

	return &App{srv: srv, scheduler: s}
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
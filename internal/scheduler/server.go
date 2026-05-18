package scheduler

import (
	"conduit/config"
	"errors"
	"net/http"
	"sync"
	"time"
	"context"
	"fmt"
)

type ServerOption func(srv *Server)

func WithAddress(addr string) ServerOption{
	return func(srv *Server){
		srv.internalServer.Addr = addr
	}
}

func WithReadTimeout(readTimeout time.Duration) ServerOption{
	return func(srv *Server){
		srv.internalServer.ReadTimeout = readTimeout
	}
}

func WithWriteTimeout(writeTimeout time.Duration) ServerOption{
	return func(srv *Server){
		srv.internalServer.WriteTimeout = writeTimeout
	}
}

func WithIdleTimeout(idleTimeout time.Duration) ServerOption{
	return func(srv *Server){
		srv.internalServer.IdleTimeout = idleTimeout
	}
}

func WithShutdownTimeout(shutdownTimeout time.Duration) ServerOption{
	return func(srv *Server){
		srv.shutdownTimeout = shutdownTimeout
	}
}

type Server struct{
	internalServer *http.Server
	errCh chan error
	shutdownTimeout time.Duration
	wg sync.WaitGroup
}

func NewServer(mux http.Handler, cfg *config.Config, opts ...ServerOption) *Server{
	srv := &Server{
		internalServer: &http.Server{
			Addr: cfg.Server.Addr,
			Handler: mux,
			ReadTimeout: cfg.Server.ReadTimeout,
			WriteTimeout: cfg.Server.WriteTimeout,
		},
		shutdownTimeout: cfg.Server.ShutdownTimeout,
		errCh: make(chan error,1),
	}

	for _, opt := range opts{
		opt(srv)
	}

	return srv
}

func (s *Server) Start(){
	s.wg.Add(1)

	go func(){
		defer s.wg.Done()
		if err := s.internalServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed){
			select {
			case s.errCh <- err:
			default:
			}
		}
	}()
}

func (s *Server) Errors() <-chan error {
	return s.errCh
}

func (s *Server) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
	defer cancel()

	if err := s.internalServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("http shutdown failed: %w", err)
	}

	s.wg.Wait()

	return nil
}
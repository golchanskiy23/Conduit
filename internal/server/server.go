package server

import (
	"conduit/internal/config"
	"errors"
	"net/http"
	"sync"
	"time"
	"context"
	"fmt"
)

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
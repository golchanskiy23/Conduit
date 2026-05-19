package server

import "time"

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
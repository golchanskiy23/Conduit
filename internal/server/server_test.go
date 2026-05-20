package server

import (
	"conduit/internal/config"
	"context"
	"net/http"
	"testing"
	"time"
)

func testServerCfg(addr string) *config.Config {
	return &config.Config{
		Server: config.HTTPServer{
			Addr:            addr,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			IdleTimeout:     time.Second,
			ShutdownTimeout: time.Second,
		},
	}
}

func TestServer_StartAndShutdown(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := NewServer(mux, testServerCfg(":19080"))
	srv.Start()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://localhost:19080/health")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown error: %v", err)
	}
}

func TestServer_ErrorsChannelOnBadPort(t *testing.T) {
	srv := NewServer(http.NewServeMux(), testServerCfg(":99999"))
	srv.Start()

	select {
	case err := <-srv.Errors():
		if err == nil {
			t.Error("expected error on bad port")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error")
	}
}

func TestServer_ShutdownIdempotent(t *testing.T) {
	srv := NewServer(http.NewServeMux(), testServerCfg(":19081"))
	srv.Start()
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("first shutdown error: %v", err)
	}
	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("second shutdown error: %v", err)
	}
}

func TestServer_ShutdownTimeout(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second)
		w.WriteHeader(http.StatusOK)
	})

	cfg := &config.Config{
		Server: config.HTTPServer{
			Addr:            ":19082",
			ReadTimeout:     2 * time.Second,
			WriteTimeout:    2 * time.Second,
			ShutdownTimeout: 50 * time.Millisecond,
		},
	}

	srv := NewServer(mux, cfg)
	srv.Start()
	time.Sleep(50 * time.Millisecond)

	// Запускаем долгий запрос в фоне
	go http.Get("http://localhost:19082/slow")
	time.Sleep(20 * time.Millisecond)

	err := srv.Shutdown(context.Background())
	if err == nil {
		t.Error("expected timeout error on shutdown")
	}
}

func TestServer_WithOptions(t *testing.T) {
	srv := NewServer(
		http.NewServeMux(),
		testServerCfg(":19083"),
		WithAddress(":19083"),
		WithReadTimeout(2*time.Second),
		WithWriteTimeout(2*time.Second),
		WithIdleTimeout(2*time.Second),
		WithShutdownTimeout(2*time.Second),
	)
	srv.Start()
	time.Sleep(50 * time.Millisecond)

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown error: %v", err)
	}
}
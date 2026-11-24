package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"taskflow/internal/api"
	"taskflow/internal/ops"
	"taskflow/internal/redisresp"
	"taskflow/internal/tasks"
)

func main() {
	ops.Logging()
	if err := run(); err != nil {
		slog.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	data := ops.Env("DATA_DIR", "./data")
	for _, sub := range []string{"uploads", "results"} {
		if err := os.MkdirAll(filepath.Join(data, sub), 0755); err != nil {
			return err
		}
	}
	if err := ops.Profiling(); err != nil {
		return err
	}
	repo := tasks.New(redisresp.FromEnv())
	server := &http.Server{Addr: ops.Env("HTTP_ADDR", ":8080"), Handler: (&api.Server{Store: repo, DataDir: data}).Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 40 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopped := make(chan error, 1)
	go func() {
		slog.Info("API listening", "address", server.Addr)
		stopped <- server.ListenAndServe()
	}()
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		drain, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := server.Shutdown(drain); err != nil {
			return err
		}
		if err := <-stopped; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

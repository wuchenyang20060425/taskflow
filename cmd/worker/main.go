package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"taskflow/internal/ops"
	"taskflow/internal/redisresp"
	"taskflow/internal/tasks"
	"taskflow/internal/worker"
)

func main() {
	ops.Logging()
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	hostname, _ := os.Hostname()
	w := worker.Worker{
		Store: tasks.New(redisresp.FromEnv()), DataDir: ops.Env("DATA_DIR", "./data"),
		Name: hostname + "-" + ops.Env("WORKER_SUFFIX", "worker"), PopplerBinary: ops.Env("PDFTOTEXT_BIN", "pdftotext"),
	}
	for _, setting := range []struct {
		key, fallback string
		dst           *time.Duration
	}{
		{"WORKER_LEASE", "40s", &w.Lease}, {"WORKER_MIN_IDLE", "45s", &w.MinIdle}, {"WORKER_TIMEOUT", "25s", &w.Timeout},
	} {
		d, err := time.ParseDuration(ops.Env(setting.key, setting.fallback))
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid %s", setting.key)
		}
		*setting.dst = d
	}
	attempts, err := strconv.Atoi(ops.Env("WORKER_MAX_ATTEMPTS", "3"))
	if err != nil || attempts < 1 {
		return fmt.Errorf("invalid WORKER_MAX_ATTEMPTS")
	}
	w.MaxAttempts = attempts
	if err := ops.Profiling(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return w.Run(ctx)
}

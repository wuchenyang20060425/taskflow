package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"taskflow/internal/ops"
	"taskflow/internal/redisresp"
	"taskflow/internal/tasks"
)

func main() {
	data := flag.String("data", ops.Env("DATA_DIR", "./data"), "shared data directory")
	age := flag.Duration("min-age", 24*time.Hour, "minimum candidate age")
	apply := flag.Bool("apply", false, "remove candidates with API and all workers stopped")
	flag.Parse()
	if *age < time.Hour {
		log.Fatal("min-age must be at least 1h")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	repo := tasks.New(redisresp.FromEnv())
	if err := repo.Ready(ctx); err != nil {
		log.Fatal(err)
	}
	// Refuse directory cleanup if this Redis has no task queue.
	if _, err := repo.Redis.Do(ctx, "XINFO", "GROUPS", tasks.Stream); err != nil {
		log.Fatal(err)
	}
	cutoff := time.Now().Add(-*age)
	for _, kind := range []string{"uploads", "results"} {
		if err := reconcile(ctx, repo, filepath.Join(*data, kind), kind, cutoff, *apply); err != nil {
			log.Fatal(err)
		}
	}
}

func reconcile(ctx context.Context, repo *tasks.Repository, dir, kind string, cutoff time.Time, apply bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Name()
		id, ok := candidate(kind, name)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.ModTime().After(cutoff) {
			continue
		}

		task, err := repo.Get(ctx, id)
		if err != nil && !errors.Is(err, tasks.ErrNotFound) {
			return err
		}
		if err == nil && (kind == "uploads" || task.ResultFile == name) {
			continue
		}

		path := filepath.Join(dir, name)
		if !apply {
			fmt.Printf("candidate %s\n", path)
			continue
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		fmt.Printf("removed %s\n", path)
	}
	return nil
}

func candidate(kind, filename string) (string, bool) {
	if kind == "uploads" {
		id, ok := strings.CutSuffix(filename, ".pdf")
		return id, ok && tasks.ValidID(id)
	}
	id, _, _ := strings.Cut(filename, "-")
	if tasks.ValidResult(id, filename) {
		return id, true
	}
	// A partial file is named <task>-<token>-<CreateTemp suffix>.partial.
	if base, ok := strings.CutSuffix(filename, ".partial"); ok {
		if i := strings.LastIndexByte(base, '-'); i > 0 {
			return id, tasks.ValidResult(id, base[:i]+".txt")
		}
	}
	return "", false
}

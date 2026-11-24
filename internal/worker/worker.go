package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"taskflow/internal/convert"
	"taskflow/internal/tasks"
)

type Executor func(ctx context.Context, input, resultsDir, taskID, token string) (string, error)

type Worker struct {
	Store         *tasks.Repository
	DataDir       string
	Name          string
	PopplerBinary string
	Lease         time.Duration
	MinIdle       time.Duration
	Timeout       time.Duration
	MaxAttempts   int
	Execute       Executor
}

func (w Worker) Run(ctx context.Context) error {
	if w.Lease == 0 {
		w.Lease = 40 * time.Second
	}
	if w.MinIdle == 0 {
		w.MinIdle = w.Lease
	}
	if w.Timeout == 0 {
		w.Timeout = 25 * time.Second
	}
	if w.MaxAttempts == 0 {
		w.MaxAttempts = 3
	}
	if w.Lease-w.Timeout < time.Second || w.Timeout <= 0 || w.MinIdle < time.Millisecond || w.MaxAttempts < 1 {
		return fmt.Errorf("invalid worker settings: lease must exceed timeout by at least a second")
	}
	if w.PopplerBinary == "" {
		w.PopplerBinary = "pdftotext"
	}
	if w.Execute == nil {
		w.Execute = func(ctx context.Context, input, dir, id, token string) (string, error) {
			return convert.PDFToText(ctx, w.PopplerBinary, input, dir, id, token)
		}
	}
	var instance [12]byte
	if _, err := rand.Read(instance[:]); err != nil {
		return err
	}
	w.Name += "-" + hex.EncodeToString(instance[:])
	log := slog.With("consumer", w.Name)
	log.Info("worker started")
	defer log.Info("worker stopped")
	groupReady := false
	cursor := "0-0"
	for ctx.Err() == nil {
		if !groupReady {
			if err := w.Store.EnsureGroup(ctx); err != nil {
				if ctx.Err() == nil {
					log.Warn("ensure group", "error", err)
				}
				wait(ctx)
				continue
			}
			groupReady = true
			cursor = "0-0"
		}
		next, recovered, err := w.Store.Reclaim(ctx, w.Name, w.MinIdle, cursor)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("reclaim pending", "error", err)
			}
			groupReady = !strings.Contains(err.Error(), "NOGROUP")
			wait(ctx)
			continue
		}
		cursor = next
		for _, m := range recovered {
			w.process(ctx, log, m)
		}
		if ctx.Err() != nil {
			break
		}
		// After recovering work, check for new messages without waiting. This
		// advances the recovery cursor while still sharing time with new work.
		msgs, err := w.Store.Read(ctx, w.Name, len(recovered) == 0 && cursor == "0-0")
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("read queue", "error", err)
			}
			groupReady = !strings.Contains(err.Error(), "NOGROUP")
			wait(ctx)
			continue
		}
		for _, m := range msgs {
			w.process(ctx, log, m)
		}
	}
	return nil
}

func wait(ctx context.Context) {
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (w Worker) process(ctx context.Context, log *slog.Logger, m tasks.Message) {
	log = log.With("task_id", m.TaskID, "stream_id", m.StreamID)
	var attempt [12]byte
	if _, err := rand.Read(attempt[:]); err != nil {
		log.Error("generate execution token", "error", err)
		return
	}
	token := hex.EncodeToString(attempt[:])
	if !tasks.ValidID(m.TaskID) {
		w.ack(ctx, log, m)
		return
	}
	state, err := w.Store.Claim(ctx, m, token, w.Lease, w.MaxAttempts)
	if err != nil {
		log.Warn("claim", "error", err)
		return
	}
	switch state {
	case tasks.Done, tasks.Missing:
		w.ack(ctx, log, m)
		return
	case tasks.Busy:
		// XAUTOCLAIM may have returned the current execution's own message.
		// Keep it pending so another worker can recover it after expiry.
		return
	case tasks.Exhausted:
		log.Info("task finished", "state", "failed", "reason", "attempt budget exhausted")
		return
	case tasks.Acquired:
	default:
		log.Error("unexpected claim state", "state", state)
		return
	}

	start := time.Now()
	jobCtx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()
	t, err := w.Store.Get(jobCtx, m.TaskID)
	if err != nil {
		log.Warn("load claimed task", "error", err)
		return
	}
	log = log.With("attempt", t.Attempts)
	log.Info("task started")
	resultDir := filepath.Join(w.DataDir, "results")
	var output string
	if t.InputFile != m.TaskID+".pdf" {
		err = fmt.Errorf("invalid stored PDF reference")
	} else {
		output, err = w.Execute(jobCtx, filepath.Join(w.DataDir, "uploads", t.InputFile), resultDir, m.TaskID, token)
	}
	if err == nil && !tasks.ValidResult(m.TaskID, output) {
		err = fmt.Errorf("executor produced an invalid result reference")
	}
	elapsed := time.Since(start).Milliseconds()
	if ctx.Err() != nil && err != nil {
		log.Info("task interrupted", "duration_ms", elapsed)
		return
	}

	finishCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finish()
	if err != nil {
		state, commitErr := w.Store.Fail(finishCtx, m, token, err.Error(), w.MaxAttempts)
		if commitErr != nil {
			log.Warn("commit failure", "error", commitErr, "duration_ms", elapsed)
			return
		}
		log.Info("task attempt finished", "state", state, "error", err, "duration_ms", elapsed)
		return
	}
	ok, err := w.Store.Succeed(finishCtx, m, token, output)
	if err != nil {
		// Redis may have committed despite a lost response. Keep published
		// candidates until reconciliation can distinguish the two cases.
		log.Warn("commit success outcome uncertain", "error", err, "duration_ms", elapsed)
		return
	}
	if !ok {
		if err := os.Remove(filepath.Join(resultDir, output)); err != nil {
			log.Warn("remove stale result", "error", err)
		}
		log.Info("task attempt finished", "state", "stale", "duration_ms", elapsed)
		return
	}
	log.Info("task attempt finished", "state", "succeeded", "duration_ms", elapsed)
}

func (w Worker) ack(ctx context.Context, log *slog.Logger, m tasks.Message) {
	if err := w.Store.Ack(ctx, m.StreamID); err != nil {
		log.Warn("acknowledge obsolete message", "error", err)
	}
}

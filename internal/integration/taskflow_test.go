package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"taskflow/internal/api"
	"taskflow/internal/convert"
	"taskflow/internal/redisresp"
	"taskflow/internal/tasks"
	"taskflow/internal/worker"
)

type fixture struct {
	repo                            *tasks.Repository
	redisDir, address, data, binary string
	process                         *exec.Cmd
}

func setup(t *testing.T) *fixture {
	t.Helper()
	if os.Getenv("TASKFLOW_INTEGRATION") != "1" {
		t.Skip("set TASKFLOW_INTEGRATION=1 to run isolated Redis integration tests")
	}
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Fatal(err)
	}
	sock, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := sock.Addr().String()
	_ = sock.Close()
	f := &fixture{repo: tasks.New(redisresp.New(address)), redisDir: t.TempDir(), address: address, data: t.TempDir(), binary: binary}
	for _, dir := range []string{"uploads", "results"} {
		if err := os.Mkdir(filepath.Join(f.data, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(f.stopRedis)
	f.startRedis(t)
	return f
}
func (f *fixture) startRedis(t *testing.T) {
	t.Helper()
	_, port, _ := net.SplitHostPort(f.address)
	f.process = exec.Command(f.binary, "--bind", "127.0.0.1", "--port", port, "--dir", f.redisDir,
		"--appendonly", "yes", "--appendfsync", "always", "--save", "", "--logfile", filepath.Join(f.redisDir, "redis.log"))
	if err := f.process.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.repo.Ready(context.Background()) == nil })
	if err := f.repo.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) stopRedis() {
	if f.process != nil {
		_ = f.process.Process.Kill()
		_ = f.process.Wait()
		f.process = nil
	}
}
func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	until := time.Now().Add(12 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func (f *fixture) runWorker(t *testing.T, exec worker.Executor) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := worker.Worker{Store: f.repo, DataDir: f.data, Lease: 3 * time.Second, MinIdle: 100 * time.Millisecond,
		Timeout: time.Second, MaxAttempts: 2, Execute: exec}
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(6 * time.Second):
				t.Error("worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func (f *fixture) submit(t *testing.T, content []byte) string {
	t.Helper()
	var body bytes.Buffer
	m := multipart.NewWriter(&body)
	part, err := m.CreateFormFile("file", "sample.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	h := (&api.Server{Store: f.repo, DataDir: f.data}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/tasks", &body)
	req.Header.Set("Content-Type", m.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 202 {
		t.Fatalf("submit %d: %s", rec.Code, rec.Body.String())
	}
	var task tasks.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	return task.ID
}

func (f *fixture) await(t *testing.T, id, status string, attempts int) tasks.Task {
	t.Helper()
	var task tasks.Task
	waitFor(t, func() bool {
		var err error
		task, err = f.repo.Get(context.Background(), id)
		if err == nil && (task.Status == "failed" || task.Status == "succeeded") && task.Status != status {
			t.Fatalf("unexpected terminal task: %+v", task)
		}
		return err == nil && task.Status == status
	})
	if task.Attempts != attempts {
		t.Fatalf("unexpected attempts: %+v", task)
	}
	return task
}

func sample(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../examples/sample.pdf")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTwoWorkersDuplicateAndPDF(t *testing.T) {
	f := setup(t)
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Fatal(err)
	}
	id1 := f.submit(t, sample(t))
	_, err := f.repo.Redis.Do(context.Background(), "XADD", tasks.Stream, "*", "task_id", id1)
	if err != nil {
		t.Fatal(err)
	}
	id2 := f.submit(t, sample(t))
	entered := make(chan string, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	fn := func(ctx context.Context, input, dir, id, token string) (string, error) {
		calls.Add(1)
		select {
		case entered <- id:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return convert.PDFToText(ctx, "pdftotext", input, dir, id, token)
	}
	f.runWorker(t, fn)
	f.runWorker(t, fn)
	got := map[string]bool{}
	for range 2 {
		select {
		case id := <-entered:
			got[id] = true
		case <-time.After(2 * time.Second):
			t.Fatal("two conversions did not overlap")
		}
	}
	close(release)
	if !got[id1] || !got[id2] {
		t.Fatalf("duplicate executed: %v", got)
	}
	h := (&api.Server{Store: f.repo, DataDir: f.data}).Handler()
	for _, id := range []string{id1, id2} {
		f.await(t, id, "succeeded", 1)
		for _, route := range []string{"result", "download"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tasks/"+id+"/"+route, nil))
			if rec.Code != 200 || !strings.Contains(rec.Body.String(), "background worker") {
				t.Fatalf("download %d: %s", rec.Code, rec.Body.String())
			}
		}
	}
	waitFor(t, func() bool {
		m, err := f.repo.Metrics(context.Background())
		return err == nil && strings.Contains(m, "taskflow_queue_pending 0") &&
			strings.Contains(m, "taskflow_tasks_finished_total{state=\"succeeded\"} 2")
	})
	if calls.Load() != 2 {
		t.Fatalf("expected two executions, got %d", calls.Load())
	}
}

func TestInvalidPDFRetriesToLimit(t *testing.T) {
	f := setup(t)
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Fatal(err)
	}
	id := f.submit(t, []byte("%PDF-1.4\ntruncated"))
	f.runWorker(t, nil)
	task := f.await(t, id, "failed", 2)
	if !strings.Contains(task.Error, "pdftotext") || task.ResultFile != "" {
		t.Fatalf("invalid failure: %+v", task)
	}
}

func TestRedisOutageDuringExecution(t *testing.T) {
	f := setup(t)
	id := f.submit(t, sample(t))
	entered := make(chan struct{})
	release := make(chan struct{})
	f.runWorker(t, func(ctx context.Context, input, dir, id, token string) (string, error) {
		select {
		case <-entered:
		default:
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return convert.PDFToText(ctx, "pdftotext", input, dir, id, token)
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	f.stopRedis()
	rec := httptest.NewRecorder()
	(&api.Server{Store: f.repo, DataDir: f.data}).Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != 503 {
		t.Fatalf("ready during outage = %d", rec.Code)
	}
	close(release)
	// Poll for the output candidate while Redis is still down, then allow the
	// failed commit attempt to complete before bringing Redis back.
	waitFor(t, func() bool {
		entries, err := os.ReadDir(filepath.Join(f.data, "results"))
		return err == nil && len(entries) == 1 && strings.HasSuffix(entries[0].Name(), ".txt")
	})
	time.Sleep(150 * time.Millisecond)
	f.startRedis(t)
	f.await(t, id, "succeeded", 2)
}

func TestEarlyRecoveryFencingAndCrashBudget(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id := f.submit(t, sample(t))
	msgs, err := f.repo.Read(ctx, "old", false)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %v %v", msgs, err)
	}
	m := msgs[0]
	claim := func(m tasks.Message, token string, lease time.Duration) tasks.ClaimState {
		state, err := f.repo.Claim(ctx, m, token, lease, 2)
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	if got := claim(m, "old", time.Hour); got != tasks.Acquired {
		t.Fatal(got)
	}
	_, reclaimed, err := f.repo.Reclaim(ctx, "new", 0, "0-0")
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("early reclaim: %v %v", reclaimed, err)
	}
	if got := claim(reclaimed[0], "new", time.Hour); got != tasks.Busy {
		t.Fatal(got)
	}
	expire := func() {
		if _, err := f.repo.Redis.Do(ctx, "HSET", tasks.Key(id), "lease_until", "1"); err != nil {
			t.Fatal(err)
		}
	}
	expire()
	if ok, err := f.repo.Succeed(ctx, m, "old", "old.txt"); ok || err != nil {
		t.Fatalf("expired commit: %v %v", ok, err)
	}
	if got := claim(reclaimed[0], "winner", time.Hour); got != tasks.Acquired {
		t.Fatal(got)
	}
	if ok, err := f.repo.Succeed(ctx, m, "old", "old.txt"); ok || err != nil {
		t.Fatalf("stale commit: %v %v", ok, err)
	}
	expire()
	if got := claim(m, "third", time.Hour); got != tasks.Exhausted {
		t.Fatal(got)
	}
	f.await(t, id, "failed", 2)
	info, err := f.repo.Metrics(ctx)
	if err != nil || !strings.Contains(info, "taskflow_queue_pending 0") {
		t.Fatalf("pending: %s %v", info, err)
	}

	broken := f.submit(t, sample(t))
	if _, err := f.repo.Redis.Do(ctx, "HSET", tasks.Key(broken), "status", "unknown"); err != nil {
		t.Fatal(err)
	}
	if got := claim(tasks.Message{TaskID: broken, StreamID: "0-0"}, "attempt", time.Hour); got != tasks.Invalid {
		t.Fatal(got)
	}
}

func TestConverterTimeout(t *testing.T) {
	if os.Getenv("TASKFLOW_INTEGRATION") != "1" {
		t.Skip("integration mode required")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "slow-poppler")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 20\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := convert.PDFToText(ctx, bin, "input.pdf", dir, "task", "token")
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("unexpected error %v", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil || len(entries) != 1 {
		t.Fatalf("partial file remains: %v %v", entries, readErr)
	}
}

func TestStaleCannotReplaceDownloadedResult(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id := f.submit(t, sample(t))
	msgs, err := f.repo.Read(ctx, "old", false)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %v %v", msgs, err)
	}
	m := msgs[0]
	old := strings.Repeat("a", 24)
	newer := strings.Repeat("b", 24)
	claim := func(token string) {
		t.Helper()
		state, err := f.repo.Claim(ctx, m, token, time.Hour, 3)
		if err != nil || state != tasks.Acquired {
			t.Fatalf("claim: %s %v", state, err)
		}
	}
	file := func(token, content string) string {
		t.Helper()
		name := id + "-" + token + ".txt"
		if err := os.WriteFile(filepath.Join(f.data, "results", name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		return name
	}
	claim(old)
	if _, err := f.repo.Redis.Do(ctx, "HSET", tasks.Key(id), "lease_until", "1"); err != nil {
		t.Fatal(err)
	}
	_, recovered, err := f.repo.Reclaim(ctx, "new", 0, "0-0")
	if err != nil || len(recovered) != 1 || recovered[0] != m {
		t.Fatalf("reclaim: %v %v", recovered, err)
	}
	claim(newer)
	winner := file(newer, "winner")
	if ok, err := f.repo.Succeed(ctx, m, newer, winner); err != nil || !ok {
		t.Fatalf("winning commit: %v %v", ok, err)
	}
	if ok, err := f.repo.Succeed(ctx, m, old, file(old, "stale")); err != nil || ok {
		t.Fatalf("stale commit: %v %v", ok, err)
	}
	if state, err := f.repo.Fail(ctx, m, old, "stale failure", 3); err != nil || state != "stale" {
		t.Fatalf("stale fail: %s %v", state, err)
	}
	h := (&api.Server{Store: f.repo, DataDir: f.data}).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tasks/"+id+"/result", nil))
	if rec.Code != 200 || rec.Body.String() != "winner" {
		t.Fatalf("download: %d %q", rec.Code, rec.Body.String())
	}
	f.await(t, id, "succeeded", 2)
}

func TestReconcileRetainsReferencedFiles(t *testing.T) {
	f := setup(t)
	id := f.submit(t, sample(t))
	stop := f.runWorker(t, nil)
	task := f.await(t, id, "succeeded", 1)
	stop()
	upload := filepath.Join(f.data, "uploads", id+".pdf")
	winner := filepath.Join(f.data, "results", task.ResultFile)
	orphan := filepath.Join(f.data, "results", id+"-"+strings.Repeat("0", 24)+".txt")
	if err := os.WriteFile(orphan, []byte("unreferenced"), 0600); err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().Add(-48 * time.Hour)
	for _, path := range []string{upload, winner, orphan} {
		if err := os.Chtimes(path, timestamp, timestamp); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("go", append([]string{"run", "./cmd/reconcile", "-data", f.data}, args...)...)
		cmd.Dir = "../.."
		cmd.Env = append(os.Environ(), "REDIS_ADDR="+f.address, "REDIS_PASSWORD=")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("reconcile: %v %s", err, out)
		}
		return string(out)
	}
	if out := run(); !strings.Contains(out, orphan) || strings.Contains(out, winner) {
		t.Fatalf("unexpected candidates: %s", out)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatal(err)
	}
	if out := run("-apply"); !strings.Contains(out, "removed "+orphan) {
		t.Fatalf("unexpected removal: %s", out)
	}
	for _, path := range []string{upload, winner} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan still present: %v", err)
	}
}

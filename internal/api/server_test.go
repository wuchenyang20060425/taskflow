package api

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"taskflow/internal/tasks"
)

type stubStore struct {
	task    tasks.Task
	created bool
}

func (s *stubStore) Create(_ context.Context, id, input string) error {
	s.created = true
	s.task = tasks.Task{ID: id, Status: "queued", InputFile: input}
	return nil
}
func (s *stubStore) Get(_ context.Context, id string) (tasks.Task, error) {
	if !s.created || s.task.ID != id {
		return tasks.Task{}, tasks.ErrNotFound
	}
	return s.task, nil
}
func (s *stubStore) Ready(context.Context) error { return nil }
func TestHealth(t *testing.T) {
	h := (&Server{Store: &stubStore{}, DataDir: t.TempDir()}).Handler()
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"service":"taskflow"`) {
		t.Fatalf("unexpected response %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/health", nil))
	if rec.Code != 405 {
		t.Fatalf("POST must be 405, got %d", rec.Code)
	}
}
func TestUploadValidAndInvalid(t *testing.T) {
	for _, tc := range []struct {
		content string
		want    int
	}{{"%PDF-1.4\nexample", 202}, {"not really a pdf", 400}} {
		store := &stubStore{}
		h := (&Server{Store: store, DataDir: t.TempDir()}).Handler()
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		part, err := writer.CreateFormFile("file", "example.pdf")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = part.Write([]byte(tc.content))
		_ = writer.Close()
		req := httptest.NewRequest("POST", "/tasks", &body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("upload %q got %d: %s", tc.content, rec.Code, rec.Body.String())
		}
	}
}
func TestNotFound(t *testing.T) {
	h := (&Server{Store: &stubStore{}, DataDir: t.TempDir()}).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/tasks/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestUploadLimitAndSafeName(t *testing.T) {
	for _, tc := range []struct{ size, want int }{
		{maxPDFSize, 202},
		{maxPDFSize + 1, 413},
	} {
		t.Run(http.StatusText(tc.want), func(t *testing.T) {
			dir := t.TempDir()
			store := &stubStore{}
			h := (&Server{Store: store, DataDir: dir}).Handler()
			var body bytes.Buffer
			w := multipart.NewWriter(&body)
			part, err := w.CreateFormFile("file", "../../outside.pdf")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write([]byte("%PDF-")); err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write(bytes.Repeat([]byte{'x'}, tc.size-5)); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/tasks", &body)
			req.Header.Set("Content-Type", w.FormDataContentType())
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			files, err := os.ReadDir(filepath.Join(dir, "uploads"))
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == 202 {
				if len(files) != 1 || files[0].Name() != store.task.ID+".pdf" {
					t.Fatalf("unsafe files: %v", files)
				}
			} else if len(files) != 0 || store.created || !strings.Contains(rec.Body.String(), "upload_too_large") {
				t.Fatalf("oversize upload was not cleaned: %v %s", files, rec.Body.String())
			}
		})
	}
}

func TestStoredResultPathRejectsOtherFiles(t *testing.T) {
	id := strings.Repeat("a", 32)
	store := &stubStore{created: true, task: tasks.Task{ID: id, Status: "succeeded", ResultFile: "../secrets.txt"}}
	h := (&Server{Store: store, DataDir: t.TempDir()}).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tasks/"+id+"/result", nil))
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "result_unavailable") {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
}

func TestCancelledUploadDoesNotEnqueue(t *testing.T) {
	var body bytes.Buffer
	m := multipart.NewWriter(&body)
	p, err := m.CreateFormFile("file", "file.pdf")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = p.Write([]byte("%PDF-1.4"))
	_ = m.Close()
	store := &stubStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := (&Server{Store: store, DataDir: t.TempDir()}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/tasks", &body).WithContext(ctx)
	req.Header.Set("Content-Type", m.FormDataContentType())
	h.ServeHTTP(httptest.NewRecorder(), req)
	if store.created {
		t.Fatal("cancelled request was published")
	}
}

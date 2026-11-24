package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"taskflow/internal/tasks"
)

const maxPDFSize = 20 << 20

type Store interface {
	Create(context.Context, string, string) error
	Get(context.Context, string) (tasks.Task, error)
	Ready(context.Context) error
}

type Server struct {
	Store   Store
	DataDir string
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID, err := newID()
		if err != nil {
			problem(w, 500, "internal", "could not generate request ID")
			return
		}
		start := time.Now()
		w.Header().Set("X-Request-ID", requestID)
		log := slog.With("request_id", requestID, "method", r.Method)
		switch r.URL.Path {
		case "/health":
			if allowed(w, r, http.MethodGet) {
				jsonResponse(w, 200, map[string]string{"status": "ok", "service": "taskflow"})
			}
		case "/ready":
			if allowed(w, r, http.MethodGet) {
				s.ready(w, r)
			}
		case "/metrics":
			if allowed(w, r, http.MethodGet) {
				s.metrics(w, r)
			}
		case "/tasks":
			if allowed(w, r, http.MethodPost) {
				s.create(w, r, log)
			}
		default:
			if strings.HasPrefix(r.URL.Path, "/tasks/") {
				id, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/tasks/"), "/")
				if tasks.ValidID(id) {
					log = log.With("task_id", id)
				}
				if allowed(w, r, http.MethodGet) {
					s.taskByID(w, r, log)
				}
			} else {
				problem(w, 404, "not_found", "route not found")
			}
		}
		log.Debug("request finished", "duration_ms", time.Since(start).Milliseconds())
	})
}

func allowed(w http.ResponseWriter, r *http.Request, verb string) bool {
	if r.Method == verb {
		return true
	}
	w.Header().Set("Allow", verb)
	problem(w, 405, "method_not_allowed", "method not allowed")
	return false
}

func problem(w http.ResponseWriter, status int, code, message string) {
	jsonResponse(w, status, map[string]string{"code": code, "error": message})
}

func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ready(ctx); err != nil {
		problem(w, 503, "redis_unavailable", "Redis is unavailable")
		return
	}
	for _, dir := range []string{"uploads", "results"} {
		info, err := os.Stat(filepath.Join(s.DataDir, dir))
		if err != nil || !info.IsDir() {
			problem(w, 503, "storage_unavailable", "task storage is unavailable")
			return
		}
	}
	jsonResponse(w, 200, map[string]string{"status": "ready"})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, log *slog.Logger) {
	r.Body = http.MaxBytesReader(w, r.Body, maxPDFSize+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		problem(w, 400, "invalid_form", "expected multipart field named file")
		return
	}
	id, err := newID()
	if err != nil {
		problem(w, 500, "internal", "could not generate task ID")
		return
	}
	log = log.With("task_id", id)
	dir := filepath.Join(s.DataDir, "uploads")
	if err := os.MkdirAll(dir, 0755); err != nil {
		problem(w, 500, "storage_unavailable", "upload storage is unavailable")
		return
	}
	name := id + ".pdf"
	path := filepath.Join(dir, name)
	if err := saveUpload(reader, path); err != nil {
		var storageErr *os.PathError
		var limitErr *http.MaxBytesError
		switch {
		case errors.As(err, &limitErr), errors.Is(err, errPDFTooLarge):
			problem(w, 413, "upload_too_large", "PDF must be at most 20 MiB")
		case errors.As(err, &storageErr):
			problem(w, 500, "storage_unavailable", "could not save PDF")
		default:
			problem(w, 400, "invalid_pdf", "expected one PDF file starting with %PDF-")
		}
		return
	}
	if r.Context().Err() != nil {
		_ = os.Remove(path)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.Store.Create(ctx, id, name); err != nil {
		// The Redis command may have applied before the connection failed.
		log.Warn("task submission outcome uncertain", "error", err)
		problem(w, 503, "queue_unavailable", "queue unavailable")
		return
	}
	log.Info("task accepted")
	location := "/tasks/" + id
	w.Header().Set("Location", location)
	jsonResponse(w, 202, map[string]string{"id": id, "status": "queued", "status_url": location})
}

var errInvalidPDF = errors.New("invalid PDF upload")
var errPDFTooLarge = errors.New("PDF exceeds file size limit")

func saveUpload(reader *multipart.Reader, path string) (err error) {
	found := false
	defer func() {
		if found && err != nil {
			_ = os.Remove(path)
		}
	}()
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if part.FormName() != "file" || part.FileName() == "" {
			if _, err := io.Copy(io.Discard, part); err != nil {
				return err
			}
			continue
		}
		if found {
			return errInvalidPDF
		}
		var header [5]byte
		if _, err := io.ReadFull(part, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return errInvalidPDF
			}
			return err
		}
		if string(header[:]) != "%PDF-" {
			return errInvalidPDF
		}
		dst, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		found = true
		_, writeErr := dst.Write(header[:])
		var count int64
		if writeErr == nil {
			count, writeErr = io.Copy(dst, io.LimitReader(part, maxPDFSize-int64(len(header))+1))
		}
		closeErr := dst.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
		if count+int64(len(header)) > maxPDFSize {
			return errPDFTooLarge
		}
	}
	if !found {
		return errInvalidPDF
	}
	return nil
}

func (s *Server) taskByID(w http.ResponseWriter, r *http.Request, log *slog.Logger) {
	id, suffix, slash := strings.Cut(strings.TrimPrefix(r.URL.Path, "/tasks/"), "/")
	if !tasks.ValidID(id) || (slash && suffix != "result" && suffix != "download") {
		problem(w, 404, "not_found", "task route not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	t, err := s.Store.Get(ctx, id)
	if errors.Is(err, tasks.ErrNotFound) {
		problem(w, 404, "not_found", "task not found")
		return
	}
	if err != nil {
		log.Warn("task lookup", "error", err)
		problem(w, 503, "queue_unavailable", "task lookup unavailable")
		return
	}
	if !slash {
		jsonResponse(w, 200, t)
		return
	}
	if t.Status != "succeeded" {
		problem(w, 409, "result_not_ready", "task has no successful result")
		return
	}
	if !tasks.ValidResult(id, t.ResultFile) {
		problem(w, 500, "result_unavailable", "invalid stored result reference")
		return
	}
	file, err := os.Open(filepath.Join(s.DataDir, "results", t.ResultFile))
	if err != nil {
		problem(w, 500, "result_unavailable", "stored result is unavailable")
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		problem(w, 500, "result_unavailable", "stored result is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", id+".txt"))
	http.ServeContent(w, r, id+".txt", info.ModTime(), file)
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

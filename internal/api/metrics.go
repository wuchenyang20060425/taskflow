package api

import (
	"context"
	"io"
	"net/http"
	"time"
)

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	store, ok := s.Store.(interface {
		Metrics(context.Context) (string, error)
	})
	if !ok {
		problem(w, 503, "metrics_unavailable", "metrics unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	body, err := store.Metrics(ctx)
	if err != nil {
		problem(w, 503, "metrics_unavailable", "metrics unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, body)
}

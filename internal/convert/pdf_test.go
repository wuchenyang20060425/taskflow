package convert

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMissingInput(t *testing.T) {
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Skip("pdftotext not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := PDFToText(ctx, "pdftotext", "/nonexistent/taskflow.pdf", t.TempDir(), "a", "b")
	if err == nil || !strings.Contains(err.Error(), "pdftotext") {
		t.Fatalf("expected conversion error, got %v", err)
	}
}
func TestOutputDir(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "out")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
}

func TestDiagnosticKeepsBoundedPrefix(t *testing.T) {
	var d diagnostic
	n, err := io.Copy(&d, io.LimitReader(strings.NewReader(strings.Repeat("x", 1000)), 1000))
	if err != nil || n != 1000 || len(d.String()) != 200 {
		t.Fatalf("copied=%d retained=%d error=%v", n, len(d.String()), err)
	}
}

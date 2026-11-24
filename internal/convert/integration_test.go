package convert

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSamplePDFConversion(t *testing.T) {
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Skip("pdftotext not installed")
	}
	input := filepath.Join("..", "..", "examples", "sample.pdf")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := PDFToText(ctx, "pdftotext", input, t.TempDir(), "example", "attempt1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(output, ".txt") {
		t.Fatalf("unexpected output name %q", output)
	}
	// The function returns only the file name. The output path is inside the results directory.
}
func TestConverterOutput(t *testing.T) {
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Skip("pdftotext not installed")
	}
	resultsDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	file, err := PDFToText(ctx, "pdftotext", filepath.Join("..", "..", "examples", "sample.pdf"), resultsDir, "demo", "token")
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(resultsDir, file))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "background worker") {
		t.Fatalf("missing expected text: %q", string(content))
	}
}

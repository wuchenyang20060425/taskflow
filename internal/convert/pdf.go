package convert

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// PDFToText writes an attempt-specific file and publishes it with a rename in
// the same directory. Only the worker's Redis commit makes it downloadable.
func PDFToText(ctx context.Context, bin, input, resultsDir, taskID, token string) (string, error) {
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return "", err
	}
	temp, err := os.CreateTemp(resultsDir, taskID+"-"+token+"-*.partial")
	if err != nil {
		return "", err
	}
	defer os.Remove(temp.Name())
	if err := temp.Close(); err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, bin, "-layout", "-enc", "UTF-8", input, temp.Name())
	cmd.WaitDelay = time.Second
	var stderr diagnostic
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", errors.New("PDF conversion timed out")
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("pdftotext: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	file, err := os.Open(temp.Name())
	if err != nil {
		return "", err
	}
	// Poppler can emit only whitespace and form feeds for a scanned PDF.
	textErr := requireText(file)
	closeErr := file.Close()
	if textErr != nil {
		return "", textErr
	}
	if closeErr != nil {
		return "", closeErr
	}

	filename := taskID + "-" + token + ".txt"
	if err := os.Rename(temp.Name(), filepath.Join(resultsDir, filename)); err != nil {
		return "", err
	}
	return filename, nil
}

func requireText(r io.Reader) error {
	var buffer [4096]byte
	for {
		n, err := r.Read(buffer[:])
		for _, b := range buffer[:n] {
			if !strings.ContainsRune(" \t\r\n\f\v", rune(b)) {
				return nil
			}
		}
		if errors.Is(err, io.EOF) {
			return errors.New("no extractable text: scanned PDFs require OCR")
		}
		if err != nil {
			return err
		}
	}
}

type diagnostic struct{ text strings.Builder }

func (d *diagnostic) String() string { return d.text.String() }

func (d *diagnostic) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := 200 - d.text.Len(); remaining > 0 {
		_, _ = d.text.Write(p[:min(n, remaining)])
	}
	return n, nil
}

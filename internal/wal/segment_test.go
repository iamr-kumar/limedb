package wal

import (
	"os"
	"strings"
	"testing"
)

func TestWalSegmentWriteAndSync(t *testing.T) {
	tempDir := t.TempDir()

	segment, err := NewWalSegment(1, tempDir, 8)
	if err != nil {
		t.Fatalf("NewWalSegment() error = %v", err)
	}
	defer segment.Close()

	payload := []byte("hello wal")

	if err := segment.Write(payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if got := segment.Size(); got != int64(len(payload)) {
		t.Fatalf("segment size = %d, want %d", got, len(payload))
	}

	if err := segment.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	info, err := os.Stat(segment.Path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	if info.Size() != int64(len(payload)) {
		t.Fatalf("segment file size = %d, want %d", info.Size(), len(payload))
	}
}

func TestWalSegmentReopenTracksExistingSize(t *testing.T) {
	tempDir := t.TempDir()
	payload := []byte("persisted")

	original, err := NewWalSegment(7, tempDir, 16)
	if err != nil {
		t.Fatalf("NewWalSegment() error = %v", err)
	}

	if err := original.Write(payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := original.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if err := original.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := NewWalSegment(7, tempDir, 16)
	if err != nil {
		t.Fatalf("NewWalSegment() reopen error = %v", err)
	}
	defer reopened.Close()

	if got := reopened.Size(); got != int64(len(payload)) {
		t.Fatalf("reopened size = %d, want %d", got, len(payload))
	}

	more := []byte("-more")
	if err := reopened.Write(more); err != nil {
		t.Fatalf("Write() after reopen error = %v", err)
	}

	if got := reopened.Size(); got != int64(len(payload)+len(more)) {
		t.Fatalf("reopened size = %d, want %d", got, len(payload)+len(more))
	}
}

func TestWalSegmentClosePreventsWritesAndSync(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewWalSegment(3, tempDir, 4)
	if err != nil {
		t.Fatalf("NewWalSegment() error = %v", err)
	}

	if err := segment.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := segment.Write([]byte("data")); err == nil {
		t.Fatal("expected error when writing to closed segment")
	} else if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("unexpected write error: %v", err)
	}

	if err := segment.Sync(); err == nil {
		t.Fatal("expected error when syncing closed segment")
	}

	if err := segment.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

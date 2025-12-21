package wal

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func getLastSync(w *WAL) time.Time {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.lastSync
}

func TestWALWriteSyncImmediate(t *testing.T) {
	tempDir := t.TempDir()
	wal, err := NewWAL(NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate))
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	entry := &WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "key1",
		Value:     "value1",
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	initialSync := getLastSync(wal)

	if err := wal.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if got := atomic.LoadInt64(&wal.bytesWritten); got != int64(len(data)) {
		t.Fatalf("bytesWritten = %d, want %d", got, len(data))
	}
	if got := atomic.LoadInt64(&wal.entriesCount); got != 1 {
		t.Fatalf("entriesCount = %d, want 1", got)
	}

	if syncAfter := getLastSync(wal); !syncAfter.After(initialSync) {
		t.Fatalf("lastSync did not advance; got %v, initial %v", syncAfter, initialSync)
	}

	info, err := os.Stat(wal.path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() < int64(len(data)) {
		t.Fatalf("wal file size = %d, want at least %d", info.Size(), len(data))
	}
}

func TestWALIntervalSyncUpdatesLastSync(t *testing.T) {
	tempDir := t.TempDir()
	wal, err := NewWAL(NewWALOptions().
		WithDataDir(tempDir).
		WithSyncMode(SyncInterval).
		WithSyncInterval(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	initial := getLastSync(wal)
	time.Sleep(50 * time.Millisecond)
	after := getLastSync(wal)

	if !after.After(initial) {
		t.Fatalf("lastSync did not advance for interval sync; initial %v, after %v", initial, after)
	}
}

func TestWALClosePreventsFurtherWrites(t *testing.T) {
	tempDir := t.TempDir()
	wal, err := NewWAL(NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate))
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	entry := &WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "key-close",
		Value:     "value-close",
	}

	if err := wal.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := wal.Write(entry); err == nil {
		t.Fatal("expected Write after close to fail, got nil error")
	} else if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("unexpected error after close: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	info, err := os.Stat(wal.path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("wal file size is zero; expected flushed content")
	}
}

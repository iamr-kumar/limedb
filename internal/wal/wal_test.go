package wal

import (
	"fmt"
	"os"
	"strings"
	"sync"
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
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = DefaultSegmentSize

	wal, err := NewWAL(opts)
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

	initialSync := getLastSync(wal)

	if err := wal.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if entry.SequenceID != 1 {
		t.Fatalf("SequenceID = %d, want 1", entry.SequenceID)
	}

	if syncAfter := getLastSync(wal); !syncAfter.After(initialSync) {
		t.Fatalf("lastSync did not advance; got %v, initial %v", syncAfter, initialSync)
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	info, err := os.Stat(wal.activeSegment.Path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() < int64(len(data)) {
		t.Fatalf("wal file size = %d, want at least %d", info.Size(), len(data))
	}
}

func TestWALIntervalSyncUpdatesLastSync(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().
		WithDataDir(tempDir).
		WithSyncMode(SyncInterval).
		WithSyncInterval(10 * time.Millisecond)
	opts.MaxSegmentSize = DefaultSegmentSize

	wal, err := NewWAL(opts)
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
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = DefaultSegmentSize

	wal, err := NewWAL(opts)
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

	info, err := os.Stat(wal.activeSegment.Path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("wal file size is zero; expected flushed content")
	}
}

func TestWALRotatesSegmentsWhenMaxSizeReached(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = 1

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	entry1 := &WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "key-1",
		Value:     "value-1",
	}

	entry2 := &WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "key-2",
		Value:     "value-2",
	}

	if err := wal.Write(entry1); err != nil {
		t.Fatalf("Write entry1 error = %v", err)
	}

	firstSegmentID := wal.GetActiveSegmentID()

	if err := wal.Write(entry2); err != nil {
		t.Fatalf("Write entry2 error = %v", err)
	}

	if got := wal.GetActiveSegmentID(); got == firstSegmentID {
		t.Fatalf("expected rotation to new segment, still on %d", got)
	}

	if len(wal.segments) != 2 {
		t.Fatalf("expected 2 segments after rotation, got %d", len(wal.segments))
	}

	oldSegment := wal.segments[firstSegmentID]
	if !oldSegment.closed {
		t.Fatalf("expected old segment %d to be closed", firstSegmentID)
	}

	if got := oldSegment.maxSeqId; got != atomic.LoadUint64(&wal.lastSeqId) {
		t.Fatalf("old segment maxSeqId = %d, want %d", got, atomic.LoadUint64(&wal.lastSeqId))
	}

	data2, err := entry2.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	newSegment := wal.segments[wal.GetActiveSegmentID()]
	if newSegment.Size() < int64(len(data2)) {
		t.Fatalf("new segment size = %d, want at least %d", newSegment.Size(), len(data2))
	}
}

func TestWALConcurrentWritesAssignUniqueSequenceIDs(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = DefaultSegmentSize

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	const writers = 5
	const perWriter = 25
	total := writers * perWriter

	seqCh := make(chan uint64, total)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				entry := &WalEntry{
					Timestamp: time.Now(),
					Operation: OperationSet,
					Key:       fmt.Sprintf("k-%d-%d", id, j),
					Value:     "v",
				}

				if err := wal.Write(entry); err != nil {
					t.Errorf("writer %d Write() error = %v", id, err)
					return
				}
				seqCh <- entry.SequenceID
			}
		}(i)
	}

	wg.Wait()
	close(seqCh)

	seen := make(map[uint64]struct{}, total)
	for seq := range seqCh {
		if _, exists := seen[seq]; exists {
			t.Fatalf("duplicate SequenceID detected: %d", seq)
		}
		seen[seq] = struct{}{}
	}

	if got := atomic.LoadUint64(&wal.lastSeqId); got != uint64(total) {
		t.Fatalf("lastSeqId = %d, want %d", got, total)
	}

	if len(seen) != total {
		t.Fatalf("unique SequenceIDs = %d, want %d", len(seen), total)
	}
}

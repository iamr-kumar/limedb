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

	// oldSegment should have maxSeqId = 1 (only entry1 was written before rotation)
	if got := oldSegment.maxSeqId; got != entry1.SequenceID {
		t.Fatalf("old segment maxSeqId = %d, want %d", got, entry1.SequenceID)
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

func TestDeleteSegmentsBeforeDeletesOldSegments(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = 1 // force rotation on each write

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	// Write 3 entries, forcing 3 segments (seg0, seg1, seg2 active)
	for i := 0; i < 3; i++ {
		entry := &WalEntry{
			Timestamp: time.Now(),
			Operation: OperationSet,
			Key:       fmt.Sprintf("key-%d", i),
			Value:     "value",
		}
		if err := wal.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	if len(wal.segments) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(wal.segments))
	}

	// Segment 0: maxSeqId=1, Segment 1: maxSeqId=2, Segment 2 (active): maxSeqId=0
	// Delete segments with maxSeqId <= 1
	if err := wal.DeleteSegmentsBefore(1); err != nil {
		t.Fatalf("DeleteSegmentsBefore() error = %v", err)
	}

	if len(wal.segments) != 2 {
		t.Fatalf("expected 2 segments after deletion, got %d", len(wal.segments))
	}

	// Segment 0 should be deleted
	if _, exists := wal.segments[0]; exists {
		t.Fatal("segment 0 should have been deleted")
	}

	// Segment 1 (maxSeqId=2) should remain
	if _, exists := wal.segments[1]; !exists {
		t.Fatal("segment 1 should still exist")
	}
}

func TestDeleteSegmentsBeforeRetainsNewerSegments(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = 1

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	// Write 4 entries → 4 segments
	for i := 0; i < 4; i++ {
		entry := &WalEntry{
			Timestamp: time.Now(),
			Operation: OperationSet,
			Key:       fmt.Sprintf("key-%d", i),
			Value:     "value",
		}
		if err := wal.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Segments: 0(max=1), 1(max=2), 2(max=3), 3(active, max=0)
	// Delete only segments with maxSeqId <= 2
	if err := wal.DeleteSegmentsBefore(2); err != nil {
		t.Fatalf("DeleteSegmentsBefore() error = %v", err)
	}

	// Segments 0 and 1 deleted, 2 and 3 remain
	if len(wal.segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(wal.segments))
	}

	if _, exists := wal.segments[2]; !exists {
		t.Fatal("segment 2 (maxSeqId=3) should be retained")
	}
}

func TestDeleteSegmentsBeforeNeverDeletesActiveSegment(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = DefaultSegmentSize // no rotation

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	entry := &WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "key",
		Value:     "value",
	}
	if err := wal.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	activeID := wal.GetActiveSegmentID()

	// Try to delete with seqId higher than anything written
	if err := wal.DeleteSegmentsBefore(999); err != nil {
		t.Fatalf("DeleteSegmentsBefore() error = %v", err)
	}

	// Active segment should still exist
	if len(wal.segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(wal.segments))
	}

	if _, exists := wal.segments[activeID]; !exists {
		t.Fatal("active segment should never be deleted")
	}
}

func TestDeleteSegmentsBeforeNoOpWhenNoMatchingSegments(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir).WithSyncMode(SyncImmediate)
	opts.MaxSegmentSize = 1

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer wal.Close()

	// Write 2 entries → segments 0(max=1), 1(active)
	for i := 0; i < 2; i++ {
		entry := &WalEntry{
			Timestamp: time.Now(),
			Operation: OperationSet,
			Key:       fmt.Sprintf("key-%d", i),
			Value:     "value",
		}
		if err := wal.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	initialCount := len(wal.segments)

	// seqId=0 should match nothing (maxSeqId starts at 1)
	if err := wal.DeleteSegmentsBefore(0); err != nil {
		t.Fatalf("DeleteSegmentsBefore() error = %v", err)
	}

	if len(wal.segments) != initialCount {
		t.Fatalf("expected %d segments unchanged, got %d", initialCount, len(wal.segments))
	}
}

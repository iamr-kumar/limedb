package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// WalSegment represents a single segment file in the WAL
// Each segment is an append-only file
// These are rotated when they reach a certain size limit
// or during periodic maintenance
// Each segment has a unique ID and corresponds to a file on disk
// File naming convention: 000001.wal, 000002.wal, etc.
// Operations on segments are not-thread safe and require external synchronization and locking
type WalSegment struct {
	Id       uint32        // Segment identifier
	Path     string        // Path to the segment file
	file     *os.File      // Underlying file handle
	writer   *bufio.Writer // Buffered writer
	size     int64         // Current size of the segment
	maxSeqId uint64        // Maximum SequenceID in this segment
	closed   bool          // Indicates if the segment is closed
}

// NewWalSegment creates a new WAL segment with the given ID and file path
func NewWalSegment(id uint32, dir string, bufferSize int) (*WalSegment, error) {
	// Create the underlying file
	fileName := getSegmentFileName(id)
	filePath := filepath.Join(dir, fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file %s: %w", filePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat segment file %s: %w", filePath, err)
	}

	segment := &WalSegment{
		Id:     id,
		Path:   filePath,
		file:   file,
		writer: bufio.NewWriterSize(file, bufferSize),
		size:   info.Size(), // Set to current file size if reopening,
		closed: false,
	}
	return segment, nil
}

// getSegmentFileName generates the file name for a segment based on its ID
func getSegmentFileName(id uint32) string {
	return fmt.Sprintf("%06d.wal", id) // Zero-padded 6-digit segment ID
}

// Write writes data to the segment
func (segment *WalSegment) Write(data []byte) error {
	if segment.closed {
		return fmt.Errorf("cannot write to closed segment %d", segment.Id)
	}

	n, err := segment.writer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to segment %d: %w", segment.Id, err)
	}

	atomic.AddInt64(&segment.size, int64(n))
	return nil
}

// Sync flushes the buffered data to disk
func (segment *WalSegment) Sync() error {
	if segment.closed {
		return fmt.Errorf("segment with id %d is closed", segment.Id)
	}

	// Flush buffered data to the file - this goes to OS buffer
	if err := segment.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer for segment %d: %w", segment.Id, err)
	}

	// Sync file to disk
	if err := segment.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment %d to disk: %w", segment.Id, err)
	}

	return nil
}

// Close closes the segment file
func (segment *WalSegment) Close() error {
	if segment.closed {
		return nil
	}

	// Ensure all data is flushed and synced
	if err := segment.Sync(); err != nil {
		return err
	}

	segment.closed = true
	return segment.file.Close()
}

// Size returns the current size of the segment
func (segment *WalSegment) Size() int64 {
	return atomic.LoadInt64(&segment.size)
}

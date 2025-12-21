package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type SyncMode int

const (
	SyncImmediate SyncMode = iota
	SyncInterval
)

// WAL represents the Write-Ahead Log structure
type WAL struct {
	mutex  sync.Mutex    // protects concurrent access
	path   string        // path to the WAL file
	file   *os.File      // underlying file
	writer *bufio.Writer // buffered writer

	syncMode     SyncMode      // sync mode
	syncInterval time.Duration // in milliseconds

	bytesWritten int64     // total bytes written
	entriesCount int64     // total entries written
	lastSync     time.Time // last sync time

	closed  bool           // indicates if WAL is closed
	closeCh chan struct{}  // channel to signal closure
	wg      sync.WaitGroup // wait group for background goroutines
}

func NewWAL(options *WALOptions) (*WAL, error) {
	if options == nil {
		options = NewWALOptions()
	} else {
		options = options.Clone() // avoid modifying the original options
	}

	// Apply default values for missing options
	options.applyDefaults()

	// Validate options
	if err := options.Validate(); err != nil {
		return nil, fmt.Errorf("Invalid WAL options: %w", err)
	}

	// Create required directories
	if err := options.EnsureDirectories(); err != nil {
		return nil, fmt.Errorf("Failed to create WAL directories: %w", err)
	}

	// Open WAL file
	walPath := options.WALFilePath()
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file:         file,
		path:         walPath,
		writer:       bufio.NewWriterSize(file, options.BufferSize),
		syncMode:     options.SyncMode,
		syncInterval: options.SyncInterval,
		closeCh:      make(chan struct{}),
		lastSync:     time.Now(),
	}

	// Start background sync goroutine if needed
	if wal.syncMode == SyncInterval {
		wal.startIntervalSync()
	}

	return wal, nil
}

// Write appends a WalEntry to the WAL
func (wal *WAL) Write(entry *WalEntry) error {
	data, err := entry.Serialize()
	if err != nil {
		return fmt.Errorf("Failed to serialize WAL entry: %w", err)
	}

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Write data to buffer
	n, err := wal.writer.Write(data)
	if err != nil {
		return fmt.Errorf("Failed to write to WAL: %w", err)
	}

	// Update metrics
	// Thread safe updates
	atomic.AddInt64(&wal.bytesWritten, int64(n))
	atomic.AddInt64(&wal.entriesCount, 1)

	// Sync immediately if in immediate mode
	if wal.syncMode == SyncImmediate {
		return wal.syncLocked()
	}

	return nil
}

// syncLocked performs a sync of the WAL file to disk
// Assumes wal.mutex is already locked by the caller
func (wal *WAL) syncLocked() error {
	// flush buffer to file
	// this only puts the data from buffer to OS cache
	if err := wal.writer.Flush(); err != nil {
		return fmt.Errorf("Failed to flush WAL buffer: %w", err)
	}

	// explicitly sync file to disk
	// forces OS to write data from cache to disk
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("Failed to sync WAL file: %w", err)
	}

	wal.lastSync = time.Now()
	return nil
}

// Sync forces a sync of the WAL file to disk
func (wal *WAL) Sync() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	return wal.syncLocked()
}

// startIntervalSync starts a background goroutine to sync the WAL at intervals
func (wal *WAL) startIntervalSync() {
	// Add to wait group
	wal.wg.Add(1)
	go func() {
		defer wal.wg.Done()
		ticker := time.NewTicker(wal.syncInterval)
		defer ticker.Stop()

		for {
			select {
			// on each tick, perform sync
			case <-ticker.C:
				wal.mutex.Lock()
				if !wal.closed {
					if err := wal.syncLocked(); err != nil {
						fmt.Printf("WAL interval sync error: %v\n", err)
					}
				}
				wal.mutex.Unlock()
			// exit on close signal
			case <-wal.closeCh:
				return
			}
		}
	}()
}

// Close closes the WAL and releases resources
func (wal *WAL) Close() error {
	// Get lock to prevent new writes
	wal.mutex.Lock()

	if wal.closed {
		wal.mutex.Unlock()
		return nil
	}

	wal.closed = true
	// Signal background goroutines to stop
	close(wal.closeCh)

	// Release lock before waiting
	wal.mutex.Unlock()
	// Wait for goroutines to finish
	wal.wg.Wait()

	// Final sync — no lock needed, we're the only accessor now
	if err := wal.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	return wal.file.Close()
}

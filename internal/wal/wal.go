package wal

import (
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
	mutex sync.Mutex // protects concurrent access
	dir   string     // directory for WAL files

	// segment management
	activeSegment     *WalSegment            // current active segment
	segments          map[uint32]*WalSegment // all segments by ID not yet merged or deleted
	nextSegmentID     uint32                 // next segment ID to use
	maxSegmentSize    int64                  // max size of each segment in bytes (64MB default)
	segmentBufferSize int                    // buffer size for segment writes

	// SequenceID management
	lastSeqId uint64 // next sequence ID to assign

	syncMode     SyncMode      // sync mode
	syncInterval time.Duration // in milliseconds
	lastSync     time.Time     // last sync time

	closed  bool           // indicates if WAL is closed
	closeCh chan struct{}  // channel to signal closure
	wg      sync.WaitGroup // wait group for background goroutines
}

// NewWAL creates a new Write-Ahead Log with the given options
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

	// Create new wal segment file
	activeSegment, err := NewWalSegment(0, options.WALDir(), options.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create initial WAL segment: %w", err)
	}

	// Initialize segments map
	segmentsMap := make(map[uint32]*WalSegment)
	segmentsMap[activeSegment.Id] = activeSegment

	// TODO: Load existing segments from disk if needed and use that to initialize lastSeqId
	wal := &WAL{
		dir:               options.WALDir(),
		activeSegment:     activeSegment,
		segments:          segmentsMap,
		nextSegmentID:     activeSegment.Id + 1,
		lastSeqId:         0,
		maxSegmentSize:    options.MaxSegmentSize,
		segmentBufferSize: options.BufferSize,
		syncMode:          options.SyncMode,
		syncInterval:      options.SyncInterval,
		closeCh:           make(chan struct{}),
		lastSync:          time.Now(),
	}

	// Start background sync goroutine if needed
	if wal.syncMode == SyncInterval {
		wal.startIntervalSync()
	}

	return wal, nil
}

// Write appends a WalEntry to the WAL
func (wal *WAL) Write(entry *WalEntry) error {
	// Assign SequenceID
	entry.SequenceID = atomic.AddUint64(&wal.lastSeqId, 1)
	data, err := entry.Serialize()
	if err != nil {
		return fmt.Errorf("Failed to serialize WAL entry: %w", err)
	}

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.closed {
		return fmt.Errorf("Failed to write to closed WAL")
	}

	// Check if active segment needs rotation
	if wal.activeSegment.Size() >= wal.maxSegmentSize {
		if err := wal.rotateSegmentLocked(); err != nil {
			return fmt.Errorf("Failed to rotate WAL segment: %w", err)
		}
	}

	// write to active segment
	if err := wal.activeSegment.Write(data); err != nil {
		return fmt.Errorf("Failed to write to WAL: %w", err)
	}

	// Sync immediately if in immediate mode
	if wal.syncMode == SyncImmediate {
		return wal.syncLocked()
	}

	return nil
}

// syncLocked performs a sync of the WAL file to disk
// Assumes wal.mutex is already locked by the caller
func (wal *WAL) syncLocked() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return fmt.Errorf("Failed to sync WAL: %w", err)
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

	// Lock again to safely close segments
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	// Close all segments
	var firstErr error
	for _, seg := range wal.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// rotateSegmentLocked rotates the active segment
// Assumes wal.mutex is already locked by the caller
func (wal *WAL) rotateSegmentLocked() error {
	// Create new wal segment file
	newSegmentID := wal.nextSegmentID
	newSegment, err := NewWalSegment(newSegmentID, wal.dir, wal.segmentBufferSize)
	if err != nil {
		return fmt.Errorf("Failed to create new WAL segment: %w", err)
	}

	// Update maxSeqId for the old segment
	// Note: lastSeqId has already been incremented for the current entry being written,
	// but that entry will go to the NEW segment, so old segment's max is lastSeqId - 1
	wal.activeSegment.maxSeqId = atomic.LoadUint64(&wal.lastSeqId) - 1
	// Close current active segment
	if err := wal.activeSegment.Close(); err != nil {
		// Cleanup new segment on failure
		newSegment.Close()
		if closeErr := os.Remove(newSegment.Path); closeErr != nil {
			// Let's swallow the close error if we already have an error
			// The goroutine that cleans up old segments will eventually remove this file
			fmt.Printf("Failed to remove new segment file %s after rotation failure: %v\n", newSegment.Path, closeErr)
		}
		return fmt.Errorf("Failed to close active WAL segment during rotation: %w", err)
	}
	// Update WAL state
	wal.segments[newSegment.Id] = newSegment
	wal.activeSegment = newSegment
	wal.nextSegmentID++

	return nil
}

// GetActiveSegmentID returns the ID of the current active segment
func (wal *WAL) GetActiveSegmentID() uint32 {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	return wal.activeSegment.Id
}

// DeleteSegmentsBefore deletes all segments with maxSeqId less than or equal to the given seqId
func (wal *WAL) DeleteSegmentsBefore(seqId uint64) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	for id, segment := range wal.segments {
		if segment == wal.activeSegment {
			continue // never delete active segment
		}
		if segment.maxSeqId != 0 && segment.maxSeqId <= seqId {
			// Close segment before deletion but do not error out if already closed
			segment.Close()
			// Delete segment file
			if err := os.Remove(segment.Path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("Failed to delete WAL segment file %s: %w", segment.Path, err)
			}
			delete(wal.segments, id)
		}
	}
	return nil
}

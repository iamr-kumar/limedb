package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type OperationType uint8

const (
	OperationSet OperationType = iota
	OperationDelete
	OperationUpdate
)

const (
	DefaultBufferSize = 64 * 1024        // 64KB write buffer
	MaxEntrySize      = 10 * 1024 * 1024 // 10MB max entry size
)

// Entry represents a single WAL entry
type Entry struct {
	Timestamp  time.Time     // When operation occurred
	Operation  OperationType // Type of operation
	Key        string        // The key affected by the operation
	Value      string        // The value associated with the key
	TTL        int64         // Time to live for the entry
	SequenceID uint64        // Unique identifier for the entry
	CheckSum   uint32        // Checksum for data integrity
}

// WAL represents the Write-Ahead Log for a single database
type WAL struct {
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	path       string
	database   string
	sequenceID uint64
	syncMode   SyncMode
	seqFile    *SequenceFile

	bytesWritten uint64
	entriesCount uint64
	lastSync     time.Time

	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

type SyncMode int

const (
	SyncImmediate SyncMode = iota // Sync after every write
	SyncInterval                  // Sync at regular intervals
	SyncBatch                     // Sync after N operations
)

// Config for creating a new WAL
type Config struct {
	Database     string
	DataDir      string // Base data directory
	SyncMode     SyncMode
	BatchSize    int           // Used if SyncMode is SyncBatch
	SyncInterval time.Duration // Used if SyncMode is SyncInterval
}

type SequenceFile struct {
	path string
	mu   sync.Mutex
}

func NewSequenceFile(walDir string) *SequenceFile {
	return &SequenceFile{
		path: filepath.Join(walDir, ".sequence"),
	}
}

// Load reads the last seqeuence ID from disk
func (sf *SequenceFile) Load() (uint64, error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	data, err := os.ReadFile(sf.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No sequence file yet
		}
		return 0, err
	}

	if len(data) < 8 {
		return 0, fmt.Errorf("corrupted sequence file")
	}
	return binary.BigEndian.Uint64(data[:8]), nil
}

// Save writes the sequence ID to disk atomically
func (sf *SequenceFile) Save(seqID uint64) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, seqID)

	// Write to temp file first
	tmpPath := sf.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	// Atomically replace the old file
	return os.Rename(tmpPath, sf.path)
}

// Serialize converts the Entry to a byte slice for storage
func (e *Entry) Serialize() ([]byte, error) {
	// Validate entry
	if len(e.Key) == 0 {
		return nil, fmt.Errorf("entry key cannot be empty")
	}
	if len(e.Value) > MaxEntrySize || len(e.Key) > MaxEntrySize {
		return nil, fmt.Errorf("entry key or value exceeds maximum size")
	}

	// Create data for checksum (exclude seq and checksum itself)
	dataForChecksum := make([]byte, 0, 1024)
	dataForChecksum = append(dataForChecksum, byte(e.Operation))
	dataForChecksum = append(dataForChecksum, []byte(e.Key)...)
	dataForChecksum = append(dataForChecksum, []byte(e.Value)...)

	ttlBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBytes, uint64(e.TTL))
	dataForChecksum = append(dataForChecksum, ttlBytes...)

	// Calculate checksum
	e.CheckSum = crc32.ChecksumIEEE(dataForChecksum)

	// Serialize all fields
	// Format: [TotalLen][SeqID][Checksum][Timestamp][Operation][TTL][KeyLen][Key][ValueLen][Value]
	keyLen := uint32(len(e.Key))
	valueLen := uint32(len(e.Value))
	totalLen := uint32(4 + 8 + 4 + 8 + 1 + 8 + 4 + keyLen + 4 + valueLen)

	buffer := make([]byte, totalLen)
	offset := 0

	binary.BigEndian.PutUint32(buffer[offset:], totalLen)
	offset += 4

	binary.BigEndian.PutUint64(buffer[offset:], e.SequenceID)
	offset += 8

	binary.BigEndian.PutUint32(buffer[offset:], e.CheckSum)
	offset += 4

	binary.BigEndian.PutUint64(buffer[offset:], uint64(e.Timestamp.UnixNano()))
	offset += 8

	buffer[offset] = byte(e.Operation)
	offset += 1

	binary.BigEndian.PutUint64(buffer[offset:], uint64(e.TTL))
	offset += 8

	binary.BigEndian.PutUint32(buffer[offset:], keyLen)
	offset += 4

	copy(buffer[offset:], e.Key)
	offset += int(keyLen)

	binary.BigEndian.PutUint32(buffer[offset:], valueLen)
	offset += 4

	copy(buffer[offset:], e.Value)

	return buffer, nil
}

// DeserializeEntry converts a byte slice back to an Entry
func DeserializeEntry(r io.Reader) (*Entry, error) {
	// Read total length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf)

	// Read the rest of the entry
	buf := make([]byte, totalLen-4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	offset := 0
	entry := &Entry{}

	// Read SequenceID
	entry.SequenceID = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	// Read CheckSum
	storedChecksum := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	// Read Timestamp
	timestamp := binary.BigEndian.Uint64(buf[offset:])
	entry.Timestamp = time.Unix(0, int64(timestamp))
	offset += 8

	// Read Operation
	entry.Operation = OperationType(buf[offset])
	offset += 1

	// Read TTL
	entry.TTL = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	// Read Key
	keyLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4
	entry.Key = string(buf[offset : offset+int(keyLen)])
	offset += int(keyLen)

	// Read Value
	valueLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4
	entry.Value = string(buf[offset : offset+int(valueLen)])

	// Verify checksum
	dataForChecksum := make([]byte, 0, 1024)
	dataForChecksum = append(dataForChecksum, byte(entry.Operation))
	dataForChecksum = append(dataForChecksum, []byte(entry.Key)...)
	dataForChecksum = append(dataForChecksum, []byte(entry.Value)...)

	ttlBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBytes, uint64(entry.TTL))
	dataForChecksum = append(dataForChecksum, ttlBytes...)

	calculatedChecksum := crc32.ChecksumIEEE(dataForChecksum)
	if calculatedChecksum != storedChecksum {
		return nil, io.ErrUnexpectedEOF // Checksum mismatch
	}

	entry.CheckSum = storedChecksum
	return entry, nil
}

func NewWAL(config Config) (*WAL, error) {
	// Validate config
	if config.Database == "" {
		return nil, fmt.Errorf("database name cannot be empty")
	}
	if config.DataDir == "" {
		config.DataDir = "./data"
	}

	// Create database specific WAL directory
	walDir := filepath.Join(config.DataDir, "wal", config.Database)
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// WAL file path
	walPath := filepath.Join(walDir, "wal.log")

	// Open or create WAL file
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file:     file,
		writer:   bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		path:     walPath,
		database: config.Database,
		syncMode: config.SyncMode,
		seqFile:  NewSequenceFile(walDir),
		closeCh:  make(chan struct{}),
		lastSync: time.Now(),
	}

	// Load the last sequence ID
	if err := wal.loadSequenceID(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load sequence ID: %w", err)
	}

	// Start background sync if using interval mode
	if config.SyncMode == SyncInterval {
		wal.startIntervalSync(config.SyncInterval)
	}

	fmt.Printf("WAL initialized for database '%s' with sequence ID: %d\n", config.Database, wal.sequenceID)

	return wal, nil
}

func (w *WAL) loadSequenceID() error {
	// Try to load from sequence file
	seqID, err := w.seqFile.Load()
	if err == nil && seqID > 0 {
		w.sequenceID = seqID
		return nil
	}

	// Fail back to scanning WAL
	fmt.Printf("[WAL] Sequence file not found, scanning WAL for database '%s'...\n", w.database)
	w.sequenceID = w.scanLastSequenceID()

	// Save for next time
	if w.sequenceID > 0 {
		return w.seqFile.Save(w.sequenceID)
	}

	return nil
}

// scanLastSequenceID scans the WAL file to find the last sequence ID
func (w *WAL) scanLastSequenceID() uint64 {
	file, err := os.Open(w.path)
	if err != nil {
		return 0
	}

	defer file.Close()

	reader := bufio.NewReader(file)
	var lastSeqID uint64
	var entriesScanned uint64

	for {
		entry, err := w.readEntry(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("[WAL] Error scanning WAL: %v\n", err)
			}
			break
		}
		if entry.SequenceID > lastSeqID {
			lastSeqID = entry.SequenceID
		}
		entriesScanned++
	}

	fmt.Printf("[WAL] Scanned %d entries, last sequence ID: %d\n", entriesScanned, lastSeqID)
	return lastSeqID
}

// readEntry reads a single entry from the WAL file
func (w *WAL) readEntry(reader *bufio.Reader) (*Entry, error) {
	return DeserializeEntry(reader)
}

func (w *WAL) Write(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set sequence ID and timestamp atomically
	entry.SequenceID = atomic.AddUint64(&w.sequenceID, 1)
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	// Serialize entry
	data, err := entry.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize WAL entry: %w", err)
	}

	// Write to buffer
	n, err := w.writer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update metrics
	atomic.AddUint64(&w.bytesWritten, uint64(n))
	atomic.AddUint64(&w.entriesCount, 1)

	// Sync based on mode
	switch w.syncMode {
	case SyncImmediate:
		if err := w.sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	case SyncBatch:
		if atomic.LoadUint64(&w.entriesCount)%100 == 0 {
			if err := w.sync(); err != nil {
				return fmt.Errorf("failed to sync WAL: %w", err)
			}
		}
	}

	return nil
}

// sync flushes the buffer and syncs to disk
func (w *WAL) sync() error {
	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return err
	}

	// Update sequence file
	if err := w.seqFile.Save(atomic.LoadUint64(&w.sequenceID)); err != nil {
		// Log but don't fail
		fmt.Printf("[WAL] Warning: failed to update sequence file: %v\n", err)
	}

	w.lastSync = time.Now()
	return nil
}

/*
* startIntervalSync starts a goroutine that syncs the WAL at regular intervals
* starts a new goroutine that listens to a ticker and syncs the WAL at the specified interval.
* if the closeCh signal is received, the goroutine exits.
 */
func (w *WAL) startIntervalSync(interval time.Duration) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.mu.Lock()
				if !w.closed {
					if err := w.sync(); err != nil {
						fmt.Printf("[WAL] Warning: failed to sync WAL: %v\n", err)
					}
				}
				w.mu.Unlock()
			case <-w.closeCh:
				return
			}
		}
	}()
}

// Replay replays the WAL entries using the provided handler
func (w *WAL) Replay(handler func(Entry) error) error {
	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL to replay
		}
		return fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entriesReplayed uint64
	var lastSeqID uint64

	fmt.Printf("[WAL] Starting replay for database '%s'...\n", w.database)
	startTime := time.Now()

	for {
		entry, err := DeserializeEntry(reader)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			// Log corrupted entry but continue
			fmt.Printf("[WAL] Warning: corrupted entry in database '%s': %v\n", w.database, err)
			continue
		}

		// Verify sequence order
		if entry.SequenceID <= lastSeqID {
			fmt.Printf("[WAL] Warning: out-of-order sequence ID: %d <= %d\n",
				entry.SequenceID, lastSeqID)
			continue
		}
		lastSeqID = entry.SequenceID

		// Apply entry using the provided handler
		if err := handler(*entry); err != nil {
			return fmt.Errorf("failed to apply WAL entry %d: %w", entry.SequenceID, err)
		}
		entriesReplayed++
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[WAL] Replay completed for database '%s': %d entries in %v\n",
		w.database, entriesReplayed, elapsed)

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	close(w.closeCh)

	// Wait for background tasks
	w.wg.Wait()

	// Final sync
	if err := w.sync(); err != nil {
		return err
	}

	return w.file.Close()
}

// GetStats returns WAL statistics
func (w *WAL) GetStats() map[string]interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, _ := w.file.Stat()

	return map[string]interface{}{
		"database":      w.database,
		"sequence_id":   atomic.LoadUint64(&w.sequenceID),
		"entries_count": atomic.LoadUint64(&w.entriesCount),
		"bytes_written": atomic.LoadUint64(&w.bytesWritten),
		"file_size":     info.Size(),
		"last_sync":     w.lastSync,
	}
}

// Rotate creates a new WAL file and archives the old one
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Sync current file
	if err := w.sync(); err != nil {
		return err
	}

	// Close current file
	if err := w.file.Close(); err != nil {
		return err
	}

	// Archive old file
	archivePath := fmt.Sprintf("%s.%d", w.path, time.Now().Unix())
	if err := os.Rename(w.path, archivePath); err != nil {
		return err
	}

	// Create new file
	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, DefaultBufferSize)

	fmt.Printf("[WAL] Rotated WAL for database '%s', archived to %s\n", w.database, archivePath)
	return nil
}

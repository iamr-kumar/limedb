package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ritik/limedb/internal/errors"
)

const (
	DefaultBufferSize   = 64 * 1024 // 64KB write buffer
	DefaultSyncInterval = 1000 * time.Millisecond
	DefaultSegmentSize  = 64 * 1024 * 1024 // 64MB per segment
	DefaultDataDir      = "./data"
)

// WALOptions holds configuration options for the Write-Ahead Log
type WALOptions struct {
	DataDir        string
	SyncMode       SyncMode
	SyncInterval   time.Duration // in milliseconds
	BufferSize     int           // write buffer size in bytes
	MaxSegmentSize int64         // max size of each segment in bytes
}

// NewWALOptions creates a new WALOptions with default values
func NewWALOptions() *WALOptions {
	return &WALOptions{
		DataDir:      DefaultDataDir,
		SyncMode:     SyncInterval,
		SyncInterval: DefaultSyncInterval,
		BufferSize:   DefaultBufferSize,
	}
}

// Builder methods for fluent configuration

// WithDataDir sets the data directory for the WAL
func (options *WALOptions) WithDataDir(dir string) *WALOptions {
	options.DataDir = dir
	return options
}

// WithSyncMode sets the synchronization mode for the WAL
func (options *WALOptions) WithSyncMode(mode SyncMode) *WALOptions {
	options.SyncMode = mode
	return options
}

// WithSyncInterval sets the synchronization interval for the WAL
func (options *WALOptions) WithSyncInterval(interval time.Duration) *WALOptions {
	options.SyncInterval = interval
	return options
}

// WithBufferSize sets the write buffer size for the WAL
func (options *WALOptions) WithBufferSize(size int) *WALOptions {
	options.BufferSize = size
	return options
}

// Derived paths

// WALDir returns the directory path for the WAL
func (options *WALOptions) WALDir() string {
	return filepath.Join(options.DataDir, "wal")
}

// SSTDir returns the directory path for SST files
func (options *WALOptions) SSTDir() string {
	return filepath.Join(options.DataDir, "sst")
}

// Validation and initialization

// Validate checks if the WALOptions are valid
func (options *WALOptions) Validate() error {
	// DataDir is required
	if options.DataDir == "" {
		return errors.ErrMissingRequiredField("DataDir")
	}

	// BufferSize must be positive
	if options.BufferSize <= 0 {
		return errors.ErrInvalidValueForParameter("BufferSize", options.BufferSize)
	}

	// MaxSegmentSize must be positive
	if options.MaxSegmentSize <= 0 {
		return errors.ErrInvalidValueForParameter("MaxSegmentSize", options.MaxSegmentSize)
	}

	// SyncInterval must be valid
	if options.SyncMode == SyncInterval && options.SyncInterval <= 0 {
		return errors.ErrInvalidValueForParameter("SyncInterval", options.SyncInterval)
	}

	return nil

}

// EnsureDirectories creates necessary directories for the WAL
func (options *WALOptions) EnsureDirectories() error {
	dirs := []string{
		options.WALDir(),
		options.SSTDir(),
	}
	// Create directories if they do not exist
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("Failed to create directory %s : %w", dir, err)
		}
	}

	return nil
}

// applyDefaults fills in zero values with defaults
func (o *WALOptions) applyDefaults() {
	if o.DataDir == "" {
		o.DataDir = DefaultDataDir
	}
	if o.BufferSize <= 0 {
		o.BufferSize = DefaultBufferSize
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = DefaultSyncInterval
	}
	if o.MaxSegmentSize <= 0 {
		o.MaxSegmentSize = DefaultSegmentSize
	}
}

// Clone creates a deep copy of the options
func (o *WALOptions) Clone() *WALOptions {
	return &WALOptions{
		DataDir:        o.DataDir,
		SyncMode:       o.SyncMode,
		SyncInterval:   o.SyncInterval,
		BufferSize:     o.BufferSize,
		MaxSegmentSize: o.MaxSegmentSize,
	}
}

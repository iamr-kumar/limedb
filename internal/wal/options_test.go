package wal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWALOptionsValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		opts WALOptions
		want string
	}{
		{
			name: "missing data dir",
			opts: WALOptions{},
			want: "missing required field: DataDir",
		},
		{
			name: "invalid buffer size",
			opts: WALOptions{DataDir: "./data", BufferSize: 0},
			want: "invalid value for parameter BufferSize",
		},
		{
			name: "invalid sync interval",
			opts: WALOptions{DataDir: "./data", BufferSize: DefaultBufferSize, SyncMode: SyncInterval, SyncInterval: 0},
			want: "invalid value for parameter SyncInterval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.opts.Validate(); err == nil {
				t.Fatal("Validate() expected error, got nil")
			} else if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want contains %q", err, tt.want)
			}
		})
	}
}

func TestWALOptionsApplyDefaults(t *testing.T) {
	opts := &WALOptions{}
	opts.applyDefaults()

	if opts.DataDir != DefaultDataDir {
		t.Fatalf("DataDir = %s, want %s", opts.DataDir, DefaultDataDir)
	}
	if opts.BufferSize != DefaultBufferSize {
		t.Fatalf("BufferSize = %d, want %d", opts.BufferSize, DefaultBufferSize)
	}
	if opts.SyncInterval != DefaultSyncInterval {
		t.Fatalf("SyncInterval = %v, want %v", opts.SyncInterval, DefaultSyncInterval)
	}
}

func TestWALOptionsEnsureDirectories(t *testing.T) {
	tempDir := t.TempDir()
	opts := NewWALOptions().WithDataDir(tempDir)

	if err := opts.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories() error = %v", err)
	}

	for _, dir := range []string{opts.WALDir(), opts.SSTDir()} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("Stat() on %s error = %v", dir, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s is not a directory", dir)
		}
	}
}

func TestWALOptionsClone(t *testing.T) {
	original := &WALOptions{
		DataDir:      "./data",
		SyncMode:     SyncInterval,
		SyncInterval: 250 * time.Millisecond,
		BufferSize:   1024,
	}

	clone := original.Clone()
	if clone == original {
		t.Fatal("Clone() returned the same pointer")
	}

	if clone.DataDir != original.DataDir || clone.SyncMode != original.SyncMode || clone.SyncInterval != original.SyncInterval || clone.BufferSize != original.BufferSize {
		t.Fatal("Clone() did not copy fields correctly")
	}

	clone.DataDir = filepath.Join(original.DataDir, "nested")
	clone.BufferSize = 2048

	if original.DataDir == clone.DataDir {
		t.Fatal("mutating clone should not affect original DataDir")
	}
	if original.BufferSize == clone.BufferSize {
		t.Fatal("mutating clone should not affect original BufferSize")
	}
}

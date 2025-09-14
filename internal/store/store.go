package store

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ritik/limedb/internal/wal"
)

type Entry struct {
	Key          string
	Value        any
	CreatedAt    time.Time
	LastAccessed time.Time
	ExpiresAt    *time.Time
	AccessCount  int64
	Version      uint64 // For optimistic concurrency control
}

type Shard struct {
	RWMutex sync.RWMutex
	data    map[string]*Entry
}

type SharedStore struct {
	shards   []*Shard
	mask     uint64
	wal      *wal.WAL
	database string

	// Metrics
	hits    uint64
	misses  uint64
	writes  uint64
	deletes uint64

	// Background tasks
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// StoreConfig holds configuration for the store
type StoreConfig struct {
	Database    string
	DataDir     string
	ShardCount  int
	WALSyncMode wal.SyncMode
}

// NewSharedStore creates a new shared store with WAL support
func NewSharedStore(config StoreConfig) (*SharedStore, error) {
	// Validate and set defaults
	if config.Database == "" {
		return nil, fmt.Errorf("database name is required")
	}
	if config.DataDir == "" {
		config.DataDir = "./data"
	}
	if config.ShardCount <= 0 || (config.ShardCount&(config.ShardCount-1)) != 0 {
		config.ShardCount = 256
	}

	// Initialize WAL
	walConfig := wal.Config{
		Database:     config.Database,
		DataDir:      config.DataDir,
		SyncMode:     config.WALSyncMode,
		SyncInterval: 5 * time.Second,
	}

	w, err := wal.NewWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	// Create shards
	shards := make([]*Shard, config.ShardCount)
	for i := 0; i < config.ShardCount; i++ {
		shards[i] = &Shard{
			data: make(map[string]*Entry),
		}
	}

	store := &SharedStore{
		shards:   shards,
		mask:     uint64(config.ShardCount - 1),
		wal:      w,
		database: config.Database,
		stopCh:   make(chan struct{}),
	}

	// Replay WAL to restore state
	if err := store.replayWAL(); err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Start background tasks
	store.startBackgroundTasks()

	fmt.Printf("[Store] Initialized database '%s' with %d shards\n", config.Database, config.ShardCount)

	return store, nil
}

func (s *SharedStore) getShard(key string) *Shard {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	index := hash.Sum64() & s.mask
	return s.shards[index]
}

// Set stores a key-value pair with WAL support
func (s *SharedStore) Set(key string, value any) error {
	return s.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a key-value pair with TTL and WAL support
func (s *SharedStore) SetWithTTL(key string, value any, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Convert value to string for WAL
	valueStr := fmt.Sprintf("%v", value)

	// Write to WAL first for durability
	walEntry := wal.Entry{
		Operation: wal.OperationSet,
		Key:       key,
		Value:     valueStr,
		TTL:       int64(ttl.Seconds()),
		Timestamp: time.Now(),
	}

	if err := s.wal.Write(walEntry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Then update in-memory store
	shard := s.getShard(key)
	shard.RWMutex.Lock()
	defer shard.RWMutex.Unlock()

	now := time.Now()

	// Check if the entry already exists
	if existingEntry, exists := shard.data[key]; exists {
		existingEntry.Value = value
		existingEntry.LastAccessed = now
		existingEntry.AccessCount++
		existingEntry.Version = walEntry.SequenceID // Use WAL sequence as version

		if ttl > 0 {
			expiresAt := now.Add(ttl)
			existingEntry.ExpiresAt = &expiresAt
		} else {
			existingEntry.ExpiresAt = nil
		}
	} else {
		entry := &Entry{
			Key:          key,
			Value:        value,
			CreatedAt:    now,
			LastAccessed: now,
			AccessCount:  0,
			Version:      walEntry.SequenceID,
		}

		if ttl > 0 {
			expiresAt := now.Add(ttl)
			entry.ExpiresAt = &expiresAt
		}

		shard.data[key] = entry
	}

	atomic.AddUint64(&s.writes, 1)
	return nil
}

// Update updates an existing key's value
func (s *SharedStore) Update(key string, value any) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Check if key exists
	shard := s.getShard(key)
	shard.RWMutex.RLock()
	_, exists := shard.data[key]
	shard.RWMutex.RUnlock()

	if !exists {
		return fmt.Errorf("key '%s' does not exist", key)
	}

	// Convert value to string for WAL
	valueStr := fmt.Sprintf("%v", value)

	// Write to WAL first
	walEntry := wal.Entry{
		Operation: wal.OperationUpdate,
		Key:       key,
		Value:     valueStr,
		Timestamp: time.Now(),
	}

	if err := s.wal.Write(walEntry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Update in-memory store
	shard.RWMutex.Lock()
	defer shard.RWMutex.Unlock()

	if entry, ok := shard.data[key]; ok {
		entry.Value = value
		entry.LastAccessed = time.Now()
		entry.AccessCount++
		entry.Version = walEntry.SequenceID
		atomic.AddUint64(&s.writes, 1)
	}

	return nil
}

// Get retrieves a value by key
func (s *SharedStore) Get(key string) (any, bool) {
	shard := s.getShard(key)
	shard.RWMutex.RLock()
	defer shard.RWMutex.RUnlock()

	entry, exists := shard.data[key]
	if !exists {
		atomic.AddUint64(&s.misses, 1)
		/*
		   TODO: Get data from persistent storage (SSTables)
		*/
		return nil, false
	}

	// Check if expired
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		atomic.AddUint64(&s.misses, 1)
		return nil, false
	}

	entry.LastAccessed = time.Now()
	entry.AccessCount++
	atomic.AddUint64(&s.hits, 1)

	return entry.Value, true
}

// Delete removes a key with WAL support
func (s *SharedStore) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Write to WAL first
	walEntry := wal.Entry{
		Operation: wal.OperationDelete,
		Key:       key,
		Timestamp: time.Now(),
	}

	if err := s.wal.Write(walEntry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Delete from in-memory store
	shard := s.getShard(key)
	shard.RWMutex.Lock()
	defer shard.RWMutex.Unlock()

	if _, exists := shard.data[key]; exists {
		delete(shard.data, key)
		atomic.AddUint64(&s.deletes, 1)
		return nil
	}

	return fmt.Errorf("key '%s' not found", key)
}

// Exists checks if a key exists and is not expired
func (s *SharedStore) Exists(key string) bool {
	shard := s.getShard(key)
	shard.RWMutex.RLock()
	defer shard.RWMutex.RUnlock()

	entry, exists := shard.data[key]
	if !exists {
		return false
	}

	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		return false
	}

	return true
}

// GetKeys returns all keys in the store
func (s *SharedStore) GetKeys() []string {
	var keys []string
	now := time.Now()

	for _, shard := range s.shards {
		shard.RWMutex.RLock()
		for k, entry := range shard.data {
			// Skip expired entries
			if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
				continue
			}
			keys = append(keys, k)
		}
		shard.RWMutex.RUnlock()
	}
	return keys
}

// replayWAL replays the WAL to restore state
func (s *SharedStore) replayWAL() error {
	return s.wal.Replay(func(entry wal.Entry) error {
		shard := s.getShard(entry.Key)

		switch entry.Operation {
		case wal.OperationSet:
			shard.RWMutex.Lock()
			dataEntry := &Entry{
				Key:          entry.Key,
				Value:        entry.Value, // In production, you might want to deserialize
				CreatedAt:    entry.Timestamp,
				LastAccessed: entry.Timestamp,
				AccessCount:  0,
				Version:      entry.SequenceID,
			}

			if entry.TTL > 0 {
				expiresAt := entry.Timestamp.Add(time.Duration(entry.TTL) * time.Second)
				// Only set if not already expired
				if time.Now().Before(expiresAt) {
					dataEntry.ExpiresAt = &expiresAt
				} else {
					// Skip expired entries during replay
					shard.RWMutex.Unlock()
					return nil
				}
			}

			shard.data[entry.Key] = dataEntry
			shard.RWMutex.Unlock()

		case wal.OperationUpdate:
			shard.RWMutex.Lock()
			if existing, ok := shard.data[entry.Key]; ok {
				existing.Value = entry.Value
				existing.LastAccessed = entry.Timestamp
				existing.Version = entry.SequenceID
			}
			shard.RWMutex.Unlock()

		case wal.OperationDelete:
			shard.RWMutex.Lock()
			delete(shard.data, entry.Key)
			shard.RWMutex.Unlock()
		}

		return nil
	})
}

// startBackgroundTasks starts background maintenance tasks
func (s *SharedStore) startBackgroundTasks() {
	// TTL cleanup task
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.cleanupExpired()
			case <-s.stopCh:
				return
			}
		}
	}()

	// Stats reporting task
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.reportStats()
			case <-s.stopCh:
				return
			}
		}
	}()
}

// cleanupExpired removes expired entries
func (s *SharedStore) cleanupExpired() {
	now := time.Now()
	var expiredKeys []string

	// Find expired entries
	for _, shard := range s.shards {
		shard.RWMutex.Lock()
		for key, entry := range shard.data {
			if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
				expiredKeys = append(expiredKeys, key)
				delete(shard.data, key)
			}
		}
		shard.RWMutex.Unlock()
	}

	// Log deletions to WAL
	for _, key := range expiredKeys {
		walEntry := wal.Entry{
			Operation: wal.OperationDelete,
			Key:       key,
			Timestamp: now,
		}
		if err := s.wal.Write(walEntry); err != nil {
			fmt.Printf("[Store] Warning: failed to log TTL deletion to WAL: %v\n", err)
		}
	}

	if len(expiredKeys) > 0 {
		fmt.Printf("[Store] Cleaned up %d expired entries\n", len(expiredKeys))
		atomic.AddUint64(&s.deletes, uint64(len(expiredKeys)))
	}
}

// reportStats logs current statistics
func (s *SharedStore) reportStats() {
	stats := s.GetStats()
	fmt.Printf("[Store] Stats: %+v\n", stats)
}

// GetStats returns store statistics
func (s *SharedStore) GetStats() map[string]interface{} {
	totalEntries := 0
	for _, shard := range s.shards {
		shard.RWMutex.RLock()
		totalEntries += len(shard.data)
		shard.RWMutex.RUnlock()
	}

	hits := atomic.LoadUint64(&s.hits)
	misses := atomic.LoadUint64(&s.misses)

	hitRate := float64(0)
	if total := hits + misses; total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	walStats := s.wal.GetStats()

	return map[string]interface{}{
		"database":  s.database,
		"entries":   totalEntries,
		"shards":    len(s.shards),
		"hits":      hits,
		"misses":    misses,
		"writes":    atomic.LoadUint64(&s.writes),
		"deletes":   atomic.LoadUint64(&s.deletes),
		"hit_rate":  fmt.Sprintf("%.2f%%", hitRate),
		"wal_stats": walStats,
	}
}

// CompareAndSwap performs an atomic compare-and-swap operation
func (s *SharedStore) CompareAndSwap(key string, oldValue, newValue any, oldVersion uint64) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("key cannot be empty")
	}

	shard := s.getShard(key)
	shard.RWMutex.Lock()
	defer shard.RWMutex.Unlock()

	entry, exists := shard.data[key]
	if !exists {
		return false, fmt.Errorf("key '%s' does not exist", key)
	}

	// Check version for optimistic concurrency control
	if entry.Version != oldVersion {
		return false, nil // Version mismatch
	}

	// Check if old value matches
	if entry.Value != oldValue {
		return false, nil // Value mismatch
	}

	// Write to WAL
	valueStr := fmt.Sprintf("%v", newValue)
	walEntry := wal.Entry{
		Operation: wal.OperationUpdate,
		Key:       key,
		Value:     valueStr,
		Timestamp: time.Now(),
	}

	if err := s.wal.Write(walEntry); err != nil {
		return false, fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Update in-memory
	entry.Value = newValue
	entry.LastAccessed = time.Now()
	entry.AccessCount++
	entry.Version = walEntry.SequenceID

	atomic.AddUint64(&s.writes, 1)
	return true, nil
}

// Flush forces a WAL sync
func (s *SharedStore) Flush() error {
	// This would typically trigger a WAL sync
	// For now, we rely on the WAL's internal sync mechanism
	return nil
}

// Checkpoint creates a checkpoint for recovery
func (s *SharedStore) Checkpoint() error {
	// In a full implementation, this would:
	// 1. Flush MemTable to SSTable
	// 2. Create a checkpoint marker in WAL
	// 3. Allow old WAL segments to be deleted

	// For now, just rotate the WAL
	return s.wal.Rotate()
}

// Close closes the store and its WAL
func (s *SharedStore) Close() error {
	// Signal background tasks to stop
	close(s.stopCh)

	// Wait for background tasks to complete
	s.wg.Wait()

	// Close WAL
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	fmt.Printf("[Store] Closed database '%s'\n", s.database)
	return nil
}

// GetEntry returns the full entry for a key (for debugging/monitoring)
func (s *SharedStore) GetEntry(key string) (*Entry, bool) {
	shard := s.getShard(key)
	shard.RWMutex.RLock()
	defer shard.RWMutex.RUnlock()

	entry, exists := shard.data[key]
	if !exists || (entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt)) {
		return nil, false
	}

	// Return a copy to avoid race conditions
	entryCopy := *entry
	return &entryCopy, true
}

// Size returns the total number of non-expired entries
func (s *SharedStore) Size() int {
	count := 0
	now := time.Now()

	for _, shard := range s.shards {
		shard.RWMutex.RLock()
		for _, entry := range shard.data {
			if entry.ExpiresAt == nil || now.Before(*entry.ExpiresAt) {
				count++
			}
		}
		shard.RWMutex.RUnlock()
	}

	return count
}

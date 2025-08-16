package store

import (
	"hash/fnv"
	"sync"
	"time"
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
	shards []*Shard
	mask   uint64
}

func NewSharedStore(shardCount int) *SharedStore {
	if shardCount <= 0 || (shardCount&(shardCount-1)) != 0 {
		shardCount = 256
	}
	shards := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &Shard{
			data: make(map[string]*Entry),
		}
	}

	return &SharedStore{
		shards: shards,
		mask:   uint64(shardCount - 1),
	}
}

func (s *SharedStore) getShard(key string) *Shard {
	hash := fnv.New64a()
	hash.Write([]byte(key))
	index := hash.Sum64() & s.mask
	return s.shards[index]
}

func (s *SharedStore) Set(key string, value any) {
	shard := s.getShard(key)
	shard.RWMutex.Lock()
	defer shard.RWMutex.Unlock()

	// Check if the entry already exists
	if existingEntry, exists := shard.data[key]; exists {
		existingEntry.Value = value
		existingEntry.LastAccessed = time.Now()
		existingEntry.AccessCount++
		existingEntry.Version++
	} else {
		shard.data[key] = &Entry{
			Key:          key,
			Value:        value,
			CreatedAt:    time.Now(),
			LastAccessed: time.Now(),
			AccessCount:  0,
			Version:      1,
		}
	}
}

func (s *SharedStore) Get(key string) (any, bool) {
	shard := s.getShard(key)
	shard.RWMutex.RLock()
	defer shard.RWMutex.RUnlock()

	entry, exists := shard.data[key]
	if !exists {
		/*
			TODO: Get data from persistent storage
		*/
		return nil, false
	}

	entry.LastAccessed = time.Now()
	entry.AccessCount++

	return entry.Value, true
}

func (s *SharedStore) Delete(key string) bool {
	shard := s.getShard(key)
	shard.RWMutex.Lock()
	defer shard.RWMutex.Unlock()

	if _, exists := shard.data[key]; exists {
		delete(shard.data, key)
		return true
	}

	return false
}

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

func (s *SharedStore) GetKeys() []string {
	var keys []string
	for _, shard := range s.shards {
		shard.RWMutex.RLock()
		for k := range shard.data {
			keys = append(keys, k)
		}
		shard.RWMutex.RUnlock()
	}
	return keys
}

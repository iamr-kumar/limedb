package memtable

import (
	"sync/atomic"

	"github.com/ritik/limedb/internal/errors"
	"github.com/ritik/limedb/internal/skiplist"
)

const MaxAllowedSizeBytes = 64 * 1024 * 1024 // 64MB

type Memtable struct {
	skipList    *skiplist.SkipList
	isImmutable atomic.Bool
	maxSeqID    atomic.Uint64
	size        atomic.Uint64
}

func NewMemtable(startSeqID uint64) *Memtable {
	m := &Memtable{
		skipList: skiplist.NewSkipList(),
	}
	m.maxSeqID.Store(startSeqID)
	return m
}

// Put inserts a MemtableEntry into the Memtable
func (m *Memtable) Put(entry *MemtableEntry) error {
	if m.isImmutable.Load() {
		return errors.ErrMemtableFull
	}

	m.skipList.Insert(entry.Key, entry.serializeMemtableEntry())
	for {
		cur := m.maxSeqID.Load()
		if entry.SequenceID <= cur {
			break
		}
		if m.maxSeqID.CompareAndSwap(cur, entry.SequenceID) {
			break
		}
	}
	return nil
}

// Get retrieves a MemtableEntry from the Memtable by key
func (m *Memtable) Get(key []byte) (*MemtableEntry, bool) {
	value, found := m.skipList.Get(key)
	if !found {
		return nil, false
	}
	entry := deserializeMemtableEntry(value)
	return entry, true
}

// MarkImmutable marks the Memtable as immutable
func (m *Memtable) MarkImmutable() {
	m.isImmutable.Store(true)
}

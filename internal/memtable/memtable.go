package memtable

import (
	"bytes"
	"sync/atomic"

	"github.com/ritik/limedb/internal/errors"
	"github.com/ritik/limedb/internal/keys"
	"github.com/ritik/limedb/internal/skiplist"
)

const MaxMemTableSize = 64 * 1024 * 1024 // 64 MB

// MemTable represents an in-memory table that stores key-value pairs.
// It uses a skip list for efficient storage and retrieval.
// MemTables are mutable until they are marked as immutable, after which they
// can no longer be modified and are ready to be flushed to disk.
type MemTable struct {
	// pointer to the skip list used for storage
	skipList *skiplist.SkipList

	// approx current size of the memtable in bytes
	currSize uint64

	// maximum sequence ID assigned to any entry in this memtable
	maxSeqID uint64

	// indicates if the memtable is
	immutable atomic.Bool
}

// NewMemTable creates and returns a new MemTable instance.
func NewMemTable() *MemTable {
	// Creating a new MemTable with an empty skip list and initial values.
	// TODO: set max seq ID and currSize from params for when creating after WAL replay
	return &MemTable{
		skipList:  skiplist.NewSkipList(),
		currSize:  0,
		maxSeqID:  0,
		immutable: atomic.Bool{},
	}
}

func (m *MemTable) Put(key, value []byte, seqID uint64, vType keys.ValueType) error {
	// check if memtable is immutable
	if m.immutable.Load() {
		return errors.ErrMemtableImmutable
	}

	ik := &keys.InternalKey{
		UserKey:    key,
		SequenceID: seqID,
		Type:       vType,
	}
	encodedKey := ik.Encode()

	// check if adding this key will exceed max memtable size
	entrySize := uint64(len(encodedKey) + len(value))
	if m.currSize+entrySize > MaxMemTableSize {
		return errors.ErrMemtableFull
	}

	// insert into skip list
	m.skipList.Insert(encodedKey, value)

	// update current size
	atomic.AddUint64(&m.currSize, entrySize)

	// update max sequence ID
	for {
		currentMax := atomic.LoadUint64(&m.maxSeqID)
		// If for a userKey, we write with a lower seqID than current max, no need to update
		// The lower seqID entries will be ignored during reads as they would be come after the most recent ones
		// in the skip list ordering and therefore, ignored during disk flush
		if seqID <= currentMax {
			break
		}
		if atomic.CompareAndSwapUint64(&m.maxSeqID, currentMax, seqID) {
			break
		}

	}

	return nil
}

// Get retrieves the value for the given user key from the memtable.
// It returns the value and a boolean indicating if the key was found.
// If the most recent entry for the user key is a deletion tombstone, it returns not found.
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	// create a lookup internal key with max sequence ID to find the most recent entry
	ik := &keys.InternalKey{
		UserKey:    key,
		SequenceID: ^uint64(0), // max uint64
		Type:       keys.TypeValue,
	}
	encodedKey := ik.Encode()

	foundKey, value, found := m.skipList.FindGreaterOrEqual(encodedKey)
	if !found {
		return nil, false
	}
	decodedKey, err := keys.DecodeInternalKey(foundKey)
	if err != nil {
		return nil, false
	}
	// check if user keys match
	if !bytes.Equal(ik.UserKey, decodedKey.UserKey) {
		return nil, false
	}
	// check if the found key is a deletion tombstone
	if decodedKey.Type == keys.TypeDeletion {
		return nil, false
	}
	return value, true
}

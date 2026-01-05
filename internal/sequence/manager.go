package sequence

import "sync/atomic"

// SequenceManager manages the generation of unique sequence IDs.
// This will be used to assign sequence numbers to entries in the memtable
// and to entries in the WAL segments.
type SequenceManager struct {
	next uint64
}

// Init initializes the SequenceManager with a starting sequence ID.
// When replaying WAL segments, this should be set to the highest sequence ID found + 1.
func (manager *SequenceManager) Init(start uint64) {
	atomic.StoreUint64(&manager.next, start)
}

// Next returns the existing next sequence ID and increments the internal counter.
func (manager *SequenceManager) Next() uint64 {
	return atomic.AddUint64(&manager.next, 1) - 1
}

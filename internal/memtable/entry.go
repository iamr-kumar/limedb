package memtable

type MemtableEntry struct {
	Key        []byte
	Value      []byte
	Type       uint8
	SequenceID uint64
	ExpiresAt  int64
}

func NewMemtableEntry(key, value []byte, entryType uint8, sequenceID uint64, expiresAt int64) *MemtableEntry {
	return &MemtableEntry{
		Key:        key,
		Value:      value,
		Type:       entryType,
		SequenceID: sequenceID,
		ExpiresAt:  expiresAt,
	}
}

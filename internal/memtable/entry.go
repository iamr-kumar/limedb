package memtable

import "github.com/ritik/limedb/internal/codec"

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

// serializeMemtableEntry serializes a MemtableEntry into a byte slice
// Simple serialization: Key length + Key + Value length + Value + Type + SequenceID + ExpiresAt
func (entry *MemtableEntry) serializeMemtableEntry() []byte {
	// Simple serialization: Key length + Key + Value length + Value + Type + SequenceID + ExpiresAt
	keyLen := uint32(len(entry.Key))
	valueLen := uint32(len(entry.Value))
	totalLen := 4 + keyLen + 4 + valueLen + 1 + 8 + 8

	buf := make([]byte, totalLen)
	offset := 0

	// Key length
	codec.EncodeUInt32ToBuffer(keyLen, buf[offset:])
	offset += 4
	// Key
	copy(buf[offset:], entry.Key)
	offset += int(keyLen)

	// Value length
	codec.EncodeUInt32ToBuffer(valueLen, buf[offset:])
	offset += 4

	// Value
	copy(buf[offset:], entry.Value)
	offset += int(valueLen)

	// Type
	buf[offset] = entry.Type
	offset += 1
	// SequenceID
	codec.EncodeUInt64ToBuffer(entry.SequenceID, buf[offset:])
	offset += 8
	// ExpiresAt
	codec.EncodeUInt64ToBuffer(uint64(entry.ExpiresAt), buf[offset:])

	return buf

}

// deserializeMemtableEntry deserializes a byte slice into a MemtableEntry
func deserializeMemtableEntry(data []byte) *MemtableEntry {
	offset := 0
	// Key length
	keyLen := codec.DecodeUInt32(data[offset:])
	offset += 4

	// Key
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)
	// Value length
	valueLen := codec.DecodeUInt32(data[offset:])
	offset += 4
	// Value
	value := make([]byte, valueLen)
	copy(value, data[offset:offset+int(valueLen)])
	offset += int(valueLen)
	// Type
	entryType := data[offset]
	offset += 1
	// SequenceID
	sequenceID := codec.DecodeUInt64(data[offset:])
	offset += 8
	// ExpiresAt
	expiresAt := int64(codec.DecodeUInt64(data[offset:]))

	return &MemtableEntry{
		Key:        key,
		Value:      value,
		Type:       entryType,
		SequenceID: sequenceID,
		ExpiresAt:  expiresAt,
	}
}

func (entry *MemtableEntry) Size() uint64 {
	return uint64(4 + len(entry.Key) + 4 + len(entry.Value) + 1 + 8 + 8)
}

package keys

import (
	"bytes"

	"github.com/ritik/limedb/internal/codec"
	"github.com/ritik/limedb/internal/errors"
)

type ValueType uint8

const (
	TypeValue    ValueType = 0 // Normal value
	TypeDeletion ValueType = 1 // Tombstone value
)

// InternalKey represents a key in the internal storage format.
// It consists of the user key, a sequence ID, and a value type.
// It is required to support versioning and deletions of user keys.
type InternalKey struct {
	UserKey    []byte
	SequenceID uint64
	Type       ValueType
}

// Encode encodes the InternalKey into a byte slice.
func (ik *InternalKey) Encode() []byte {
	encoded := make([]byte, len(ik.UserKey)+8+1)
	// format: [Type(1 byte) | SequenceID(8 bytes) | UserKey(variable length)]
	encoded[0] = byte(ik.Type)
	codec.EncodeUInt64ToBuffer(ik.SequenceID, encoded[1:9])
	copy(encoded[9:], ik.UserKey)
	return encoded
}

// DecodeInternalKey decodes a byte slice into an InternalKey.
func DecodeInternalKey(data []byte) (*InternalKey, error) {
	if len(data) < 9 {
		return nil, errors.ErrInvalidInternalKey
	}

	ik := &InternalKey{}
	ik.Type = ValueType(data[0])

	// verify that type is valid
	if ik.Type != TypeValue && ik.Type != TypeDeletion {
		return nil, errors.ErrInvalidInternalKey
	}

	ik.SequenceID = codec.DecodeUInt64(data[1:9])

	ik.UserKey = make([]byte, len(data)-9)
	copy(ik.UserKey, data[9:])
	return ik, nil
}

// CompareInternalKeys compares two encoded internal keys.
// Ordering: UserKey ASC, SequenceID DESC
func CompareInternalKeys(keyA, keyB *InternalKey) int {

	// First: UserKey ascending
	if cmp := bytes.Compare(keyA.UserKey, keyB.UserKey); cmp != 0 {
		return cmp
	}

	// Second: SequenceID descending (higher seq = "smaller" = comes first)
	if keyA.SequenceID > keyB.SequenceID {
		return -1
	}
	if keyA.SequenceID < keyB.SequenceID {
		return 1
	}

	// Same UserKey + same SequenceID = equal (shouldn't happen)
	return 0
}

// ComesBefore returns true if ik comes before other in the internal key ordering.
func (ik *InternalKey) ComesBefore(other *InternalKey) bool {
	return CompareInternalKeys(ik, other) < 0
}

// ComesAfter returns true if ik comes after other in the internal key ordering.
func (ik *InternalKey) ComesAfter(other *InternalKey) bool {
	return CompareInternalKeys(ik, other) > 0
}

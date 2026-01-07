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
	// format: [UserKey(variable)][InverseSeqID(8 bytes)][Type(1 byte)]
	// UserKey comes first for lexicographical ordering followed by InverseSeqID and Type
	// InverseSeqID = ^SequenceID to ensure higher sequence IDs sort before lower ones
	// Type is last byte to distinguish between value and tombstone for same UserKey+SeqID
	copy(encoded, ik.UserKey)
	invSeqID := ^ik.SequenceID
	codec.EncodeUInt64ToBuffer(invSeqID, encoded[len(ik.UserKey):len(ik.UserKey)+8])
	encoded[len(encoded)-1] = byte(ik.Type)
	return encoded
}

// DecodeInternalKey decodes a byte slice into an InternalKey.
func DecodeInternalKey(data []byte) (*InternalKey, error) {
	if len(data) < 9 {
		return nil, errors.ErrInvalidInternalKey
	}

	t := ValueType(data[len(data)-1])
	if t != TypeValue && t != TypeDeletion {
		return nil, errors.ErrInvalidInternalKey
	}

	// last 9 bytes are [InverseSeqID(8 bytes)][Type(1 byte)]
	invSeqID := codec.DecodeUInt64(data[len(data)-9 : len(data)-1])
	seqID := ^invSeqID

	userKeyLen := len(data) - 9
	uk := make([]byte, userKeyLen)
	copy(uk, data[:userKeyLen])

	return &InternalKey{
		UserKey:    uk,
		SequenceID: seqID,
		Type:       t,
	}, nil
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

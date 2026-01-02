package wal

import (
	"io"
	"time"

	"github.com/ritik/limedb/internal/codec"
	"github.com/ritik/limedb/internal/errors"
)

type OperationType uint8

const (
	OperationSet OperationType = iota
	OperationSetWithTTL
	OperationDelete
)

const (
	MaxEntrySize = 10 * 1024 * 1024 // 10MB max entry size
)

type WalEntry struct {
	Timestamp time.Time
	Operation OperationType
	Key       string
	Value     string
	// Unix timestamp in seconds (0 means no expiration)
	ExpiresAt uint64
	// Unique sequence ID for ordering
	SequenceID uint64
	// Checksum for data integrity
	Checksum uint32
}

func (entry *WalEntry) Serialize() ([]byte, error) {
	// Validate entry
	if len(entry.Key) == 0 {
		return nil, errors.ErrInvalidWalEntry
	}
	if len(entry.Value) > MaxEntrySize || len(entry.Key) > MaxEntrySize {
		return nil, errors.ErrInvalidWalEntry
	}

	// Create data for checksum calculation
	dataForChecksum := getDataForChecksum(entry)
	// Calculate checksum
	entry.Checksum = codec.CalculateChecksum(dataForChecksum)

	// Serialize the entry
	// Format: [TotalLen][SeqID][Checksum][Timestamp][ExpiresAt][Operation][KeyLen][Key][ValueLen][Value]
	keyLen := uint32(len(entry.Key))
	valueLen := uint32(len(entry.Value))
	// total length = 4 bytes (totalLen) + 8 bytes (SeqID) + 4 bytes (Checksum) + 8 bytes (Timestamp)
	// 									+ 8 bytes (ExpiresAt) + 1 bytes (Operation) + 4 bytes (KeyLen) + keyLen + 4 bytes (ValueLen) + valueLen
	totalLen := uint32(4 + 8 + 4 + 8 + 8 + 1 + 4 + keyLen + 4 + valueLen)

	buffer := make([]byte, totalLen)
	offset := 0

	// Write total length
	codec.EncodeUInt32ToBuffer(totalLen, buffer[offset:])
	offset += 4

	// Write SequenceID
	codec.EncodeUInt64ToBuffer(entry.SequenceID, buffer[offset:])
	offset += 8

	// Write Checksum
	codec.EncodeUInt32ToBuffer(entry.Checksum, buffer[offset:])
	offset += 4

	// Write Timestamp
	codec.EncodeUInt64ToBuffer(uint64(entry.Timestamp.Unix()), buffer[offset:])
	offset += 8

	// Write ExpiresAt
	codec.EncodeUInt64ToBuffer(entry.ExpiresAt, buffer[offset:])
	offset += 8

	// Write Operation
	buffer[offset] = byte(entry.Operation)
	offset += 1

	// Write KeyLen and Key
	codec.EncodeUInt32ToBuffer(keyLen, buffer[offset:])
	offset += 4
	copy(buffer[offset:], []byte(entry.Key))
	offset += int(keyLen)

	// Write ValueLen and Value
	codec.EncodeUInt32ToBuffer(valueLen, buffer[offset:])
	offset += 4
	copy(buffer[offset:], []byte(entry.Value))

	return buffer, nil
}

func DeserializeWalEntry(reader io.Reader) (*WalEntry, error) {
	// Read total length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		return nil, err
	}

	totalLen := codec.DecodeUInt32(lenBuf)

	// Read the rest of the entry based on total length
	entryBuf := make([]byte, totalLen-4)
	if _, err := io.ReadFull(reader, entryBuf); err != nil {
		return nil, err
	}

	offset := 0
	entry := &WalEntry{}

	// Read SequenceID
	entry.SequenceID = codec.DecodeUInt64(entryBuf[offset:])
	offset += 8

	// Read Checksum
	entry.Checksum = codec.DecodeUInt32(entryBuf[offset:])
	offset += 4

	// Read Timestamp
	timestampUnix := codec.DecodeUInt64(entryBuf[offset:])
	entry.Timestamp = time.Unix(int64(timestampUnix), 0)
	offset += 8

	// Read ExpiresAt
	entry.ExpiresAt = codec.DecodeUInt64(entryBuf[offset:])
	offset += 8

	// Read Operation
	entry.Operation = OperationType(entryBuf[offset])
	offset += 1

	// Read KeyLen and Key
	keyLen := codec.DecodeUInt32(entryBuf[offset:])
	offset += 4
	entry.Key = string(entryBuf[offset : offset+int(keyLen)])
	offset += int(keyLen)

	// Read ValueLen and Value
	valueLen := codec.DecodeUInt32(entryBuf[offset:])
	offset += 4
	entry.Value = string(entryBuf[offset : offset+int(valueLen)])

	// Validate checksum
	dataForChecksum := getDataForChecksum(entry)
	calculatedChecksum := codec.CalculateChecksum(dataForChecksum)
	if calculatedChecksum != entry.Checksum {
		return nil, errors.ErrInvalidWalEntry
	}

	return entry, nil
}

// getDataForChecksum prepares the byte slice used for checksum calculation
func getDataForChecksum(entry *WalEntry) []byte {
	dataForChecksum := make([]byte, 0, 1024)
	dataForChecksum = append(dataForChecksum, byte(entry.Operation))
	dataForChecksum = append(dataForChecksum, []byte(entry.Key)...)
	dataForChecksum = append(dataForChecksum, []byte(entry.Value)...)
	dataForChecksum = append(dataForChecksum, codec.EncodeUInt64(entry.ExpiresAt)...)

	return dataForChecksum
}

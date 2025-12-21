package codec

import "encoding/binary"

// EncodeUInt64 encodes a uint64 value into an 8-byte big-endian byte slice
func EncodeUInt64(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	return buffer
}

// DecodeUInt64 decodes an 8-byte big-endian byte slice into a uint64 value
func DecodeUInt64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

// EncodeUInt32 encodes a uint32 value into a 4-byte big-endian byte slice
func EncodeUInt32(value uint32) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, value)
	return buffer
}

// DecodeUInt32 decodes a 4-byte big-endian byte slice into a uint32 value
func DecodeUInt32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// EncodeUInt64ToBuffer encodes a uint64 value into the provided byte slice in big-endian format
func EncodeUInt64ToBuffer(value uint64, buffer []byte) {
	binary.BigEndian.PutUint64(buffer, value)
}

// EncodeUInt32ToBuffer encodes a uint32 value into the provided byte slice in big-endian format
func EncodeUInt32ToBuffer(value uint32, buffer []byte) {
	binary.BigEndian.PutUint32(buffer, value)
}

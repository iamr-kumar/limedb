package codec

import "hash/crc32"

// CalculateChecksum computes the CRC32 checksum of the given data
func CalculateChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
